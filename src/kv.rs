use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::io::SeekFrom::Current;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::errors::KvError::{FailToOpenFile, KeyNotFound};
use crate::kv::KvCommand::{Put, Rm};

use super::{KvError, Result};

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone, Copy)]
struct BinLocation {
    offset: usize,
    length: usize,
}

/// The storage engine.
/// It implements the in-memory Hash index like bitcask.
pub struct KvStore {
    index: HashMap<String, BinLocation>,
    file: File,
    path: PathBuf,
    steal: usize,
}

impl KvStore {
    const STEAL_THRESHOLDS: usize = 1024 * 1024; // 1MB
}

#[derive(Serialize, Deserialize, Debug)]
enum KvCommand {
    Put { key: String, value: String },
    Rm { key: String },
}

impl KvCommand {
    fn set(key: String, value: String) -> Self {
        Self::Put { key, value }
    }

    fn remove(key: String) -> Self {
        Self::Rm { key }
    }

    fn key(&self) -> &str {
        match self {
            KvCommand::Put { key, .. } => key,
            KvCommand::Rm { key } => key,
        }
        .as_str()
    }
}

impl KvStore {
    /// build the in-memory index from file.
    fn build_index(&mut self) -> Result<usize> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut reader = BufReader::new(&mut self.file);
        let mut buf = String::new();
        let mut x;
        let mut steal = 0;
        while {
            x = reader.read_line(&mut buf)?;
            x > 0
        } {
            let json = serde_json::from_slice(buf.as_bytes())?;
            match json {
                KvCommand::Put { key: key_read, .. } => {
                    let offset = reader.seek(SeekFrom::Current(0))? as usize;
                    let old = self.index.insert(
                        key_read,
                        BinLocation {
                            offset: offset - x,
                            length: x,
                        },
                    );
                    if let Some(BinLocation { length, .. }) = old {
                        steal += length
                    }
                }
                KvCommand::Rm { key: key_read } => {
                    let offset = reader.seek(SeekFrom::Current(0))? as usize;
                    let old = self.index.insert(
                        key_read,
                        BinLocation {
                            offset: offset - x,
                            length: x,
                        },
                    );
                    if let Some(BinLocation { length, .. }) = old {
                        steal += length
                    }
                }
            }
            buf.clear();
        }
        Ok(steal)
    }

    /// load a command from one `BinLocation`.
    fn load_command(&mut self, location: BinLocation) -> Result<KvCommand> {
        self.file.seek(SeekFrom::Start(location.offset as u64))?;
        let mut buf = String::new();
        let mut reader = BufReader::new(&mut self.file);
        reader.read_line(&mut buf)?;
        let result = serde_json::from_slice(buf.as_bytes())?;
        Ok(result)
    }

    /// save a command into data file, and update the index.
    fn save_command(&mut self, command: KvCommand) -> Result<()> {
        let serialized = Self::serialize_command(&command);
        let offset = self.file.seek(SeekFrom::Current(0))? as usize;
        self.file.write_all(serialized.as_bytes())?;
        let key = command.key().to_owned();
        let old = self.index.insert(
            key,
            BinLocation {
                offset,
                length: serialized.len(),
            },
        );
        if let Some(BinLocation { length, .. }) = old {
            self.steal += length;
            if self.steal > Self::STEAL_THRESHOLDS {
                self.compact_file()?;
            }
        }
        Ok(())
    }

    /// support method for serialize one command.
    fn serialize_command(command: &KvCommand) -> String {
        let mut serialized = serde_json::to_string(&command).unwrap();
        serialized.push('\n');
        serialized
    }

    /// Compact the file.
    /// This will merge all the indices, only save the last put or rm operation in the log.
    /// This should be called maybe, so that the log file will not grow too fast.
    pub fn compact_file(&mut self) -> Result<()> {
        let path = &self.path.join("kvs-compact-temp-file");
        let mut temp_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(path)
            .map_err(|io_error| FailToOpenFile {
                file_name: path.to_str().unwrap_or("unknown").to_owned(),
                io_error,
            })?;
        self.compact_file_to(&mut temp_file)?;
        std::fs::copy(path, &self.path.join("kvs-db-data.json"))?;
        std::fs::remove_file(path)?;
        self.reopen_file()?;
        self.steal = 0;
        Ok(())
    }

    /// write the compacted data file into an stream.
    fn compact_file_to(&mut self, temp_file: &mut (impl Write + Seek)) -> Result<()> {
        let old_index = std::mem::replace(&mut self.index, HashMap::new());
        for (k, v) in old_index.iter() {
            // we deserialize the stream so that we are able to check consistency.
            let command = self.load_command(*v)?;
            if command.key() != k.as_str() {
                panic!("Failed in check consistency between in-memory index and disk file: the file has key {}, but the index has key {}.", command.key(), k.as_str());
            }
            let serialized = Self::serialize_command(&command);
            self.index.insert(
                k.to_owned(),
                BinLocation {
                    offset: temp_file.seek(Current(0))? as usize,
                    length: serialized.len(),
                },
            );
            temp_file.write_all(serialized.as_bytes())?;
        }
        temp_file.flush()?;

        Ok(())
    }

    /// reopen the db file.
    fn reopen_file(&mut self) -> Result<()> {
        self.file = OpenOptions::new()
            .append(true)
            .read(true)
            .open(&self.path.join("kvs-db-data.json"))
            .map_err(|e| KvError::FailToOpenFile {
                file_name: String::from(self.path.to_str().unwrap_or("unknown")),
                io_error: e,
            })?;
        Ok(())
    }

    /// make an KvStore by an database file.
    ///
    /// # Error
    ///
    /// If failed to open file, a `FailToOpenFile` will be thrown;
    /// During the process of building the index, we may face some deserialize/IO exception, which will also be thrown.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path.as_ref().join("kvs-db-data.json"))
            .map_err(|e| KvError::FailToOpenFile {
                file_name: String::from(path.as_ref().to_str().unwrap_or("unknown")),
                io_error: e,
            })?;
        let mut store = KvStore {
            file,
            path: Path::new(path.as_ref()).to_owned(),
            index: HashMap::new(),
            steal: 0,
        };
        store.steal = store.build_index()?;
        Ok(store)
    }

    /// get a value from the KvStore.
    ///
    /// # Error
    ///
    /// when IO/serialize error happens during read data before the log, we will
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let cache = self.index.get(key.as_str());
        if cache.is_none() {
            return Ok(None);
        }
        let pos = *cache.unwrap();
        let cmd = self.load_command(pos)?;
        match cmd {
            Rm { .. } => Ok(None),
            Put { value, .. } => Ok(Some(value)),
        }
    }

    /// Put a value into the KvStore.
    /// This operation will be automatically persisted into the log file.
    ///
    /// # Error
    ///
    /// when IO/serialize error happens during save the command into log, will throw error about them.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = KvCommand::set(key.clone(), value);
        self.save_command(command)?;
        Ok(())
    }

    /// Remove an value from the KvStore
    ///
    /// # Error
    ///
    /// when the key isn't present, will throw `KeyNotFound`.
    /// when IO/serialize error happens during save the command into log, will throw error about them.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if !self.index.contains_key(key.as_str()) {
            return Err(KeyNotFound);
        }

        let command = KvCommand::remove(key.clone());
        self.save_command(command)?;
        Ok(())
    }
}
