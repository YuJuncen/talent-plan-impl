use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::io::SeekFrom::Current;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use serde::{Deserialize, Serialize};

use crate::engines::engine::KvsEngine;

use super::engine;
use super::errors::{KvError, Result};
use super::errors::KvError::{FailToOpenFile, KeyNotFound};

use self::KvCommand::{Put, Rm};

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone, Copy)]
struct BinLocation {
    offset: usize,
    length: usize,
}

#[derive(Clone)]
pub struct KvStore {
    index: Arc<RwLock<HashMap<String, BinLocation>>>,
    reader: RefCell<KvReader>,
    writer: Arc<Mutex<File>>,
    path: PathBuf,
    steal: Arc<RwLock<usize>>,
}

pub struct KvReader {
    file: File,
    root: PathBuf,
}

impl Read for KvReader {
    fn read(&mut self, buf: &mut [u8]) -> std::result::Result<usize, std::io::Error> {
        self.file.read(buf)
    }
}

impl Seek for KvReader {
    fn seek(&mut self, pos: SeekFrom) -> std::result::Result<u64, std::io::Error> {
        self.file.seek(pos)
    }
}

impl Clone for KvReader {
    fn clone(&self) -> Self {
        KvReader {
            root: self.root.clone(),
            file: OpenOptions::new()
                .read(true)
                .open(&self.root)
                // when `clone` called, we can assume that the file is always available.
                .unwrap(),
        }
    }
}

impl KvStore {
    const STEAL_THRESHOLDS: usize = 1024 * 1024 * 8; // 8MB
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


impl KvsEngine for KvStore {
    /// get a value from the KvStore.
    ///
    /// # Error
    ///
    /// when IO/serialize error happens during read data before the log, we will
    fn get(&self, key: String) -> Result<Option<String>> {
        let cache = self.index.read()?.get(key.as_str()).cloned();
        if cache.is_none() {
            return Ok(None);
        }
        let pos = cache.unwrap();
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
    fn set(&self, key: String, value: String) -> Result<()> {
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
    fn remove(&self, key: String) -> Result<()> {
        if !self.index.read()?.contains_key(key.as_str()) {
            return Err(KeyNotFound);
        }

        let command = KvCommand::remove(key.clone());
        self.save_command(command)?;
        Ok(())
    }

}

impl KvStore {
    /// build the in-memory index from file.
    fn build_index(&mut self) -> Result<usize> {
        self.reader.borrow_mut().seek(SeekFrom::Start(0))?;
        let mut inner = self.reader.borrow_mut();
        let mut reader = BufReader::new(inner.by_ref());
        let mut buf = String::new();
        let mut x;
        let mut steal = 0;
        let mut idx = self.index.write()?;
        while {
            x = reader.read_line(&mut buf)?;
            x > 0
        } {
            let json = serde_json::from_slice(buf.as_bytes())?;
            match json {
                KvCommand::Put { key: key_read, .. } => {
                    let offset = reader.seek(SeekFrom::Current(0))? as usize;
                    let old = idx.insert(
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
                    let old = idx.insert(
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
    fn load_command(&self, location: BinLocation) -> Result<KvCommand> {
        self.reader.borrow_mut().seek(SeekFrom::Start(location.offset as u64))?;
        let mut buf = String::new();
        let mut ref_mut = self.reader.borrow_mut();
        let mut reader = BufReader::new(ref_mut.by_ref());
        reader.read_line(&mut buf)?;
        let result = serde_json::from_slice(buf.as_bytes())?;
        Ok(result)
    }

    /// save a command into data file, and update the index.
    fn save_command(&self, command: KvCommand) -> Result<()> {
        let serialized = Self::serialize_command(&command);
        let mut writer = self.writer.lock()?;
        let offset = writer.seek(SeekFrom::End(0))? as usize;
        writer.write_all(serialized.as_bytes())?;
        let key = command.key().to_owned();
        let old = self.index.write()?.insert(
            key,
            BinLocation {
                offset,
                length: serialized.len(),
            },
        );
        if let Some(BinLocation { length, .. }) = old {
            let mut steal = self.steal.write()?;
            *steal += length;
            if *steal > Self::STEAL_THRESHOLDS {
                drop(steal);
                drop(writer);
                self.compact_file()?;
                return Ok(());
            }
        }
        writer.flush()?;
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
    pub fn compact_file(&self) -> Result<()> {
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
        let mut writer = self.writer.lock()?;
        std::fs::copy(path, &self.path.join("kvs-db-data.json"))?;
        std::fs::remove_file(path)?;
        *writer = OpenOptions::new()
            .append(true)
            .read(true)
            .open(&self.path.join("kvs-db-data.json"))
            .map_err(|e| KvError::FailToOpenFile {
                file_name: String::from(self.path.to_str().unwrap_or("unknown")),
                io_error: e,
            })?;
        *self.steal.write()? = 0;
        Ok(())
    }

    /// write the compacted data file into an stream.
    fn compact_file_to(&self, temp_file: &mut (impl Write + Seek)) -> Result<()> {
        let mut idx = self.index.write()?;
        let old_index = std::mem::replace(&mut *idx, HashMap::new());
        for (k, v) in old_index.iter() {
            // we deserialize the stream so that we are able to check consistency.
            let command = self.load_command(*v)?;
            if command.key() != k.as_str() {
                panic!("Failed in check consistency between in-memory index and disk file: the file has key {}, but the index has key {}.", command.key(), k.as_str());
            }
            let serialized = Self::serialize_command(&command);
            idx.insert(
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

    /// make an KvStore by an database file.
    ///
    /// # Error
    ///
    /// If failed to open file, a `FailToOpenFile` will be thrown;
    /// During the process of building the index, we may meet some deserialize/IO exception, which will also be thrown,
    /// sealed in the `OtherIOException` variant.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        engine::check_engine::<&P>(&path, "kvs")?;

        let writer = Arc::new(Mutex::new(OpenOptions::new()
            .create(true)
            .append(true)
            .open(path.as_ref().join("kvs-db-data.json"))
            .map_err(|e| KvError::FailToOpenFile {
                file_name: String::from(path.as_ref().to_str().unwrap_or("unknown")),
                io_error: e,
            })?));
        let reader = KvReader {
            file: OpenOptions::new()
                .read(true)
                .open(path.as_ref().join("kvs-db-data.json"))
                .map_err(|e| KvError::FailToOpenFile {
                file_name: String::from(path.as_ref().to_str().unwrap_or("unknown")),
                io_error: e,
                })?,
            root: path.as_ref().join("kvs-db-data.json").to_owned()
        };
        let mut store = KvStore {
            reader: RefCell::new(reader),
            writer,
            path: Path::new(path.as_ref()).to_owned(),
            index: Arc::new(RwLock::new(HashMap::new())),
            steal: Arc::new(RwLock::new(0)),
        };
        *store.steal.write()? = store.build_index()?;
        Ok(store)
    }
}

