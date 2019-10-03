use std::collections::HashMap;
use std::env::temp_dir;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::io::SeekFrom::Current;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tempfile::TempDir;

use crate::errors::KvError::{FailToAppendFile, FailToOpenFile};

use super::{KvError, Result};

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
struct BinLocation {
    offset : usize,
}

pub struct KvStore {
    index: HashMap<String, BinLocation>,
    file: File,
    path: PathBuf
}

#[derive(Serialize, Deserialize, Debug)]
enum KvCommand {
    Put { key : String, value: String },
    Rm { key : String }
}

impl KvCommand {
    fn key(&self) -> &str {
        match self {
            KvCommand::Put {key, ..} => key,
            KvCommand::Rm {key} => key
        }.as_str()
    }
}

impl KvStore {
    fn build_index(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(0)).map_err(|io_error| KvError::FailToReadFile { io_error })?;
        let mut reader = BufReader::new(&mut self.file);
        let mut buf = String::new();
        let mut x;
        while {
            x = reader.read_line(&mut buf).map_err(|io_error| KvError::FailToReadFile { io_error })?;
            x > 0
        } {
            let json = serde_json::from_slice(buf.as_bytes()).map_err(|serde_error| KvError::FailToParseFile { serde_error })?;
            match json {
                KvCommand::Put { key: key_read, .. } => {
                    let offset = reader.stream_position().map_err(|io_error| KvError::FailToReadFile { io_error })? as usize;
                    self.index.entry(key_read).or_insert(BinLocation { offset: offset - x });
                },
                KvCommand::Rm { key: key_read } => {
                    let offset = reader.stream_position().map_err(|io_error| KvError::FailToReadFile { io_error })? as usize;
                    self.index.entry(key_read).or_insert(BinLocation { offset: offset - x });
                }
            }
            buf.clear();
        }
        Ok(())
    }

    fn load_command(&mut self, offset: usize) -> Result<KvCommand> {
        self.file.seek(SeekFrom::Start(offset as u64)).map_err(|io_error| KvError::FailToReadFile { io_error })?;
        let mut reader = BufReader::new(&mut self.file);
        let mut buf = String::new();
        reader.read_line(&mut buf).map_err(|io_error| KvError::FailToReadFile { io_error })?;
        serde_json::from_slice(buf.as_bytes()).map_err(|serde_error| KvError::FailToParseFile { serde_error })
    }

    fn save_command(&mut self, command: KvCommand) -> Result<()> {
        let serialized = Self::serialize_command(&command);
        let offset = self.file.seek(SeekFrom::Current(0)).map_err(|io_error| FailToAppendFile { io_error })? as usize;
        self.file.write(serialized.as_bytes()).map_err(|io_error| FailToAppendFile { io_error })?;
        let key = command.key().to_owned();
        self.index.insert(key, BinLocation { offset });
        Ok(())
    }

    fn serialize_command(command: &KvCommand) -> String {
        let mut serialized = serde_json::to_string(&command).unwrap();
        serialized.push('\n');
        serialized
    }

    /// Compact the file.
    /// This will merge all the indices, only save the last put or rm operation in the log.
    /// This should be called maybe, so that the log file will not grow too fast.
    pub fn compact_file(&mut self) -> Result<()> {
        self.build_index()?;
        let temp_dict = TempDir::new().expect("unable to open temporary folder.");
        let path = temp_dict.path();
        let mut temp_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(path)
            .map_err(|io_error| FailToOpenFile { file_name: path.to_str().unwrap_or("unknown").to_owned(), io_error})?;
        self.compact_file_to(&mut temp_file);
        std::fs::copy(path, &self.path).map_err(|io_error| FailToAppendFile {io_error})?;
        std::fs::remove_file(path).map_err(|io_error| FailToAppendFile {io_error})?;
        Ok(())
    }

    fn compact_file_to(&mut self, temp_file: &mut (impl Write + Seek)) -> Result<()> {
        let old_index = std::mem::replace(&mut self.index, HashMap::new());
        for (k, v) in old_index.iter() {
            // we deserialize the stream so that we are able to check consistency.
            let command = self.load_command(v.offset)?;
            if command.key() != k.as_str() {
                panic!("Failed in check consistency between in-memory index and disk file: the file has key {}, but the index has key {}.", command.key(), k.as_str());
            }
            let serialized = Self::serialize_command(&command);
            self.index.insert(k.to_owned(), BinLocation{offset: temp_file.seek(Current(0)).map_err(|io_error| FailToAppendFile {io_error})? as usize});
            temp_file.write(serialized.as_bytes()).map_err(|io_error| FailToAppendFile {io_error})?;
        }
        temp_file.flush().map_err(|io_error| FailToAppendFile {io_error})?;

        Ok(())
    }

    fn reopen_file(&mut self) -> Result<()> {
        self.file = OpenOptions::new()
            .append(true)
            .read(true)
            .open(&self.path)
            .map_err(|e| KvError::FailToOpenFile {
                file_name: String::from(self.path.to_str().unwrap_or("unknown")),
                io_error: e
            })?;
        Ok(())
    }

    /// make an KvStore by an database file.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new()
            .append(true)
            .read(true)
            .open(path.as_ref())
            .map_err(|e| KvError::FailToOpenFile {
                file_name: String::from(path.as_ref().to_str().unwrap_or("unknown")),
                io_error: e
            })?;
        Ok(KvStore {
            file,
            path: Path::new(path.as_ref()).to_owned(),
            index: HashMap::new()
        })
    }

    /// get a value from the KvStore.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if !self.index.contains_key(key.as_str()) {
            self.build_index()?;
        }

        let cache = self.index.get(key.as_str());
        if cache.is_none() { return Ok(None); }

        let offset = cache.unwrap().offset;
        Ok(match self.load_command(offset)? {
            KvCommand::Rm {..} => None ,
            KvCommand::Put {value, ..} => Some(value)
        })
    }

    /// Put a value into the KvStore.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = KvCommand::Put { key: key.clone(), value };
        self.save_command(command)?;
        Ok(())
    }

    /// Remove an value from the KvStore
    pub fn remove(&mut self, key: String) -> Result<()> {
        let command = KvCommand::Rm { key: key.clone() };
        self.save_command(command)?;
        Ok(())
    }
}
