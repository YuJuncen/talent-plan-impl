use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

use failure::_core::cell::Ref;
use failure::_core::intrinsics::offset;
use serde::{Deserialize, Serialize};

use crate::errors::KvError::{FailToAppendFile, FailToReadFile};

use super::{KvError, Result};

#[derive(Debug, Eq, Ord)]
struct BinLocation {
    offset : u64,
    length : usize
}

pub struct KvStore {
    index: HashMap<String, BinLocation>,
    file: RefCell<File>,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize, Debug)]
enum KvCommand {
    Put { key : String, value: String },
    Rm { key : String }
}

impl KvStore {

    /// make an KvStore by an database file.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
                .append(true)
                .read(true)
                .open(path)
            .map_err(|e| KvError::FailToOpenFile { file_name: path, io_error: e })?;
        Ok(KvStore {
            file: RefCell::new(file),
            index: HashMap::new()
        })
    }

    /// get a value from the KvStore.
    /// ```rust
    /// # use kvs::{KvStore, KvError};
    /// # let mut store = KvStore::new();
    /// assert_eq!(store.get("not-included".to_owned()), Err(KvError::KeyNotFound {key : "not-included".to_owned()}));
    /// store.set("some-field".to_owned(), "42".to_owned());
    /// assert_eq!(store.get("some-field".to_owned()), Ok("42".to_owned()));
    /// ```
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(BinLocation {offset, length}) = self.index.get(key.as_str()) {
            let mut file = &mut *self.file.borrow_mut();
            file.seek(SeekFrom::Start(*offset));
            let mut buf = [0u8; length];
            file.read(&mut buf);
            let json : KvCommand = serde_json::from_slice(&buf).map_err(|serde_error| KvError::FailToParseFile { serde_error })?;
            let result = match json {
                KvCommand::Put {..} => Some(value),
                KvCommand::Rm {..} => None
            };
            return Ok(result)
        }

        let mut reader = BufReader::new(&mut *self.file.borrow_mut());
        let mut result = None;
        while let command = serde_json::de::from_reader(&mut reader).map_error(|serde_error| KvError::FailToParseFile { serde_error }) {
            match command {
                KvCommand::Put { key: key_read , value} => if key == key_read {
                    self.index.entry(key_read).or_insert(reader.stream_position()?);
                    result = value;
                }
                KvCommand::Rm { key: key_read} => if key == key_read {
                    result = None;
                }
            }
        }
        Ok(result)
    }

    /// Put a value into the KvStore.
    /// ```rust
    /// # use kvs::{KvStore, KvError};
    /// # let mut store = KvStore::new();
    /// assert_eq!(store.get("some-field".to_owned()), Ok(None));
    /// store.set("some-field".to_owned(), "42".to_owned());
    /// assert_eq!(store.get("some-field".to_owned()), Ok(Some("42".to_owned())));
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = KvCommand::Put { key : key.clone(), value };
        let serialized = serde_json::to_string(command).unwrap();
        let mut file = self.file.borrow_mut();
        let offset = file.stream_position().map_err(|io_error| FailToAppendFile { io_error })?;
        let length = file.write(serialized.as_bytes()).map_err(|io_error| FailToAppendFile { io_error })?;
        self.index.insert(key, BinLocation { offset, length });
        Ok(())
    }

    /// Remove an value from the KvStore
    /// ```rust
    /// # use kvs::{KvStore, KvError};
    /// # let mut store = KvStore::new();
    /// # store.set("some-field".to_owned(), "42".to_owned());
    /// assert_eq!(store.get("some-field".to_owned()), Ok("42".to_owned()));
    /// store.remove("some-field".to_owned());
    /// assert_eq!(store.get("some-field".to_owned()), Err(KvError::KeyNotFound {key : "some-field".to_owned()}));
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.store.remove(key.as_str());
        Ok(())
    }


}
