use std::io::{Read, Write};
use std::path::Path;

use sled::Db;
use sled::Error::Io;

use crate::{KvError, KvsEngine};
use crate::engines::errors::KvError::IllegalWorkingDirectory;

use super::errors::Result;

pub struct SledEngine {
    db : Db
}

impl From<sled::Error> for KvError {
    fn from(error: sled::Error) -> KvError {
        KvError::Other { reason: format!("{}", error) }
    }
}

impl SledEngine {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        super::engine::check_engine::<&P>(&path, "sled")?;

        Db::open(&path)
            .map(|db| SledEngine {db} )
            .map_err(|err|
                if let Io(io_error) = err {
                    KvError::FailToOpenFile {
                        file_name: path.as_ref().to_str().unwrap_or("Unknown").to_owned(),
                        io_error
                    }
                } else {
                    KvError::Other {
                        reason: format!("{}", err)
                    }
                }
            )
    }
}

impl KvsEngine for SledEngine {
    fn get(&mut self, key: String) -> Result<Option<String>> {
        let result = self.db.get(key)?;
        if let Some(v) = result {
            return Ok(Some(String::from_utf8(v.to_owned().to_vec())
                .map_err(|utf8_error| KvError::Other
                    {reason: format!("decode from sled binary failed since: {}", utf8_error)})?));
        }
        self.db.flush()?;
        Ok(None)
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.db.insert(key, value.as_str())?;
        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let result = match self.db.remove(key)? {
            None => Err(KvError::KeyNotFound),
            Some(_) => Ok(())
        };
        self.db.flush()?;
        result
    }
}