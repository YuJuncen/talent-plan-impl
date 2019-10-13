use std::io::Read;
use std::path::Path;
use std::sync::{Arc, RwLock};

use sled::Db;
use sled::Error::Io;

use crate::{KvError, KvsEngine};

use super::errors::Result;

#[derive(Clone)]
pub struct SledEngine {
    db: Arc<RwLock<Db>>
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
            .map(|db| SledEngine { db: Arc::new(RwLock::new(db)) })
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
    fn get(&self, key: String) -> Result<Option<String>> {
        let result = self.db.read().unwrap().get(key)?;
        if let Some(v) = result {
            return Ok(Some(String::from_utf8(v.to_owned().to_vec())
                .map_err(|utf8_error| KvError::Other
                    {reason: format!("decode from sled binary failed since: {}", utf8_error)})?));
        }
        self.db.write()?.flush()?;
        Ok(None)
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        self.db.write().unwrap().insert(key, value.as_str())?;
        Ok(())
    }

    fn remove(&self, key: String) -> Result<()> {
        let result = match self.db.write().unwrap().remove(key)? {
            None => Err(KvError::KeyNotFound),
            Some(_) => Ok(())
        };
        self.db.write()?.flush()?;
        result
    }
}