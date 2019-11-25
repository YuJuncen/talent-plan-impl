use std::io::{Read, Write};
use std::path::Path;

use crate::engines::errors::KvError::IllegalWorkingDirectory;

use super::errors::Result;

pub(crate) fn check_engine<P: AsRef<Path>>(path: P, engine_name: &str) -> Result<()> {
    if std::fs::metadata(path.as_ref().join(".engine")).is_err() {
        let mut f = std::fs::File::create(path.as_ref().join(".engine"))?;
        f.write_all(engine_name.as_bytes())?;
    }
    let mut f = std::fs::File::open(path.as_ref().join(".engine"))?;
    let mut buf = String::new();
    f.read_to_string(&mut buf)?;
    if buf.to_lowercase().as_str() != engine_name {
        return Err(IllegalWorkingDirectory);
    }
    Ok(())
}

/// The engine of out `KvServer`.
/// This is the basic abstract of an Key-value database.
///
/// This is a sub-trait of `Send` and `Clone`, so that is can be simply send between threads.
/// It grantees that it's cheap to `Clone` it, so you needn't share it with `Arc`.
///
/// The semantic of `get`, `set`, `remove` are same as what you thinks.
pub trait KvsEngine: Send + Clone + 'static {
    /// get value from store by key.
    /// when the key not exists, return `None`.
    fn get(&self, key: String) -> Result<Option<String>>;
    /// set value to store with specified key.
    fn set(&self, key: String, value: String) -> Result<()>;
    /// remove the key from the store.
    ///
    /// # Error
    ///
    /// When the key not found, it should throw `KeyNotFound`.
    fn remove(&self, key: String) -> Result<()>;
}