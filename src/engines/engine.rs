use std::io::{Read, Write};
use std::path::Path;

use crate::engines::errors::KvError::IllegalWorkingDirectory;

use super::errors::Result;

pub(crate) fn check_engine<P: AsRef<Path>>(path: P, engine_name: &str) -> Result<()> {
    if let Err(_) = std::fs::metadata(path.as_ref().join(".engine")) {
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

pub(crate) trait KvsEngine {
    fn get(&mut self, key: String) -> Result<Option<String>>;
    fn set(&mut self, key: String, value: String) -> Result<()>;
    fn remove(&mut self, key: String) -> Result<()>;
}