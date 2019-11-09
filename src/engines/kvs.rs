use core::sync::atomic::Ordering;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::collections::hash_map::RandomState;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::iter::FromIterator;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use atomic::Atomic;
use concurrent_hashmap::ConcHashMap;
use regex::Regex;
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

use lazy_static::lazy_static;

use crate::common::SeekExt;
use crate::engines::engine::KvsEngine;

use super::engine;
use super::errors::{KvError, Result};
use super::errors::KvError::{FailToOpenFile, KeyNotFound};

use self::KvCommand::{Put, Rm};

fn filename_of(epoch: u64) -> String {
    format!("kvs-data-{}", epoch)
}

fn into_result<T>(option: Option<T>) -> std::result::Result<T, ()> {
    match option {
        Some(x) => Ok(x),
        None => Err(())
    }
}

fn parse_gen(filename: &str) -> Option<u64> {
    lazy_static! {
        static ref PATTERN: Regex = Regex::new(r"^kvs-data-\{(\d+)}$").unwrap();
    }
    for cap in PATTERN.captures_iter(filename) {
        return Some(format!("{}", &cap[1]).parse().unwrap())
    }
    None
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone, Copy)]
struct BinLocation {
    offset: usize,
    length: usize,
    epoch: u64
}

macro_rules! bin_loc {
    (Gen[$gen: expr] $start: expr => $len: expr ) => {
        BinLocation {
            epoch: $gen,
            offset: $start,
            length: $len
        }
    };
}

#[derive(Clone)]
pub struct KvStore {
    index: Arc<ConcHashMap<String, BinLocation>>,
    reader: RefCell<KvReader>,
    writer: Arc<Mutex<KvWriter>>,
    current_epoch: Arc<Atomic<u64>>,
    path: PathBuf,
    steal: Arc<RwLock<usize>>,
}

struct KvWriter {
    file: File,
    path: PathBuf,
}

impl KvWriter {
    pub fn write_command(&mut self, command: KvCommand) -> Result<BinLocation> {
        let serialized = Self::serialize_command(&command);
        let writer = &mut self.file;
        let offset = writer.seek_to_end()?;
        writer.write_all(serialized.as_bytes())?;
        writer.flush()?;
        Ok(bin_loc! { Gen[1] offset => serialized.as_bytes().len() })
    }

    pub fn open(p: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(p.as_ref().join("kvs-db-data.json"))
            .map_err(|e| KvError::FailToOpenFile {
                file_name: String::from(p.as_ref().to_str().unwrap_or("unknown")),
                io_error: e,
            })?;
        Ok(KvWriter {
            file,
            path: p.as_ref().to_owned(),
        })
    }

    pub fn set_epoch() {}

    /// support method for serialize one command.
    pub fn serialize_command(command: &KvCommand) -> String {
        let mut serialized = serde_json::to_string(&command).unwrap();
        serialized.push('\n');
        serialized
    }
}

struct KvReader {
    readers: BTreeMap<u64, File>,
    current_epoch: Arc<Atomic<u64>>,
    root: PathBuf,
}


impl Clone for KvReader {
    fn clone(&self) -> Self {
        KvReader::open(self.root.clone(), self.current_epoch.clone()).unwrap()
    }
}

impl KvReader {
    fn open_epoch(&mut self, epoch: u64) -> Result<&mut File> {
        if epoch < self.current_epoch.load(Ordering::SeqCst) {
            panic!("KV_READER: trying to open an file that elder than current epoch!");
        }
        Ok(self.readers.entry(epoch).or_insert(OpenOptions::new()
            .read(true)
            .open(filename_of(epoch))
            .map_err(|e| KvError::FailToOpenFile {
                file_name: filename_of(epoch),
                io_error: e,
            })?))
    }

    fn drop_epoch(&mut self, epoch: u64) -> Result<()> {
        self.readers.remove(&epoch);
        Ok(())
    }

    fn forget_old_time(&mut self) {
        let epoch = self.current_epoch.load(Ordering::SeqCst);
        let epochs = self.readers.keys()
            .filter(|&&x| x < epoch)
            .map(ToOwned::to_owned)
            .collect::<Vec<u64>>();
        for i in epochs {
            self.drop_epoch(i);
        }
    }

    /// load a command from one `BinLocation`.
    pub fn load_command(&mut self, location: BinLocation) -> Result<KvCommand> {
        self.forget_old_time();

        let reader = self.open_epoch(location.epoch)?;
        reader.seek_to(location.offset);
        let mut buf = vec![0u8; location.length];
        reader.read(buf.as_mut_slice())?;
        let result = serde_json::from_slice(buf.as_slice())?;
        Ok(result)
    }

    pub fn open(path: impl AsRef<Path>, epoch: Arc<Atomic<u64>>) -> Result<Self> {
        Ok(KvReader {
            readers: BTreeMap::new(),
            root: path.as_ref().to_owned(),
            current_epoch: epoch
        })
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
        }.as_str()
    }
}

impl KvsEngine for KvStore {
    /// get a value from the KvStore.
    ///
    /// # Error
    ///
    /// when IO/serialize error happens during read data before the log, we will
    fn get(&self, key: String) -> Result<Option<String>> {
        let cache = self.index.find(key.as_str());
        if cache.is_none() {
            return Ok(None);
        }
        let pos = cache.unwrap();
        let cmd = self.reader.borrow_mut().load_command(pos.get().clone())?;
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
        if self.index.find(key.as_str()).is_none() {
            return Err(KeyNotFound);
        }

        let command = KvCommand::remove(key.clone());
        self.save_command(command)?;
        Ok(())
    }

}

struct InitIndex {
    index: ConcHashMap<String, BinLocation>,
    epoch: u64,
    steal: usize,
}

impl InitIndex {
    fn new() -> Self {
        InitIndex {
            index: ConcHashMap::<_, _, RandomState>::new(),
            epoch: 0,
            steal: 0,
        }
    }

    fn override_record(&mut self, key: &str, new: BinLocation) -> Option<usize> {
        self.index.insert(key.to_owned(), new)
            .map(|old| old.length)
    }
}

impl KvStore {
    fn enumerate_epoch_files(p: impl AsRef<Path>) -> impl Iterator<Item=(PathBuf, u64)> {
        WalkDir::new(p)
            .into_iter()
            .filter(|entry| entry
                .as_ref()
                .map_err(|_| ())
                .and_then(|entry|
                    into_result(entry.file_name()
                        .to_str()
                        .map(|s| s.starts_with("kvs-data-"))))
                .unwrap_or(false))
            .map(|file| {
                let file = file.unwrap();
                let path = file.path().to_owned();
                let gen = file.file_name().to_str().and_then(parse_gen).unwrap();
                (path, gen)
            })
    }

    /// build the in-memory index from file.
    fn build_index(path: impl AsRef<Path>) -> Result<InitIndex> {
        let entries: Vec<(PathBuf, u64)> = KvStore::enumerate_epoch_files(path).collect();
        let mut res = InitIndex::new();
        for (filename, epoch) in entries {
            let mut buf = String::new();
            let mut reader = BufReader::new(File::open(filename)?);
            let mut x;
            if epoch > res.epoch { res.epoch = epoch; }
            while {
                x = reader.read_line(&mut buf)?;
                x > 0
            } {
                let json: KvCommand = serde_json::from_slice(buf.as_bytes())?;
                let offset = reader.current_position()?;
                if let Some(n) = res.override_record(json.key(), bin_loc! {Gen[epoch] offset - x => x }) {
                    res.steal += n
                };
                buf.clear();
            }
        }
        Ok(res)
    }

    fn override_record(&self, key: &str, location: BinLocation) -> Option<usize> {
        let idx = self.index.as_ref();
        idx.insert(key.to_owned(), location)
            .map(|old| old.length)
    }

    fn add_steal(&self, size: usize) -> Result<()> {
        let mut lock = self.steal.write()?;
        let origin = *lock;
        *lock = origin + size;
        Ok(())
    }

    fn get_steal(&self) -> Result<usize> {
        let lock = self.steal.read()?;
        Ok(*lock)
    }

    fn reset_steal(&self) -> Result<()> {
        let mut lock = self.steal.write()?;
        *lock = 0;
        Ok(())
    }

    /// save a command into data file, and update the index.
    fn save_command(&self, command: KvCommand) -> Result<()> {
        let mut writer = self.writer.lock()?;
        let key = command.key().to_owned();
        let new = writer.write_command(command)?;
        if let Some(n) = self.override_record(key.as_str(), new) {
            self.add_steal(n)?;
            if self.get_steal()? > Self::STEAL_THRESHOLDS {
                drop(writer);
                self.compact_file()?;
            }
        };
        Ok(())
    }

    /// Compact the file.
    /// This will merge all the indices, only save the last put or rm operation in the log.
    /// This should be called maybe, so that the log file will not grow too fast.
    fn compact_file(&self) -> Result<()> {
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
        *writer = KvWriter::open(self.path.to_owned())?;
        self.reset_steal()?;
        Ok(())
    }

    /// write the compacted data file into an stream.
    fn compact_file_to(&self, temp_file: &mut (impl Write + Seek)) -> Result<()> {
        let idx = self.index.as_ref();
        for (k, v) in idx.clone().iter() {
            // we deserialize the stream so that we are able to check consistency.
            let command = self.reader.borrow_mut().load_command(*v)?;
            if command.key() != k.as_str() {
                panic!("Failed in check consistency between in-memory index and disk file: the file has key {}, but the index has key {}.", command.key(), k.as_str());
            }
            let serialized = KvWriter::serialize_command(&command);
            idx.insert(
                k.to_owned(),
                BinLocation {
                    offset: temp_file.current_position()?,
                    length: serialized.len(),
                    epoch: 1
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
        let writer = Arc::new(Mutex::new(KvWriter::open(path.as_ref())?));
        let init = KvStore::build_index(path.as_ref())?;
        let epoch = Arc::new(Atomic::<u64>::new(init.epoch));
        let reader = KvReader::open(path.as_ref(), epoch.clone())?;
        let mut store = KvStore {
            reader: RefCell::new(reader),
            writer,
            current_epoch: epoch,
            path: Path::new(path.as_ref()).to_owned(),
            index: Arc::new(init.index),
            steal: Arc::new(RwLock::new(init.steal)),
        };
        Ok(store)
    }
}

