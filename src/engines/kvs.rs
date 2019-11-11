use core::sync::atomic::Ordering;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::hash_map::RandomState;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, atomic::AtomicU64, Mutex, RwLock};
use std::thread;

use concurrent_hashmap::ConcHashMap;
use regex::Regex;
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

use lazy_static::lazy_static;

use crate::common::SeekExt;
use crate::engines::engine::KvsEngine;

use super::engine;
use super::errors::{KvError, Result};
use super::errors::KvError::KeyNotFound;

use self::KvCommand::{Put, Rm};

fn filename_of(epoch: u64) -> String {
    format!("kvs-data-{}", epoch)
}

fn into_result<T>(option: Option<T>) -> std::result::Result<T, ()> {
    match option {
        Some(x) => Ok(x),
        None => Err(()),
    }
}

fn read_file_of(base: impl AsRef<Path>, epoch: u64) -> Result<File> {
    let filename = base.as_ref().join(filename_of(epoch));
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(&filename)
        .map_err(|e| KvError::FailToOpenFile {
            file_name: filename_of(epoch),
            io_error: e,
        })
}

fn parse_gen(filename: &str) -> Option<u64> {
    lazy_static! {
        static ref PATTERN: Regex = Regex::new(r"^kvs-data-(\d+)$").unwrap();
    }
    PATTERN
        .captures_iter(filename)
        .next()
        .map(|cap| cap[1].to_string().parse::<u64>().unwrap())
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone, Copy)]
struct BinLocation {
    offset: usize,
    length: usize,
    epoch: u64,
}

macro_rules! bin_loc {
    (Gen[$gen: expr] $start: expr => $len: expr ) => {
        BinLocation {
            epoch: $gen,
            offset: $start,
            length: $len,
        }
    };
}

#[derive(Clone)]
/// The default engine.
///
/// It implements the in-memory Hash index like bitcask.
/// Using epoch-based garbage collection.
///
/// **Be aware**:
/// It uses internal mutability to adapt the api defined on `KvsEngine` trait.
/// (`get`, `set` and `rm` only needs `&self` instead of `&mut self`)
/// When you want to share it between threads, simply `copy` it instead of use `Arc`,
/// otherwise it may face runtime errors.
pub struct KvStore {
    index: Arc<ConcHashMap<String, BinLocation>>,
    reader: RefCell<KvReader>,
    writer: Arc<Mutex<KvWriter>>,
    current_epoch: Arc<AtomicU64>,
    tail_epoch: Arc<AtomicU64>,
    path: PathBuf,
    steal: Arc<AtomicU64>,
}

struct KvWriter {
    file: File,
    path: PathBuf,
    current_epoch: u64,
}

impl KvWriter {
    pub fn write_command(&mut self, command: KvCommand) -> Result<BinLocation> {
        let serialized = Self::serialize_command(&command);
        let writer = &mut self.file;
        let offset = writer.seek_to_end()?;
        writer.write_all(serialized.as_bytes())?;
        writer.flush()?;
        Ok(bin_loc! { Gen[self.current_epoch] offset => serialized.as_bytes().len() })
    }

    pub fn open(p: impl AsRef<Path>, gen: u64) -> Result<Self> {
        let file = read_file_of(&p, gen)?;
        Ok(KvWriter {
            file,
            path: p.as_ref().to_owned(),
            current_epoch: gen,
        })
    }

    pub fn set_epoch(&mut self, epoch: u64) -> Result<()> {
        let new_file = read_file_of(&self.path, epoch)?;
        self.file = new_file;
        self.current_epoch = epoch;
        Ok(())
    }

    /// support method for serialize one command.
    pub fn serialize_command(command: &KvCommand) -> String {
        let mut serialized = serde_json::to_string(&command).unwrap();
        serialized.push('\n');
        serialized
    }
}

struct KvReader {
    readers: BTreeMap<u64, File>,
    tail_epoch: Arc<AtomicU64>,
    root: PathBuf,
    active: Arc<ConcHashMap<u64, RwLock<()>>>,
}

impl Clone for KvReader {
    fn clone(&self) -> Self {
        KvReader::open(
            self.root.clone(),
            self.tail_epoch.clone(),
            self.active.clone(),
        )
            .unwrap()
    }
}

impl KvReader {
    fn open_epoch(
        readers: &mut BTreeMap<u64, File>,
        current_epoch: u64,
        root: impl AsRef<Path>,
        epoch: u64,
    ) -> Result<&mut File> {
        if epoch < current_epoch {
            panic!("KV_READER: trying to open an file that elder than current epoch!");
        }
        Ok(readers.entry(epoch).or_insert(
            OpenOptions::new()
                .read(true)
                .open(root.as_ref().join(filename_of(epoch)))
                .map_err(|e| KvError::FailToOpenFile {
                    file_name: filename_of(epoch),
                    io_error: e,
                })?,
        ))
    }

    // This file may be removed by other reader, so it's ok to fail to remove file.
    #[allow(unused_must_use)]
    fn drop_epoch(&mut self, epoch: u64) -> Result<()> {
        self.readers.remove(&epoch);
        let _active = self.active.clone();
        if let Some(x) = self.active.remove(&epoch) {
            let path = self.root.clone();
            thread::spawn(move || -> Result<()> {
                let _guard = x.write()?;
                std::fs::remove_file(path.join(filename_of(epoch)));
                Ok(())
            });
        }
        Ok(())
    }

    fn forget_old_time(&mut self) -> Result<()> {
        let epoch = self.tail_epoch.load(Ordering::SeqCst);
        let epochs = self
            .active
            .iter()
            .map(|acc| acc.0)
            .filter(|&&x| x < epoch)
            .map(ToOwned::to_owned)
            .collect::<Vec<u64>>();
        for i in epochs {
            self.drop_epoch(i)?;
        }
        Ok(())
    }

    /// load a command from one `BinLocation`.
    pub fn load_command(&mut self, location: BinLocation) -> Result<KvCommand> {
        self.forget_old_time()?;

        if self.active.find(&location.epoch).is_none() {
            self.active.insert(location.epoch, RwLock::new(()));
        }
        let _guard = self.active.find(&location.epoch).unwrap().get().read()?;
        let reader = KvReader::open_epoch(
            &mut self.readers,
            self.tail_epoch.load(Ordering::SeqCst),
            &self.root,
            location.epoch,
        )?;
        let mut buf = vec![0u8; location.length];
        reader.seek_to(location.offset)?;
        reader.read_exact(buf.as_mut_slice())?;
        let r = serde_json::from_slice(buf.as_slice());
        r.map_err(|e| e.into())
    }

    pub fn open(
        path: impl AsRef<Path>,
        epoch: Arc<AtomicU64>,
        active: Arc<ConcHashMap<u64, RwLock<()>>>,
    ) -> Result<Self> {
        Ok(KvReader {
            readers: BTreeMap::new(),
            root: path.as_ref().to_owned(),
            tail_epoch: epoch,
            active,
        })
    }
}

impl KvStore {
    const STEAL_THRESHOLDS: u64 = 1024 * 1024 * 8; // 8MB
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
    tail_epoch: u64,
    steal: u64,
}

impl InitIndex {
    fn new() -> Self {
        InitIndex {
            index: ConcHashMap::<_, _, RandomState>::new(),
            epoch: 0,
            tail_epoch: u64::max_value(),
            steal: 0,
        }
    }

    fn override_record(&mut self, key: &str, new: BinLocation) -> Option<u64> {
        match self.index.find(key) {
            Some(ref old) if old.get().epoch > new.epoch => Some(new.length as u64),
            any => {
                drop(any);
                self.index
                    .insert(key.to_owned(), new)
                    .map(|old| old.length as u64)
            }
        }
    }
}

impl KvStore {
    fn enumerate_epoch_files(p: impl AsRef<Path>) -> impl Iterator<Item=(PathBuf, u64)> {
        WalkDir::new(p)
            .into_iter()
            .filter(|entry| {
                entry
                    .as_ref()
                    .map_err(|_| ())
                    .and_then(|entry| {
                        into_result(
                            entry
                                .file_name()
                                .to_str()
                                .map(|s| s.starts_with("kvs-data-")),
                        )
                    })
                    .unwrap_or(false)
            })
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
        if entries.is_empty() {
            res.epoch = 1;
            res.tail_epoch = 0;
            return Ok(res);
        }

        for (filename, epoch) in entries {
            let mut buf = String::new();
            let mut reader = BufReader::new(File::open(filename)?);
            let mut x;
            if epoch > res.epoch {
                res.epoch = epoch;
            }
            if epoch < res.tail_epoch {
                res.tail_epoch = epoch;
            }
            while {
                x = reader.read_line(&mut buf)?;
                x > 0
            } {
                let json: KvCommand = serde_json::from_slice(buf.as_bytes())?;
                let offset = reader.current_position()?;
                if let Some(n) =
                res.override_record(json.key(), bin_loc! {Gen[epoch] offset - x => x })
                {
                    res.steal += n
                };
                buf.clear();
            }
        }
        Ok(res)
    }

    fn override_record(&self, key: &str, location: BinLocation) -> Option<u64> {
        let idx = self.index.as_ref();
        match idx.find(key) {
            Some(ref old) if old.get().epoch > location.epoch => Some(location.length as u64),
            any => {
                drop(any);
                idx.insert(key.to_owned(), location)
                    .map(|old| old.length as u64)
            }
        }
    }

    fn add_steal(&self, size: u64) -> Result<()> {
        self.steal.fetch_add(size, Ordering::SeqCst);
        Ok(())
    }

    fn get_steal(&self) -> Result<u64> {
        Ok(self.steal.load(Ordering::SeqCst))
    }

    fn reset_steal(&self) -> Result<()> {
        self.steal.store(0, Ordering::SeqCst);
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
        let epoch = self.current_epoch.fetch_add(2, Ordering::SeqCst);
        let compact_to_epoch = epoch + 1;
        let new_write_to_epoch = epoch + 2;
        let writer = KvWriter::open(&self.path, compact_to_epoch)?;
        self.reset_steal()?;
        let this = self.clone();
        thread::spawn(move || {
            this.compact_file_to_writer(writer).unwrap();
            this.tail_epoch.fetch_add(2, Ordering::SeqCst);
        });
        let mut w = self.writer.lock()?;
        w.set_epoch(new_write_to_epoch)?;
        Ok(())
    }

    fn compact_file_to_writer(&self, mut writer: KvWriter) -> Result<()> {
        let idx = self.index.as_ref();
        for (k, v) in idx.clone().iter() {
            let command = self.reader.borrow_mut().load_command(*v)?;
            let new_location = writer.write_command(command)?;
            self.override_record(k.as_str(), new_location);
        }
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
        let init = KvStore::build_index(path.as_ref())?;
        let writer = Arc::new(Mutex::new(KvWriter::open(path.as_ref(), init.epoch)?));
        let epoch = Arc::new(AtomicU64::new(init.epoch));
        let tail_epoch = Arc::new(AtomicU64::new(init.tail_epoch));
        let reader = KvReader::open(
            path.as_ref(),
            tail_epoch.clone(),
            Arc::new(ConcHashMap::<_, _, RandomState>::new()),
        )?;
        let store = KvStore {
            reader: RefCell::new(reader),
            writer,
            tail_epoch,
            current_epoch: epoch,
            path: Path::new(path.as_ref()).to_owned(),
            index: Arc::new(init.index),
            steal: Arc::new(AtomicU64::new(init.steal as u64)),
        };
        Ok(store)
    }
}
