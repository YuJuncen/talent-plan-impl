use std::fs;
use std::process::exit;

use structopt::StructOpt;

use kvs::{KvError, KvStore, Result};

#[derive(Debug, StructOpt)]
#[structopt(name = "kvs",
  about = env!("CARGO_PKG_DESCRIPTION"),
  author = env!("CARGO_PKG_AUTHORS"),
  version = env!("CARGO_PKG_VERSION"))]
enum KVOpt {
    Set {
        /// a key string to put.
        key: String,
        /// a value string to put with the key.
        value: String,
    },
    Get {
        /// a key string to get.
        key: String,
    },
    Rm {
        /// a key string to remove.
        key: String,
    },
}

fn main() -> Result<()> {
    use KVOpt::*;
    let opt = KVOpt::from_args();
    let db_folder = std::env::current_dir().unwrap();
    if !db_folder.exists() {
        fs::create_dir(&db_folder).unwrap();
    }
    let mut store = KvStore::open(&db_folder)?;
    match opt {
        Set { key, value } => {
            store.set(key, value)?;
            exit(0);
        }
        Get { key } => {
            let result = store.get(key)?;
            if result.is_some() {
                print!("{}", result.unwrap());
            } else {
                print!("Key not found");
            }
            exit(0);
        }
        Rm { key } => {
            let result = store.remove(key);
            if result.is_err() {
                if let KvError::KeyNotFound = result.unwrap_err() {
                    print!("Key not found")
                }
                exit(1)
            }
            exit(0);
        }
    };
}
