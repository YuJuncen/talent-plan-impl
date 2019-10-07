use std::io;

use kvs::contract::KvContractMessage;
use kvs::engines::engine::KvsEngine;
use kvs::KvStore;
use kvs::Result;

fn main() -> Result<()> {
    let mut engine = KvStore::open(std::env::current_dir().unwrap())?;
    engine.set("hello".to_owned(), "world".to_owned())?;
    dbg!(engine.get("hello".to_owned())?);
    Ok(())
}
