use kvs::{KvStore, Result};

fn main() -> Result<()> {
    let _ = std::fs::File::create("foo.txt").unwrap();
    let mut store = KvStore::open("foo.txt")?;
    store.set("Hello".to_owned(), "world".to_owned())?;
    println!("{:?}", store.get("Hello".to_owned()));
    store.set("universe".to_owned(), "42".to_owned())?;
    store.remove("Hello".to_owned())?;
    println!("{:?}", store.get("universe".to_owned()));
    println!("{:?}", store.get("Hello".to_owned()));
    store.compact_file()?;
    Ok(())
}