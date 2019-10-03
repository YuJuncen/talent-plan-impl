use kvs::{KvStore, Result};

fn main() -> Result<()> {
    let mut store = KvStore::open("foo.txt")?;
    store.set("Hello".to_owned(), "world".to_owned());
    store.get("hello".to_owned());
    Ok(())
}