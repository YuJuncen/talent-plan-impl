use std::collections::HashMap;

#[derive(Debug, Default)]
/// The in-memory storage engine.
pub struct KvStore {
    store: HashMap<String, String>,
}

impl KvStore {
    /// Create a new KvStore, with empty content.
    /// ```rust
    /// # use kvs::KvStore;
    /// // `store` will be an empty KvStore.
    /// let store = KvStore::new();
    /// // You can use the store then.
    /// ```
    pub fn new() -> Self {
        KvStore {
            store: HashMap::new(),
        }
    }

    /// get a value from the KvStore.
    /// ```rust
    /// # use kvs::KvStore;
    /// # let mut store = KvStore::new();
    /// assert_eq!(store.get("not-included".to_owned()), None);
    /// store.set("some-field".to_owned(), "42".to_owned());
    /// assert_eq!(store.get("some-field".to_owned()), Some("42".to_owned()));
    /// ```
    pub fn get(&self, key: String) -> Option<String> {
        self.store.get(key.as_str()).map(Clone::clone)
    }

    /// Put a value into the KvStore.
    /// ```rust
    /// # use kvs::KvStore;
    /// # let mut store = KvStore::new();
    /// assert_eq!(store.get("some-field".to_owned()), None);
    /// store.set("some-field".to_owned(), "42".to_owned());
    /// assert_eq!(store.get("some-field".to_owned()), Some("42".to_owned()));
    /// ```
    pub fn set(&mut self, key: String, value: String) {
        self.store.insert(key, value);
    }

    /// Remove an value from the KvStore
    /// ```rust
    /// # use kvs::KvStore;
    /// # let mut store = KvStore::new();
    /// # store.set("some-field".to_owned(), "42".to_owned());
    /// assert_eq!(store.get("some-field".to_owned()), Some("42".to_owned()));
    /// store.remove("some-field".to_owned());
    /// assert_eq!(store.get("some-field".to_owned()), None);
    /// ```
    pub fn remove(&mut self, key: String) -> Option<String> {
        self.store.remove(key.as_str())
    }
}