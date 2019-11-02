use std::collections::HashSet;
use std::net::SocketAddr;
use std::process::Command;
use std::sync::{Arc, Condvar, Mutex, RwLock};

use assert_cmd::cargo::cargo_bin;
use assert_cmd::prelude::CommandCargoExt;
use crossbeam_utils::sync::WaitGroup;
use rand::{Rng, thread_rng};
use rand::prelude::IteratorRandom;

use crate::{KvError, KvsEngine};
use crate::engines::errors::KvError::Other;
use crate::server_common::{Engine, Pool};
use crate::thread_pool::ThreadPool;

/// The Future Monad, but it's blocking.
/// It likes `Future` of Java more,
/// instead of the name `Promise` we talk in some languages' functional part
/// like ECMAScript, which's behavior like Monad more
/// (using `then` method instead of language builtin control-flow to combine),
/// and is non-blocking.
#[derive(Clone)]
pub struct Promise<T> {
    item: Arc<(Mutex<Option<T>>, Condvar)>,
}

impl<T> Promise<T> {
    /// Create an empty Promise.
    pub fn new() -> Self {
        Promise {
            item: Arc::new((Mutex::new(None), Condvar::new())),
        }
    }

    /// Fulfill an Promise.
    ///
    /// # Example
    /// ```rust
    /// # use std::thread;
    /// # use kvs::benchmark_common::Promise;
    /// let promise = Promise::new();
    /// thread::spawn({
    ///     let promise = promise.clone();
    ///     move || {
    ///         promise.fulfill(42);
    ///     }
    /// });
    /// assert_eq!(42, promise.get());
    /// ```
    pub fn fulfill(&self, item: T) {
        let mut l = self.item.0.lock().unwrap();
        *l = Some(item);
        self.item.1.notify_one();
    }

    pub fn is_fulfill(&self) -> bool {
        self.item.0.lock().unwrap().is_some()
    }

    /// blocking the current thread until the promise is fulfilled.
    pub fn get(&self) -> T {
        let l = self.item.0.lock().unwrap();
        let mut l = self.item.1.wait(l).unwrap();
        l.take().unwrap()
    }
}

#[derive(Clone, Debug)]
pub struct RemoteEngine {
    remote: SocketAddr
}

impl RemoteEngine {
    pub fn new() -> Self {
        RemoteEngine {
            remote: SocketAddr::new("127.0.0.1".parse().unwrap(), 4000)
        }
    }

    pub fn with_remote(remote: SocketAddr) -> Self {
        RemoteEngine {
            remote
        }
    }

    pub fn spawn_new(addr: SocketAddr, engine: Engine, pool: Pool) -> Self {
        std::process::Command::cargo_bin("kvs-server")
            .unwrap()
            .args(&["--engine", engine.as_ref(), "--pool", pool.as_ref(), "--addr", addr.to_string().as_str()])
            .spawn();
        RemoteEngine { remote: addr }
    }
}

impl KvsEngine for RemoteEngine {
    fn get(&self, key: String) -> Result<Option<String>, KvError> {
        let x = std::process::Command::cargo_bin("kvs-client")
            .unwrap()
            .args(&["--addr", format!("{}", self.remote).as_str()])
            .output()?;
        let result = String::from_utf8(x.stdout)
            .map_err(|err| KvError::Other { reason: format!("{}", err) })?;
        if result == "Key not found\n" {
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }

    fn set(&self, key: String, value: String) -> Result<(), KvError> {
        let x = std::process::Command::cargo_bin("kvs-client")
            .unwrap()
            .args(&["--addr", format!("{}", self.remote).as_str(), "set", key.as_str(), value.as_str()])
            .output()?;
        if x.status.success() {
            Ok(())
        } else {
            Err(KvError::Other { reason: "failed to execute `set` command.".to_owned() })
        }
    }

    fn remove(&self, key: String) -> Result<(), KvError> {
        let output = std::process::Command::cargo_bin("kvs-client")
            .unwrap()
            .args(&["--addr", format!("{}", self.remote).as_str(), "rm", key.as_str()])
            .output()?;
        if output.status.success() {
            Ok(())
        } else {
            Err(KvError::KeyNotFound)
        }
    }
}

pub fn insert_keys(store: impl KvsEngine, pool: &impl ThreadPool, key_size: usize, key_range: (usize, usize))
                   -> Arc<RwLock<HashSet<usize>>> {
    let mut keys = Arc::new(RwLock::new(HashSet::new()));
    let wg: WaitGroup = WaitGroup::new();
    for i in 0..key_size {
        pool.spawn({
            let wg = wg.clone();
            let store = store.clone();
            let keys = keys.clone();
            move || {
                let v = thread_rng().gen_range(key_range.0, key_range.1);
                keys.write().unwrap().insert(v);
                store.set(format!("Key{}", v), format!("Value{}", v)).unwrap();
                drop(wg);
            }
        });
    }
    wg.wait();
    keys
}

pub fn read_exist(store: impl KvsEngine, pool: &impl ThreadPool, times: usize, keys: Arc<RwLock<HashSet<usize>>>) {
    let promises: Vec<Promise<_>> = (0..times).map(|_| Promise::new()).collect();
    for i in 0..times {
        let promise = promises[i].clone();
        let keys = keys.clone();
        let store = store.clone();
        pool.spawn(move || {
            let guard = keys.read().unwrap();
            let k = guard.iter().choose(&mut thread_rng()).unwrap();
            let v = store.get(format!("Key{}", *k)).unwrap().unwrap();
            promise.fulfill(v);
        })
    }
    assert!(promises.iter().all(|promise| {
        let k = promise.get();
        k == format!("Value{}", k[3..].to_owned())
    }));
}
