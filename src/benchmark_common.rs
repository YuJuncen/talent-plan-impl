use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::{Arc, atomic::Ordering, Condvar, Mutex, RwLock};

use assert_cmd::prelude::CommandCargoExt;
use crossbeam_utils::sync::WaitGroup;
use failure::_core::hash::BuildHasher;
use failure::_core::sync::atomic::AtomicBool;
use rand::prelude::IteratorRandom;
use rand::thread_rng;

use crate::{KvError, KvsEngine};
use crate::server_common::{Engine, Pool};
use crate::thread_pool::ThreadPool;

/// The Future Monad, but it's blocking.
/// It likes `Future` of Java more,
/// instead of the name `Promise` we talk in some languages' functional part
/// like ECMAScript, which's behavior like Monad more
/// (using `then` method instead of language builtin control-flow to combine),
/// and is non-blocking.
#[derive(Clone, Default)]
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
    /// ```no-run
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

    /// test whether the promise is fulfilled.
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
/// The engine that wraps a remote `kvs-server`.
/// When query method called, it trivially send a request to the remote server.
pub struct RemoteEngine {
    remote: SocketAddr,
}

impl Default for RemoteEngine {
    fn default() -> Self {
        RemoteEngine {
            remote: SocketAddr::new("127.0.0.1".parse().unwrap(), 4000),
        }
    }
}

impl RemoteEngine {
    /// Create a new `RemoteEngine` that bind to the default server running on localhost.
    /// This method won't start server, if you need to start a server, use `spawn_new` instead.
    pub fn new() -> Self {
        Default::default()
    }

    /// create a new `RemoteEngine` that bind to the specified server.
    /// This method won't start server, if you need to start a server, use `spawn_new` instead.
    pub fn with_remote(remote: SocketAddr) -> Self {
        RemoteEngine { remote }
    }

    /// spawn a new server at the addr, with specified storage engine and thread pool.
    ///
    /// if the `addr` is `None`, use the default server address(localhost:4000).
    ///
    /// # Example
    /// This will start a new server at localhost:4000, and return a `RemoteEngine` bind to it,
    /// with default config(KvStore, SharedQueueThreadPool).
    /// ```no-run
    /// let engine = spawn_new(None, Default::default(), Default::default());
    /// ```
    pub fn spawn_new(addr: Option<SocketAddr>, engine: Engine, pool: Pool) -> Self {
        let addr = addr.unwrap_or_else(|| "127.0.0.1:4000".parse().unwrap());
        std::process::Command::cargo_bin("kvs-server")
            .unwrap()
            .args(&[
                "--engine",
                engine.as_ref(),
                "--pool",
                pool.as_ref(),
                "--addr",
                addr.to_string().as_str(),
            ])
            .spawn()
            .unwrap();
        RemoteEngine { remote: addr }
    }
}

impl KvsEngine for RemoteEngine {
    fn get(&self, key: String) -> Result<Option<String>, KvError> {
        let x = std::process::Command::cargo_bin("kvs-client")
            .unwrap()
            .args(&[
                "get",
                "--addr",
                format!("{}", self.remote).as_str(),
                key.as_str(),
            ])
            .output()?;
        let mut result = String::from_utf8(x.stdout).map_err(|err| KvError::Other {
            reason: format!("{}", err),
        })?;
        if result == "Key not found\n" {
            Ok(None)
        } else {
            result.pop();
            Ok(Some(result))
        }
    }

    fn set(&self, key: String, value: String) -> Result<(), KvError> {
        let x = std::process::Command::cargo_bin("kvs-client")
            .unwrap()
            .args(&[
                "set",
                "--addr",
                format!("{}", self.remote).as_str(),
                key.as_str(),
                value.as_str(),
            ])
            .output()?;
        if x.status.success() {
            Ok(())
        } else {
            Err(KvError::Other {
                reason: "failed to execute `set` command.".to_owned(),
            })
        }
    }

    fn remove(&self, key: String) -> Result<(), KvError> {
        let output = std::process::Command::cargo_bin("kvs-client")
            .unwrap()
            .args(&[
                "rm",
                "--addr",
                format!("{}", self.remote).as_str(),
                key.as_str(),
            ])
            .output()?;
        if output.status.success() {
            Ok(())
        } else {
            Err(KvError::KeyNotFound)
        }
    }
}

/// insert fix size of keys into a `KvsEngine`.
/// it grantees that, for all `n` in the set this function returns,
/// `store.get(format!("Key{}", n)) == format!("Value{}", n)`
pub fn insert_keys(
    store: impl KvsEngine,
    pool: &impl ThreadPool,
    key_size: usize,
) -> Arc<RwLock<HashSet<usize>>> {
    let keys = Arc::new(RwLock::new(HashSet::new()));
    let wg: WaitGroup = WaitGroup::new();
    for i in 0..key_size {
        pool.spawn({
            let wg = wg.clone();
            let store = store.clone();
            let keys = keys.clone();
            move || {
                let v = i;
                keys.write().unwrap().insert(v);
                store
                    .set(format!("Key{}", v), format!("Value{}", v))
                    .unwrap();
                drop(wg);
            }
        });
    }
    wg.wait();
    keys
}

/// read a fixed size of keys from `store`.
/// This implies that for all `n` in the `keys` set,
/// `store.get(format!("Key{}", n)) == format!("Value{}", n)`(*).
///
/// # Panics
///
/// When the constraint (*) is broken.
pub fn read_exist<S: BuildHasher + Sync + Send + 'static>(
    store: impl KvsEngine,
    pool: &impl ThreadPool,
    times: usize,
    keys: Arc<RwLock<HashSet<usize, S>>>,
) {
    let wg = WaitGroup::new();
    let success = Arc::new(AtomicBool::new(true));
    for _ in 0..times {
        let keys = keys.clone();
        let store = store.clone();
        let success = success.clone();
        let wg = wg.clone();
        pool.spawn(move || {
            let guard = keys.read().unwrap();
            let k = guard.iter().choose(&mut thread_rng()).unwrap();
            let v = store.get(format!("Key{}", *k)).unwrap().unwrap();
            if v != format!("Value{}", k) {
                success.store(false, Ordering::SeqCst)
            }
            drop(wg);
        })
    }
    wg.wait();
    assert!(success.load(Ordering::SeqCst));
}
