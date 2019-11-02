pub use engines::engine::KvsEngine;
pub use engines::errors::{KvError, Result};
pub use engines::kvs::KvStore;

/// About the TCP-based contract.
pub mod contract;
/// About the KvEngine abstract.
pub mod engines;
/// The thread pools.
pub mod thread_pool;
/// Common part of server.
pub mod server_common;
/// the default config of server.
pub mod config;
/// Common part of benchmarking.
pub mod benchmark_common;