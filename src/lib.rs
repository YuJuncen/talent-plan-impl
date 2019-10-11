pub use engines::engine::KvsEngine;
pub use engines::errors::{KvError, Result};
pub use engines::kvs::KvStore;

/// About the TCP-based contract.
pub mod contract;
/// About the KvEngine abstract.
pub mod engines;