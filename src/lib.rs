//! # KvStore
//! A key-value database server.
//! with store engine abstract, it can run based on `sled` or default engine `kvs`.
//! `sled` engine uses by LSM-tree index, and `kvs` engine uses hash index.
//!
//! ## quick start
//! This project provides 2 CLIs: `kvs-server`, `kvs-client`, and its name reveals its usage.
//! ### server
//! ```bash
//! # to start server
//! cargo run --bin kvs-server
//! ```
//! This command will run the kvs-server with default engine `kvs` and listen on `localhost:4000`.
//! Use `--help` to learn more.
//!
//! ### client
//! ```bash
//! # to get value of $KEY_NAME.
//! cargo run --bin kvs-client -- get $KEY_NAME
//! # to set key $KEY_NAME as $KEY_VALUE.
//! cargo run --bin kvs-client -- set $KEY_NAME $VALUE
//! # to remove key $KEY_NAME.
//! cargo run --bin kvs-client -- rm $KEY_NAME
//! ```
//! All operations will be performed on server at `localhost:4000`.
//! Use `--help` to learn more.


#![deny(warnings)]
#![deny(missing_docs)]

pub use engines::engine::KvsEngine;
pub use engines::errors::{KvError, Result};
pub use engines::kvs::KvStore;

/// Common part of benchmarking.
pub mod benchmark_common;
mod common;
/// the default config of server.
pub mod config;
/// About the TCP-based contract.
pub mod contract;
/// About the KvEngine abstract.
pub mod engines;
/// Common part of server.
pub mod server_common;
/// The thread pools.
pub mod thread_pool;
