//! # KvStore
//! A key-value database.
//!
//! ## quick start
//! ```bash
//! # to get value of $KEY_NAME.
//! cargo run --bin kvs -- get $KEY_NAME
//! # to set key $KEY_NAME as $KEY_VALUE.
//! cargo run --bin kvs -- set $KEY_NAME $VALUE
//! # to remove key $KEY_NAME.
//! cargo run --bin kvs -- rm $KEY_NAME
//! ```
//! Use `--help` to learn more.
#![deny(missing_docs)]
#![deny(warnings)]

pub use self::errors::{KvError, Result};
pub use self::kv::KvStore;

mod errors;
mod kv;
