use std::net::SocketAddr;
use std::str::FromStr;

use failure::Fail;
use structopt::StructOpt;

use crate::KvError;
use crate::server_common::ServerError::{EngineError, UnsupportedContract};

#[derive(Debug, StructOpt, Clone)]
#[structopt(name = "kvs",
about = env ! ("CARGO_PKG_DESCRIPTION"),
author = env ! ("CARGO_PKG_AUTHORS"),
version = env ! ("CARGO_PKG_VERSION"))]
/// the server command line option.
pub struct ServerOpt {
    #[structopt(
    default_value = "127.0.0.1:4000",
    parse(try_from_str = str::parse),
    long = "--addr"
    )]
    /// the address to listen.
    pub addr: SocketAddr,
    #[structopt(
    default_value = "kvs",
    parse(try_from_str = str::parse),
    long = "--engine"
    )]
    /// the engine to use.
    pub engine: Engine,
    #[structopt(
    default_value = "shared_queue",
    parse(try_from_str = str::parse),
    long = "--pool"
    )]
    /// the thread pool to use.
    pub pool: Pool,
}

/// the engine of user select.
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum Engine {
    /// the `KvStore` engine.
    Kvs,
    /// the `SledEngine` engine.
    Sled,
}

impl Default for Engine {
    fn default() -> Self {
        Engine::Kvs
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy, Fail)]
#[fail(display = "No such engine")]
/// Throws when we cannot parse the command line input into an engine.
pub struct NoSuchEngine;

impl FromStr for Engine {
    type Err = NoSuchEngine;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "kvs" => Ok(Self::Kvs),
            "sled" => Ok(Self::Sled),
            _ => Err(NoSuchEngine),
        }
    }
}

impl AsRef<str> for Engine {
    fn as_ref(&self) -> &str {
        match self {
            Engine::Kvs => "kvs",
            Engine::Sled => "sled",
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
/// The thread pool type of the server.
pub enum Pool {
    /// the `NaiveThreadPool`, it just spawn new threads.
    Naive,
    /// the `RayonThreadPool`, from the `rayon` creat.
    Rayon,
    /// the `SharedQueueThreadPool`, a fixed thread pool that uses a shared, boundless queue to work.
    SharedQueue,
}

impl Default for Pool {
    fn default() -> Self {
        Pool::SharedQueue
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Fail)]
#[fail(display = "No such pool: {}", 0)]
/// Throws when we cannot parse the command line to an thread pool name.
pub struct NoSuchPool(String);

impl FromStr for Pool {
    type Err = NoSuchPool;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "naive" => Ok(Pool::Naive),
            "shared_queue" => Ok(Pool::SharedQueue),
            "rayon" => Ok(Pool::Rayon),
            _ => Err(NoSuchPool(s.to_owned())),
        }
    }
}

impl AsRef<str> for Pool {
    fn as_ref(&self) -> &str {
        match *self {
            Pool::Naive => "naive",
            Pool::Rayon => "rayon",
            Pool::SharedQueue => "shared_queue",
        }
    }
}

#[derive(Debug, Fail)]
/// the error type of `KvServer` context.
/// It simply extends the `KvError` with two new conditions:
/// `BadRequest` and `UnSupportedContract`.
pub enum ServerError {
    #[fail(display = "Engine exception: {}", eng_error)]
    /// Throws when the underlying engine meet an exception.
    EngineError {
        #[cause]
        /// the inner exception thrown by `KvsEngine`.
        eng_error: crate::KvError,
    },
    #[fail(display = "Bad request.")]
    /// Throws when the request has right binary format, but bad semantic of a request.
    BadRequest,
    #[fail(display = "Unsupported contract.")]
    /// Throws when the request has malformed binary format.
    UnsupportedContract {
        #[cause]
        /// the error occurs on contract.
        contract_error: crate::contract::Error,
    },
}

/// The `Result` type of `Server` context.
pub type Result<T> = std::result::Result<T, ServerError>;

impl From<crate::KvError> for ServerError {
    fn from(err: KvError) -> Self {
        EngineError { eng_error: err }
    }
}

impl From<crate::contract::Error> for ServerError {
    fn from(contract_error: crate::contract::Error) -> Self {
        UnsupportedContract { contract_error }
    }
}

impl From<std::io::Error> for ServerError {
    fn from(e: std::io::Error) -> Self {
        Self::from(KvError::from(e))
    }
}
