use std::net::SocketAddr;
use std::str::FromStr;

use failure::Fail;
use structopt::StructOpt;

use crate::KvError;
use crate::server_common::ServerError::{EngineError, UnsupportedContract};
use crate::thread_pool::SharedQueueThreadPool;

#[derive(Debug, StructOpt, Clone)]
#[structopt(name = "kvs",
about = env ! ("CARGO_PKG_DESCRIPTION"),
author = env ! ("CARGO_PKG_AUTHORS"),
version = env ! ("CARGO_PKG_VERSION"))]
pub struct ServerOpt {
    #[structopt(
    default_value = "127.0.0.1:4000",
    parse(try_from_str = str::parse),
    long = "--addr"
    )]
    pub addr: SocketAddr,
    #[structopt(
    default_value = "kvs",
    parse(try_from_str = str::parse),
    long = "--engine"
    )]
    pub engine: Engine,
    #[structopt(
    default_value = "shared_queue",
    parse(try_from_str = str::parse),
    long = "--pool"
    )]
    pub pool: Pool,
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum Engine {
    Kvs,
    Sled,
}

impl Default for Engine {
    fn default() -> Self {
        Engine::Kvs
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy, Fail)]
#[fail(display = "No such engine")]
pub struct NoSuchEngine;

impl FromStr for Engine {
    type Err = NoSuchEngine;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "kvs" => Ok(Self::Kvs),
            "sled" => Ok(Self::Sled),
            _ => Err(NoSuchEngine)
        }
    }
}

impl AsRef<str> for Engine {
    fn as_ref(&self) -> &str {
        match self {
            Engine::Kvs => "kvs",
            Engine::Sled => "sled"
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum Pool {
    Naive,
    Rayon,
    SharedQueue,
}

impl Default for Pool {
    fn default() -> Self {
        Pool::SharedQueue
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Fail)]
#[fail(display = "No such pool: {}", 0)]
pub struct NoSuchPool(String);

impl FromStr for Pool {
    type Err = NoSuchPool;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "naive" => Ok(Pool::Naive),
            "shared_queue" => Ok(Pool::SharedQueue),
            "rayon" => Ok(Pool::Rayon),
            _ => Err(NoSuchPool(s.to_owned()))
        }
    }
}

impl AsRef<str> for Pool {
    fn as_ref(&self) -> &str {
        match self {
            &Pool::Naive => "naive",
            &Pool::Rayon => "rayon",
            &Pool::SharedQueue => "shared_queue"
        }
    }
}

#[derive(Debug, Fail)]
pub enum ServerError {
    #[fail(display = "Engine exception: {}", eng_error)]
    EngineError {
        #[cause]
        eng_error: crate::KvError,
    },
    #[fail(display = "Bad request.")]
    BadRequest,
    #[fail(display = "Unsupported contract.")]
    UnsupportedContract {
        #[cause]
        contract_error: crate::contract::Error
    },
}

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
