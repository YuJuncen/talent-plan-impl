use std::net::SocketAddr;
use std::path::Path;

use failure::_core::str::FromStr;
use failure::Fail;
use log::{error, info};
use structopt::StructOpt;
use tokio::net::TcpListener;
use tokio::prelude::*;

use kvs::{KvError, KvsEngine, KvStore};
use kvs::contract::KvContractMessage;
use kvs::contract::Request;
use kvs::engines::sled::SledEngine;

use crate::ServerError::{BadRequest, EngineError};

#[derive(Debug, StructOpt, Clone)]
#[structopt(name = "kvs",
about = env!("CARGO_PKG_DESCRIPTION"),
author = env!("CARGO_PKG_AUTHORS"),
version = env!("CARGO_PKG_VERSION"))]
struct ServerOpt {
    #[structopt(
    default_value = "127.0.0.1:4000",
    parse(try_from_str = str::parse),
    long = "--addr"
    )]
    addr: SocketAddr,
    #[structopt(
    default_value = "kvs",
    parse(try_from_str = str::parse),
    long = "--engine"
    )]
    engine: Engine
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
enum Engine {
    Kvs, Sled
}
#[derive(Debug, Eq, PartialEq, Clone, Copy, Fail)]
#[fail(display = "No such engine")]
struct NoSuchEngine;

impl FromStr for Engine {
    type Err = NoSuchEngine;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "kvs" => Ok(Self::Kvs),
            "sled" => Ok(Self::Sled),
            _ => Err(NoSuchEngine)
        }
    }
}

fn make_server(stream: TcpListener, engine: Engine, p: impl AsRef<Path>) -> Box<dyn Future<Item=(), Error=()> + Send> {
    match engine {
        Engine::Kvs => Box::new(make_task(stream, KvStore::open(p).unwrap())),
        Engine::Sled => Box::new(make_task(stream, SledEngine::open(p).unwrap()))
    }
}

#[derive(Debug, Fail)]
enum ServerError {
    #[fail(display = "Engine exception: {}", eng_error)]
    EngineError{
        #[cause]
        eng_error: kvs::KvError,
    },
    #[fail(display = "Bad request.")]
    BadRequest
}

impl From<kvs::KvError> for ServerError {
    fn from(err: KvError) -> Self {
        EngineError { eng_error: err }
    }
}

extern "C"
fn death_whisper() {
    error!("kvs - {} - our server will shutdown.", env!("CARGO_PKG_VERSION"));
}

fn make_task<E: KvsEngine>(stream: TcpListener, engine: E) -> impl Future<Item=(), Error=()> {
    stream.incoming()
        .and_then(|stream| {
            tokio::io::read_to_end(stream, vec![])
        })
        .and_then(move |stream| {
            let (sink, read) = stream;
            let get_result = || {
                let message = KvContractMessage::parse(read.as_slice()).map_err(|_| BadRequest)?;
                info!("Received message: {:?}", message);
                match message.to_request() {
                    Some(Request::Get { key }) => {
                        let result = engine.get(key.to_owned())?;
                        let response = match result {
                            Some(content) => KvContractMessage::response_content(content),
                            None => KvContractMessage::response_no_content()
                        };
                        Ok(response.into_binary())
                    },
                    Some(Request::Set { key, value }) => {
                        engine.set(key.to_owned(), value.to_owned())?;
                        let response = KvContractMessage::response_no_content();
                        Ok(response.into_binary())
                    },
                    Some(Request::Remove { key }) => {
                        engine.remove(key.to_owned())?;
                        let response = KvContractMessage::response_no_content();
                        Ok(response.into_binary())
                    },
                    None => {
                        Err(ServerError::BadRequest)
                    }
                }
            };
            match get_result().map_err(|err| KvContractMessage::response_err(format!("{}", err)).into_binary()) {
                Ok(buffer) => tokio::io::write_all(sink, buffer),
                Err(server_err) => tokio::io::write_all(sink, server_err)
            }
        })
        .for_each(|(stream, _written)| {
            future::result(stream.shutdown(std::net::Shutdown::Write))
        })
        .map_err(|err| {
            error!("server internal io error: {:?}", err);
            ()
        })
}

fn main() -> std::io::Result<()> {
    let opt : ServerOpt = ServerOpt::from_args();
    stderrlog::new()
        .verbosity(5)
        .module(module_path!()).init().unwrap();
    let stream = TcpListener::bind(&opt.addr)?;
    let task = make_server(stream, opt.engine, std::env::current_dir().unwrap());
    info!("kvs - {} - server running on {}", env!("CARGO_PKG_VERSION"), opt.addr.to_string());
    info!("kvs - {} - our data directory is {}.", env!("CARGO_PKG_VERSION"), std::env::current_dir().unwrap().to_str().unwrap());
    unsafe {
        libc::atexit(death_whisper);
    }
    tokio::run(task);
    Ok(())
}