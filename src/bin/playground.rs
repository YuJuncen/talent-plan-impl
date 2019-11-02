use std::net::SocketAddr;

use assert_cmd::prelude::CommandCargoExt;
use log::{error, info};
use tempfile::TempDir;

use kvs::{KvsEngine, Result};
use kvs::benchmark_common::Promise;
use kvs::server_common::{Engine, Pool};

fn main() -> Result<()> {
    log4rs::init_config(kvs::config::log4rs::config()).unwrap();
    let engine = kvs::benchmark_common::RemoteEngine::spawn_new("127.0.0.1:4000".parse().unwrap(),
                                                                Engine::Kvs,
                                                                Pool::SharedQueue);

    std::thread::sleep(std::time::Duration::from_secs(3));
    engine.set("hello".to_owned(), "world".to_owned()).unwrap();
    info!("{:?}", engine.get("hello".to_owned()));
    engine.remove("hello".to_owned()).unwrap();
    info!("{:?}", engine.get("hello".to_owned()));
    Ok(())
}
