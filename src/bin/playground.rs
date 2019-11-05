use std::net::SocketAddr;

use assert_cmd::prelude::CommandCargoExt;
use log::{error, info};
use tempfile::TempDir;

use kvs::{KvsEngine, Result};
use kvs::benchmark_common::Promise;
use kvs::server_common::{Engine, Pool};

fn main() -> Result<()> {
    let temp = tempfile::tempdir().unwrap();
    std::env::set_current_dir(temp.path()).unwrap();
    log4rs::init_config(kvs::config::log4rs::config()).unwrap();
    let fut = Promise::new();
    let send = fut.clone();
    std::thread::spawn(move || {
        send.fulfill(1);
    });
    println!("{:?}", fut.get());
    Ok(())
}
