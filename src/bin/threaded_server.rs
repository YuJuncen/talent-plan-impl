use std::io::Write;
use std::net::{SocketAddr, TcpListener};
use std::net::TcpStream;

use failure::_core::time::Duration;
use log::{error, info};
use structopt::StructOpt;

use kvs::{KvsEngine, KvStore};
use kvs::contract::{KvContractMessage, Request};
use kvs::engines::sled::SledEngine;
use kvs::server_common::*;
use kvs::server_common::ServerError::BadRequest;
use kvs::thread_pool::*;

struct Server<E, P> {
    engine: E,
    pool: P,
}

impl<E, P> Server<E, P> where
    E: KvsEngine,
    P: ThreadPool {
    fn new(engine: E, pool: P) -> Self {
        Server { engine, pool }
    }

    fn handle_request(mut stream: TcpStream, engine: E) -> Result<()> {
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        let message = KvContractMessage::parse(&mut stream)?;
        let request = match message.to_request() {
            Some(request) => request,
            None => return Err(BadRequest)
        };
        info!(target: "app::request", "handling request {:?}.", &request);
        let result = Self::query_db(request, engine)?;
        let bin = result.into_binary();
        stream.write_all(bin.as_slice())?;
        Ok(())
    }

    fn query_db(request: Request, engine: E) -> Result<KvContractMessage> {
        match request {
            Request::Get { key } => {
                let queried = engine.get(key.to_owned())?;
                match queried {
                    Some(value) => Ok(KvContractMessage::response_content(value)),
                    None => Ok(KvContractMessage::response_no_content())
                }
            }
            Request::Set { key, value } => {
                match engine.set(key.to_owned(), value.to_owned()) {
                    Ok(()) => Ok(KvContractMessage::response_no_content()),
                    Err(err) => Ok(KvContractMessage::response_err(format!("{}", err)))
                }
            }
            Request::Remove { key } => {
                match engine.remove(key.to_owned()) {
                    Ok(()) => Ok(KvContractMessage::response_no_content()),
                    Err(err) => Ok(KvContractMessage::response_err(format!("{}", err)))
                }
            }
        }
    }

    fn do_listen_on(self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(&addr)?;
        info!("succeed to bind to {}, listening incoming requests.", addr);
        for stream in listener.incoming() {
            self.pool.spawn({
                let engine = self.engine.clone();
                move || {
                    let stream = stream.unwrap();
                    let peer_addr = stream.peer_addr().map(|addr| format!("{}", addr)).unwrap_or("UNKNOWN".to_owned());
                    match Self::handle_request(stream, engine) {
                        Ok(_) => (),
                        Err(err) => error!(target: "app::error", "An error: {} occurs during processing... with peer: {}", err, peer_addr)
                    };
                }
            })
        }
        Ok(())
    }

    fn listen_on(self, addr: SocketAddr) {
        info!("Our server will on: {}", addr);
        match self.do_listen_on(addr.clone()) {
            Err(err) => error!(target: "app::error", "err:{}; Our server on {} will stop...", err, addr),
            Ok(_) => info!("goodbye!")
        }
    }
}


fn main() -> Result<()> {
    let opt: ServerOpt = ServerOpt::from_args();
    let addr = opt.addr;
    let path = std::env::current_dir().unwrap();
    //log4rs::init_config(kvs::config::log4rs::config()).expect("unable to init logger.");
    error!(target: "app::error", "=== app::error === [kvs version {}, listen on {}]", env!("CARGO_PKG_VERSION"), addr);
    info!(target: "app::request", "=== app::request === [kvs version {}, listen on {}]", env!("CARGO_PKG_VERSION"), addr);
    info!("config: {:?}", opt);
    match opt.engine {
        Engine::Kvs => {
            let server = Server::new(
                KvStore::open(&path)?,
                SharedQueueThreadPool::new(4)?);
            server.listen_on(addr.clone());
        }
        Engine::Sled => {
            let server = Server::new(
                SledEngine::open(&path)?,
                SharedQueueThreadPool::new(4)?);
            server.listen_on(addr.clone());
        }
    };
    info!("goodbye.");
    Ok(())
}