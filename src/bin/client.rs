use std::io::Write;
use std::net::SocketAddr;
use std::process::exit;

use structopt::StructOpt;

use kvs::contract::KvContractMessage;
use kvs::contract::Response;

#[derive(Debug, StructOpt)]
#[structopt(name = "kvs",
about = env!("CARGO_PKG_DESCRIPTION"),
author = env!("CARGO_PKG_AUTHORS"),
version = env!("CARGO_PKG_VERSION"))]
enum ClientOpt {
    Set {
        /// a key string to put.
        key: String,
        /// a value string to put with the key.
        value: String,
        /// the server
        #[structopt(
        parse(try_from_str = str::parse),
        name = "addr",
        long = "--addr",
        default_value = "127.0.0.1:4000"
        )]
        server: SocketAddr,
    },
    Get {
        /// a key string to get.
        key: String,
        #[structopt(
        parse(try_from_str = str::parse),
        name = "addr",
        long = "--addr",
        default_value = "127.0.0.1:4000"
        )]
        server: SocketAddr,
    },
    Rm {
        /// a key string to remove.
        key: String,
        #[structopt(
        parse(try_from_str = str::parse),
        name = "addr",
        long = "--addr",
        default_value = "127.0.0.1:4000"
        )]
        server: SocketAddr,
    },
}
#[derive(Debug, Eq, PartialEq)]
enum Operate {
    Get,
    Set,
    Rm,
}

impl ClientOpt {
    fn to_operate(&self) -> Operate {
        use Operate::*;
        match self {
            Self::Set { .. } => Set,
            Self::Get { .. } => Get,
            Self::Rm { .. } => Rm,
        }
    }
}

fn send_to(message: KvContractMessage, addr: SocketAddr) -> std::io::Result<KvContractMessage> {
    let bin = message.into_binary();
    let mut stream = std::net::TcpStream::connect(addr).unwrap();
    stream.write_all(bin.as_slice())?;
    stream.shutdown(std::net::Shutdown::Write)?;
    Ok(KvContractMessage::parse(stream).unwrap())
}

impl ClientOpt {
    fn send(self) -> std::io::Result<KvContractMessage> {
        match self {
            Self::Set { key, value, server } => send_to(KvContractMessage::put(key, value), server),
            Self::Get { key, server } => send_to(KvContractMessage::get(key), server),
            Self::Rm { key, server } => send_to(KvContractMessage::remove(key), server),
        }
    }
}

fn main() -> std::io::Result<()> {
    let opt = ClientOpt::from_args();
    let operate = opt.to_operate();
    match opt.send()?.to_response().unwrap() {
        Response::NoContent => {
            if operate == Operate::Get {
                println!("Key not found");
            }
            exit(0);
        }
        Response::Content { content } => {
            println!("{}", content);
            exit(0);
        }
        Response::Error { reason } => {
            eprintln!("{}", reason);
            exit(1);
        }
    };
}
