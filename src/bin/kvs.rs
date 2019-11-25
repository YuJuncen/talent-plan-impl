use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "kvs",
  about = env!("CARGO_PKG_DESCRIPTION"),
  author = env!("CARGO_PKG_AUTHORS"),
  version = env!("CARGO_PKG_VERSION"))]
enum KVOpt {
    Set {
        /// a key string to put.
        key: String,
        /// a value string to put with the key.
        value: String,
    },
    Get {
        /// a key string to get.
        key: String,
    },
    Rm {
        /// a key string to remove.
        key: String,
    },
}

fn main() {
    let _opt = KVOpt::from_args();
    unimplemented!("All CLI functions are unimplemented now!")
}
