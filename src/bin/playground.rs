use std::io::{Seek, SeekFrom};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Debug, Deserialize, Clone)]
struct Simple {
    foo: i32,
    bar: String,
}

struct SharedReader {}

trait SeekExt {
    fn current_position(&mut self) -> std::io::Result<usize>;
}

impl<R: Seek> SeekExt for R {
    fn current_position(&mut self) -> std::io::Result<usize> {
        self.seek(SeekFrom::Current(0))
            .map(|n| n as usize)
    }
}

fn main() {

}
