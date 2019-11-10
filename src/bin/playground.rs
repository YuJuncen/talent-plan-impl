use std::io::{Seek, SeekFrom};

use regex::Regex;
use serde::{Deserialize, Serialize};

fn main() -> std::io::Result<()> {
    std::fs::remove_file("not-exists.");
    Ok(())
}
