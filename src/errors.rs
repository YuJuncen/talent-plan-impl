use failure::Fail;

pub type Result<T> = std::result::Result<T, KvError>;

#[derive(Debug, Fail)]
pub enum KvError {
    #[fail(display = "Failed to open file {} because error [{}].", file_name, io_error)]
    FailToOpenFile {
        file_name: String,
        io_error: std::io::Error
    },
    #[fail(display = "Failed to append to file because error [{}].", io_error)]
    FailToAppendFile {
        io_error: std::io::Error
    },
    #[fail(display = "Failed to read file because error [{}]", io_error)]
    FailToReadFile {
        io_error: std::io::Error
    },
    #[fail(display = "Failed to parse file because error [{}]", serde_error)]
    FailToParseFile {
        serde_error: serde_json::Error
    }
}
