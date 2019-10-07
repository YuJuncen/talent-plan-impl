use failure::Fail;

pub type Result<T> = std::result::Result<T, KvError>;

#[derive(Debug, Fail)]
pub enum KvError {
    #[fail(
        display = "Failed to open file {} because error [{}].",
        file_name, io_error
    )]
    FailToOpenFile {
        file_name: String,
        #[cause]
        io_error: std::io::Error,
    },
    #[fail(
        display = "Failed because some unexpected IO exception [{}].",
        io_error
    )]
    OtherIOException {
        #[cause]
        io_error: std::io::Error,
    },
    #[fail(display = "Failed to parse file because error [{}]", serde_error)]
    FailToParseFile {
        #[cause]
        serde_error: serde_json::Error,
    },
    #[fail(display = "Key not found")]
    KeyNotFound,
    #[fail(display = "other exception: {}", reason)]
    Other {
        reason: String
    },
    #[fail(display = "illegal working directory: another instance is working here.")]
    IllegalWorkingDirectory
}

impl From<serde_json::Error> for KvError {
    fn from(err: serde_json::Error) -> Self {
        KvError::FailToParseFile { serde_error: err }
    }
}

impl From<std::io::Error> for KvError {
    fn from(io_error: std::io::Error) -> Self {
        KvError::OtherIOException { io_error }
    }
}
