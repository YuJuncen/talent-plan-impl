use std::sync::PoisonError;

use failure::Fail;
use rayon::ThreadPoolBuildError;

/// The result type used in the `KvEngine` context.
pub type Result<T> = std::result::Result<T, KvError>;

/// The Error type of `KvEngine` context.
/// It has some variant that warps other types, like `std::io::Error`, `serde_json::Error`.
/// This error type implements the `From` trait of those error types.
#[derive(Debug, Fail)]
pub enum KvError {
    #[fail(
        display = "Failed to open file {} because error [{}].",
        file_name, io_error
    )]
    /// failed to open an db file.
    FailToOpenFile {
        /// the filename which failed to open.
        file_name: String,
        #[cause]
        /// the original io exception.
        io_error: std::io::Error,
    },
    #[fail(
        display = "Failed because some unexpected IO exception [{}].",
        io_error
    )]
    /// Failed because generic io exception, like broken pipe, removed file.
    /// It warps `std::io::Error`.
    OtherIOException {
        #[cause]
        /// the inner error.
        io_error: std::io::Error,
    },
    #[fail(display = "Failed to build an rayon thread pool: {}", error)]
    /// The error for `RayonThreadPool`, it wraps `rayon_core::ThreadPoolBuildError`.
    RayonThreadPoolFailedToBuild {
        #[cause]
        /// the inner error.
        error: ThreadPoolBuildError,
    },
    #[fail(display = "Failed to parse file because error [{}]", serde_error)]
    /// The `KvStore` meet malformed datafile.
    /// It wraps `serde_json::Error`
    FailToParseFile {
        #[cause]
        /// the inner error.
        serde_error: serde_json::Error,
    },
    /// Throws when trying to delete a non-exist key.
    #[fail(display = "Key not found")]
    KeyNotFound,
    /// The most generic exception type for some 'WTF'(What a Terrible Failure) condition.
    #[fail(display = "other exception: {}", reason)]
    Other {
        /// the reason, you can write anything you want here.
        /// ...even it's an anti-pattern to use string-structured data structure.
        reason: String,
    },
    /// Throws when trying to open an engine on directory that is 'dominated' by other engine.
    #[fail(display = "illegal working directory: another instance is working here.")]
    IllegalWorkingDirectory,
    /// Throws when meeting some bad things during play with some concurrent data-structures or locks.
    #[fail(display = "when operate with lock, something bad happens.")]
    ConcurrentError,
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

impl<T> From<PoisonError<T>> for KvError {
    fn from(_: PoisonError<T>) -> Self {
        KvError::ConcurrentError
    }
}

impl From<ThreadPoolBuildError> for KvError {
    fn from(error: ThreadPoolBuildError) -> Self {
        KvError::RayonThreadPoolFailedToBuild { error }
    }
}
