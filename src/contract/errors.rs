use failure::Fail;

/// the error type of contract.
#[derive(Debug, Fail)]
pub enum Error {
    /// the contract data from TCP is malformed.
    #[fail(display = "Failed to parse the format of binary data.")]
    MalformedBinary
}
/// the `Result` type of our contract.
pub type Result<T> = std::result::Result<T, Error>;