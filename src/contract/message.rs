use std::collections::HashMap;
use std::io::Read;

use log::error;
use serde::{Deserialize, Serialize};

use super::{Error::MalformedBinary, Result};

/// the struct of the contract based on TCP to connect with the KvServer.
/// It is simply json.
/// I use json for this just for keep it simple(!),
/// hence I can reuse the escape implement in json.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct KvContractMessage {
    /// the operate type, see the constant below.
    pub operate_type: u8,
    /// the parameter of the message.
    pub param: HashMap<String, String>,
}

/// the request view of a message.
#[derive(Eq, PartialEq, Debug)]
pub enum Request<'a> {
    /// get request view.
    Get {
        /// the key to get.
        key: &'a str,
    },
    /// set request view.
    Set {
        /// the key to set.
        key: &'a str,
        /// the value to set.
        value: &'a str,
    },
    /// rm request view.
    Remove {
        /// the key to remove.
        key: &'a str,
    },
}

/// the response view of a message.
#[derive(Eq, PartialEq, Debug)]
pub enum Response<'a> {
    /// response with no content.
    NoContent,
    /// response with some message.
    Content {
        /// content of the message.
        content: &'a str,
    },
    /// response with error.
    Error {
        /// reason of this error.
        reason: &'a str,
    },
}

impl KvContractMessage {
    pub(crate) const GET: u8 = 0;
    pub(crate) const PUT: u8 = 1;
    pub(crate) const REMOVE: u8 = 2;

    pub(crate) const RESPONSE_WITH_CONTENT: u8 = 253;
    pub(crate) const RESPONSE_NO_CONTENT: u8 = 254;
    pub(crate) const RESPONSE_ERR: u8 = 255;
}

impl KvContractMessage {
    /// create an message that represents an get request.
    pub fn get(key: String) -> Self {
        KvContractMessage {
            operate_type: Self::GET,
            param: vec![("key".to_owned(), key)].into_iter().collect(),
        }
    }

    /// create an message that represents an set request.
    pub fn put(key: String, value: String) -> Self {
        KvContractMessage {
            operate_type: Self::PUT,
            param: vec![("key".to_owned(), key), ("value".to_owned(), value)]
                .into_iter()
                .collect(),
        }
    }

    /// create an message that represents an remove request.
    pub fn remove(key: String) -> Self {
        KvContractMessage {
            operate_type: Self::REMOVE,
            param: vec![("key".to_owned(), key)].into_iter().collect(),
        }
    }

    /// create an ok response message, with no content.
    pub fn response_no_content() -> Self {
        KvContractMessage {
            operate_type: Self::RESPONSE_NO_CONTENT,
            param: HashMap::new(),
        }
    }

    /// create an error response message.
    pub fn response_err(reason: String) -> Self {
        KvContractMessage {
            operate_type: Self::RESPONSE_ERR,
            param: vec![("reason".to_owned(), reason)].into_iter().collect(),
        }
    }

    /// create a success response with some content.
    pub fn response_content(content: String) -> Self {
        KvContractMessage {
            operate_type: Self::RESPONSE_WITH_CONTENT,
            param: vec![("content".to_owned(), content)].into_iter().collect(),
        }
    }

    /// parse an contact message from a stream.
    ///
    /// # Error
    ///
    /// if the binary format isn't right, throw `MalformedBinary`.
    pub fn parse(mut raw: (impl Read)) -> Result<Self> {
        serde_json::from_reader(&mut raw).map_err(|err| {
            error!(target: "app::error", "failed to parse request, exception: {}.", err);
            MalformedBinary
        })
    }

    /// serialize the message into binary from.
    /// Even now it's just simply JSON text(!).
    pub fn into_binary(self) -> Vec<u8> {
        let serialized = serde_json::to_string(&self).expect("unable to serialize self into json.");
        serialized.into_bytes()
    }

    /// match the raw message as `Request`.
    ///
    /// ```rust
    /// # use kvs::contract::{KvContractMessage, Request};
    /// let message = KvContractMessage::get("hello".to_owned());
    /// assert_eq!(message.to_request(), Some(Request::Get { key: "hello" }));
    /// ```
    ///
    /// # Error
    ///
    /// When failed to parse it as an request message, return `None`.
    pub fn to_request(&self) -> Option<Request> {
        match self.operate_type {
            Self::PUT => self.param.get("key").and_then(|key| {
                self.param.get("value").map(|value| Request::Set {
                    key: key.as_str(),
                    value: value.as_str(),
                })
            }),
            Self::GET => self
                .param
                .get("key")
                .map(|key| Request::Get { key: key.as_str() }),
            Self::REMOVE => self
                .param
                .get("key")
                .map(|key| Request::Remove { key: key.as_str() }),
            _ => None,
        }
    }

    /// match the raw message as `Response`.
    ///
    /// # Error
    ///
    /// When failed to parse it as an response message, return `None`.
    pub fn to_response(&self) -> Option<Response> {
        match self.operate_type {
            Self::RESPONSE_NO_CONTENT => Some(Response::NoContent),
            Self::RESPONSE_WITH_CONTENT => {
                self.param.get("content").map(|content| Response::Content {
                    content: content.as_str(),
                })
            }
            Self::RESPONSE_ERR => self.param.get("reason").map(|reason| Response::Error {
                reason: reason.as_str(),
            }),
            _ => None,
        }
    }
}
