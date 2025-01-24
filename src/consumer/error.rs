use redis::RedisError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConsumerError {
    #[error("cannot connect to Redis: {0}")]
    ConnectionError(String),
    #[error("you must connect to Redis before streaming")]
    NotConnected,
    /// this should not happen, meaning that redis shouldn't return messages
    /// from non-registered streams
    #[error("retrieved message from invalid stream: {0}")]
    InvalidStreamName(String),
}

impl From<RedisError> for ConsumerError {
    fn from(value: RedisError) -> Self {
        Self::ConnectionError(value.to_string())
    }
}
