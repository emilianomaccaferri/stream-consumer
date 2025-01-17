use redis::RedisError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConsumerError {
    #[error("cannot connect to Redis: {0}")]
    ConnectionError(String),
}

impl From<RedisError> for ConsumerError {
    fn from(value: RedisError) -> Self {
        Self::ConnectionError(value.to_string())
    }
}