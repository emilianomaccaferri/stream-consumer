use redis::RedisError;
use thiserror::Error;

use super::{config::ConsumerConfiguration, error::ConsumerError, Consumer};

pub(crate) struct ConsumerBuilder {
    config: ConsumerConfiguration,
}

impl ConsumerBuilder {
    pub fn new() -> ConsumerBuilder {
        ConsumerBuilder {
            config: ConsumerConfiguration::default(),
        }
    }
    pub fn build(self) -> Result<Consumer, ConsumerError> {
        let redis_url = self.config.redis_url.clone();
        let skip_backlog_queue = self.config.skip_backlog_queue;
        Ok(Consumer {
            config: self.config,
            redis: redis::Client::open(redis_url)?,
            active_queue_key: String::from(if skip_backlog_queue { ">" } else { "0-0" }),
            unclaimed_queue_key: String::from("0-0"),
            redis_connection: None,
        })
    }
    pub fn skip_backlog_queue(mut self, value: bool) -> ConsumerBuilder {
        self.config.skip_backlog_queue = value;
        self
    }
    pub fn name(mut self, name: &str) -> ConsumerBuilder {
        self.config.name = name.to_string();
        self
    }
    pub fn redis_url(mut self, redis_url: &str) -> ConsumerBuilder {
        self.config.redis_url = redis_url.to_string();
        self
    }
    pub fn stream_name(mut self, stream_name: &str) -> ConsumerBuilder {
        self.config.stream_name = stream_name.to_string();
        self
    }
    pub fn notification_group(mut self, notification_group: &str) -> ConsumerBuilder {
        self.config.notification_group = notification_group.to_string();
        self
    }
    pub fn item_count(mut self, item_count: usize) -> ConsumerBuilder {
        self.config.item_count = item_count;
        self
    }
    pub fn block_time(mut self, block_time: usize) -> ConsumerBuilder {
        self.config.block_time = block_time;
        self
    }
    pub fn autoclaim_time(mut self, autoclaim_time: usize) -> ConsumerBuilder {
        self.config.autoclaim_time = autoclaim_time;
        self
    }
}
