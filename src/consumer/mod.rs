use std::collections::HashMap;

use async_stream::stream;
use config::ConsumerConfiguration;
use error::ConsumerError;
use futures::Stream;
use redis::{
    streams::{StreamId, StreamKey, StreamReadOptions, StreamReadReply},
    AsyncCommands,
};
use utils::autoclaim;

mod autoclaim_reply;
pub(crate) mod builder;
mod config;
pub(crate) mod error;
mod utils;

pub struct Consumer {
    config: ConsumerConfiguration,
    redis: redis::Client,
    active_queue_keys: HashMap<String, String>,
    unclaimed_queue_keys: HashMap<String, String>,
    redis_connection: Option<redis::aio::MultiplexedConnection>,
}
pub struct StreamMessage {
    pub stream_name: String,
    pub len: usize,
    pub items: Vec<StreamId>,
}
pub enum ConsumerMessage {
    EmptyStream(String),
    Message(StreamMessage),
}
impl Consumer {
    pub async fn connect(&mut self) -> Result<(), ConsumerError> {
        self.redis_connection = Some(self.redis.get_multiplexed_async_connection().await?);
        Ok(())
    }
    pub async fn stream<'a>(
        &'a mut self,
    ) -> Result<impl Stream<Item = Result<ConsumerMessage, ConsumerError>> + 'a, ConsumerError>
    {
        if self.redis_connection.is_none() {
            return Err(ConsumerError::NotConnected);
        }

        let x_readgroup_options = StreamReadOptions::default()
            .group(&self.config.notification_group, &self.config.name)
            .count(self.config.item_count)
            .block(self.config.block_time);

        let mut conn = self.redis_connection.clone().unwrap();
        Ok(stream! {
            loop {
                let active_queue_keys = self.active_queue_keys
                    .keys()
                    .cloned()
                    .collect::<Vec<String>>();
                let active_queue_ids = self.active_queue_keys
                    .values()
                    .collect::<Vec<&String>>();
                let reply: StreamReadReply = conn
                    .xread_options(
                        &active_queue_keys, // stream names
                        &active_queue_ids, // stream indexes
                        &x_readgroup_options
                    ).await?;

                if reply.keys.is_empty() {
                    // autoclaim here, this happens when there are no
                    // events after "block_time" ms passed
                    if !self.config.skip_autoclaim {
                        self.perform_autoclaim().await?;
                    }
                    for key in active_queue_keys {
                        yield Ok(ConsumerMessage::EmptyStream(key));
                    }
                }


                for StreamKey {
                    key,
                    ids
                } in reply.keys {
                    if ids.is_empty() {
                        // autoclaim here, this happens when there are no
                        // events after "block_time" ms passed
                        self.perform_autoclaim().await?;
                        yield Ok(ConsumerMessage::EmptyStream(key.clone()))
                    }

                    // for each stream the consumer is montoring
                    // we emit messages
                    if let Some(current_key_value) = self.active_queue_keys.get_mut(&key) {
                        if ids.is_empty() {
                            if current_key_value != ">" {
                                // backlog queue is drained
                                *current_key_value = ">".to_string();
                            }
                            yield Ok(ConsumerMessage::EmptyStream(key.clone()));
                        }


                        yield Ok(ConsumerMessage::Message(StreamMessage {
                                stream_name: key,
                                len: ids.len(),
                                items: ids
                            })
                        )
                    } else {
                        yield Err(ConsumerError::InvalidStreamName(key.clone()));
                    }
                }

            }
        })
    }
    pub fn name(&self) -> &str {
        &self.config.name
    }
    async fn perform_autoclaim(&mut self) -> Result<(), ConsumerError> {
        if !self.config.skip_autoclaim {
            for stream_name in &self.config.streams {
                let response = autoclaim(
                    self.redis_connection.as_mut().unwrap(),
                    stream_name,
                    &self.config.notification_group,
                    &self.config.name,
                    self.config.autoclaim_time,
                    self.unclaimed_queue_keys.get(stream_name).unwrap(),
                    self.config.item_count,
                )
                .await?;
                if response.claimed_items != 0 {
                    if let Some(entry) = self.active_queue_keys.get_mut(stream_name) {
                        *entry = "0-0".to_string();
                        let unclaimed_entry_key =
                            self.unclaimed_queue_keys.get_mut(stream_name).unwrap();
                        *unclaimed_entry_key = response.next; // if there are more keys we move to the next claimable "set"
                    } else {
                        return Err(ConsumerError::InvalidStreamName(stream_name.to_string()));
                    };
                }
            }
        }
        Ok(())
    }
}
