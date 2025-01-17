use std::pin::Pin;

use async_stream::stream;
use config::ConsumerConfiguration;
use error::ConsumerError;
use futures::{pin_mut, Stream, StreamExt};
use redis::{
    streams::{StreamKey, StreamReadOptions, StreamReadReply},
    AsyncCommands,
};

pub(crate) mod builder;
mod config;
pub(crate) mod error;

pub struct Consumer {
    config: ConsumerConfiguration,
    redis: redis::Client,
    active_queue_key: String,
    unclaimed_queue_key: String,
}
pub struct StreamMessage {
    pub stream_name: String,
    pub messages: usize,
}
pub enum ConsumerMessage {
    EmptyStream,
    Message(StreamMessage),
}
impl Consumer {
    pub async fn stream<'a>(
        &'a mut self,
    ) -> Result<impl Stream<Item = Result<ConsumerMessage, ConsumerError>> + 'a, ConsumerError>
    {
        let x_readgroup_options = StreamReadOptions::default()
            .group(&self.config.notification_group, &self.config.name)
            .count(self.config.item_count)
            .block(self.config.block_time);
        let mut redis_connection = self.redis.get_multiplexed_async_connection().await?;

        Ok(stream! {
            loop {
                let active_stream_key = self.active_queue_key.clone();
                let stream_name = self.config.stream_name.clone();
                let reply: StreamReadReply = redis_connection.clone()
                    .xread_options(
                        &[&stream_name],
                        &[&active_stream_key],
                        &x_readgroup_options
                    ).await?;

                if reply.keys.is_empty() {
                    // autoclaim here
                    yield Ok(ConsumerMessage::EmptyStream)
                }
                for StreamKey {
                    key,
                    ids
                } in reply.keys {

                    // for each stream the consumer is montoring (one for now)
                    // we emit messages

                    if ids.len() == 0 && active_stream_key != ">".to_string() {
                        // backlog queue is drained
                        self.active_queue_key = ">".to_string();
                    }

                    yield Ok(ConsumerMessage::Message(StreamMessage { stream_name: key, messages: ids.len() }))

                }

            }
        })
    }
}
