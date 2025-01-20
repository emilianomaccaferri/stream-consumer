use async_stream::stream;
use autoclaim_reply::AutoclaimReply;
use config::ConsumerConfiguration;
use error::ConsumerError;
use futures::Stream;
use redis::{
    streams::{StreamId, StreamKey, StreamReadOptions, StreamReadReply},
    AsyncCommands, RedisConnectionInfo,
};

mod autoclaim_reply;
pub(crate) mod builder;
mod config;
pub(crate) mod error;

pub struct Consumer {
    config: ConsumerConfiguration,
    redis: redis::Client,
    active_queue_key: String,
    unclaimed_queue_key: String,
    redis_connection: Option<redis::aio::MultiplexedConnection>,
}
pub struct StreamMessage {
    pub stream_name: String,
    pub len: usize,
    pub items: Vec<StreamId>,
}
pub enum ConsumerMessage {
    EmptyStream,
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

        Ok(stream! {
            loop {
                let conn = self.redis_connection.as_mut().unwrap();
                let reply: StreamReadReply = conn
                    .xread_options(
                        &[&self.config.stream_name],
                        &[&self.active_queue_key],
                        &x_readgroup_options
                    ).await?;

                if reply.keys.is_empty() {
                    // autoclaim here, this happens when there are no
                    // events after "block_time" ms passed
                    if self.config.skip_autoclaim {
                        self.autoclaim().await?;
                    }
                    yield Ok(ConsumerMessage::EmptyStream)
                }
                for StreamKey {
                    key,
                    ids
                } in reply.keys {

                    // for each stream the consumer is montoring (one for now)
                    // we emit messages

                    if ids.is_empty() && self.active_queue_key != ">" {
                        // backlog queue is drained
                        self.active_queue_key = ">".to_string();
                    }

                    yield Ok(ConsumerMessage::Message(StreamMessage {
                            stream_name: key,
                            len: ids.len(),
                            items: ids
                        })
                    )

                }

            }
        })
    }
    /// This method allows the cosnumer to claim messages that are left pending by other
    /// consumers.
    async fn autoclaim(&mut self) -> Result<(), ConsumerError> {
        let conn = self.redis_connection.as_mut().unwrap();
        let response: AutoclaimReply = redis::cmd("xautoclaim")
            .arg(&self.config.stream_name)
            .arg(&self.config.notification_group)
            .arg(&self.config.name)
            .arg(self.config.autoclaim_time) // unclaimed key age
            .arg(&self.unclaimed_queue_key)
            .arg("count")
            .arg(self.config.item_count)
            .query_async(conn)
            .await?;

        if response.claimed_items != 0 {
            // this way, once the "main" stream gets polled again,
            // autoclaimed messages will automatically be delivered to the stream consumer
            self.active_queue_key = "0-0".to_string();
        }
        self.unclaimed_queue_key = response.next; // if there are more keys we move to the next claimable "set"

        Ok(())
    }
}
