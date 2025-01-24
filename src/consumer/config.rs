pub(super) struct ConsumerConfiguration {
    /// Consumer name inside Redis
    pub name: String,
    /// Redis connection url
    pub redis_url: String,
    /// Redis streams to monitor
    pub streams: Vec<String>,
    /// Redis notification group name
    pub notification_group: String,
    /// How many items should be retrieved each time (default: 1)
    pub item_count: usize,
    /// How many milliseconds should the consumer block for (default: 0) (https://redis.io/docs/latest/commands/xread/)
    pub block_time: usize,
    /// Autoclaim time in milliseconds. Basically how old should claimable messages be
    /// to be considered from the consumer (https://redis.io/docs/latest/commands/xautoclaim/)
    /// (default: 60_000)
    pub autoclaim_time: usize,
    /// If true, when the consumer starts, no backlog messages will be processed
    /// i.e. if messages have arrived when the consumer was offline, those messages
    /// won't be processed (default: false)
    pub skip_backlog_queue: bool,
    /// If true, when the consumer timeouts after "block_time" milliseconds,
    /// no autoclaim will happen, meaning that the consumer won't
    /// claim messages that are left inside the stream and are not processed by other
    /// consumers (default: false) (https://redis.io/docs/latest/commands/xautoclaim/)
    pub skip_autoclaim: bool,
}

impl Default for ConsumerConfiguration {
    fn default() -> Self {
        ConsumerConfiguration {
            name: fake_rs::generate_name().to_lowercase(),
            redis_url: String::from("redis://127.0.0.1:6379"),
            streams: vec![String::from("redis_stream")],
            notification_group: String::from("notification_group"),
            item_count: 1,
            block_time: 0,
            autoclaim_time: 60_000,
            skip_backlog_queue: false,
            skip_autoclaim: false,
        }
    }
}
