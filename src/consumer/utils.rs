use redis::aio::MultiplexedConnection;

use super::{autoclaim_reply::AutoclaimReply, error::ConsumerError};

/// This method allows the consumer to claim messages that are left pending by other
/// consumers.
pub async fn autoclaim(
    conn: &mut MultiplexedConnection,
    stream_name: &str,
    group: &str,
    consumer_name: &str,
    autoclaim_time: usize,
    unclaimed_queue_key: &str,
    item_count: usize,
) -> Result<AutoclaimReply, ConsumerError> {
    let response: AutoclaimReply = redis::cmd("xautoclaim")
        .arg(stream_name)
        .arg(group)
        .arg(consumer_name)
        .arg(autoclaim_time) // unclaimed key age
        .arg(unclaimed_queue_key)
        .arg("count")
        .arg(item_count)
        .query_async(conn)
        .await?;

    Ok(response)
}
