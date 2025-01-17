use std::process::exit;

use consumer::{builder::ConsumerBuilder, ConsumerMessage, StreamMessage};
use futures::{pin_mut, StreamExt};
mod consumer;

#[tokio::main]
async fn main() {
    if let Ok(mut consumer) = ConsumerBuilder::new()
        .stream_name("streams:images_data")
        .notification_group("builders")
        .block_time(1000)
        .item_count(10)
        .skip_backlog_queue(false)
        .build()
    {
        let stream = consumer.stream().await.unwrap();
        pin_mut!(stream);
        while let Some(n) = stream.next().await {
            match n {
                Ok(stream_result) => match stream_result {
                    ConsumerMessage::EmptyStream => println!("empty"),
                    ConsumerMessage::Message(StreamMessage {
                        stream_name,
                        messages,
                    }) => println!("{}: {} messages", stream_name, messages),
                },
                Err(e) => println!("{}", e),
            }
        }
        exit(0);
    } else {
        panic!("NOUUUU");
    }
}
