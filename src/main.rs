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
        if let Err(conn_err) = consumer.connect().await {
            panic!("cannot connect: {}", conn_err);
        }
        let stream = consumer.stream().await.unwrap();
        pin_mut!(stream);
        while let Some(n) = stream.next().await {
            match n {
                Ok(stream_result) => match stream_result {
                    ConsumerMessage::EmptyStream => println!("empty"),
                    ConsumerMessage::Message(StreamMessage {
                        stream_name,
                        len,
                        items,
                    }) => for item in items {},
                },
                Err(e) => {
                    dbg!(&e);
                }
            }
        }
        exit(0);
    } else {
        panic!("NOUUUU");
    }
}
