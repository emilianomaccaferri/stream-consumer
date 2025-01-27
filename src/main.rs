use std::process::exit;

use consumer::{builder::ConsumerBuilder, ConsumerMessage, StreamMessage};
use futures::{pin_mut, StreamExt};
mod consumer;

#[tokio::main]
async fn main() {
    if let Ok(mut consumer) = ConsumerBuilder::new()
        .add_stream("streams:images_data")
        .add_stream("streams:another_stream")
        .notification_group("builders")
        .block_time(5_000)
        .item_count(10)
        .skip_backlog_queue(false)
        .build()
    {
        if let Err(conn_err) = consumer.connect().await {
            panic!("cannot connect: {}", conn_err);
        }
        println!("listening as: {}", &consumer.name());
        let stream = consumer.stream().await.unwrap();
        pin_mut!(stream);
        while let Some(n) = stream.next().await {
            match n {
                Ok(stream_result) => match stream_result {
                    ConsumerMessage::EmptyStream(stream_name) => {
                        println!("{} is empty", stream_name)
                    }
                    ConsumerMessage::Message(StreamMessage {
                        stream_name,
                        len,
                        items,
                    }) => {
                        for item in items {
                            println!("message received from stream {}", stream_name);
                        }
                    }
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
