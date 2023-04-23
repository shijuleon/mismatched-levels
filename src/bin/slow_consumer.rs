use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use rand::Rng;
use std::{thread, time};

struct StochasticPerformer {}
mod producer;

fn read_messages(consumer: &mut kafka::consumer::Consumer, rng: &mut rand::rngs::ThreadRng) {
  for ms in consumer.poll().unwrap().iter() {
    for m in ms.messages() {
      println!("Received offset: {}", m.offset);
      let event: crate::producer::Event = serde_json::from_slice(&m.value.to_vec()).unwrap();

      if rng.gen::<bool>() {
        println!("Failed, retrying: {:?}: {}", event, m.offset);
        for i in 1..rng.gen_range(0..10) {
          println!("Retry: {}", i);
          thread::sleep(time::Duration::from_secs(1));
        }
      }

      println!("Done processing: {:?}", event);
      consumer
        .consume_message("mismatched-levels", 0, m.offset)
        .unwrap();
      consumer.commit_consumed().unwrap();
      // consumer.client_mut().commit_offset("slow-group", "mismatched-levels", 0, m.offset).unwrap();
    }
  }
}

fn main() {
  let mut consumer = Consumer::from_hosts(vec!["localhost:9092".to_owned()])
    .with_topic("mismatched-levels".to_owned())
    .with_fallback_offset(FetchOffset::Earliest)
    .with_group("slow-group".to_owned())
    .with_offset_storage(GroupOffsetStorage::Kafka)
    .create()
    .unwrap();

  let mut rng = rand::thread_rng();

  loop {
    read_messages(&mut consumer, &mut rng);
  }
}
