use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

fn main() {
  let mut consumer = Consumer::from_hosts(vec!["localhost:9092".to_owned()])
    .with_topic("mismatched-levels".to_owned())
    .with_fallback_offset(FetchOffset::Earliest)
    .with_group("regular-group".to_owned())
    .with_offset_storage(GroupOffsetStorage::Kafka)
    .create()
    .unwrap();
  loop {
    for ms in consumer.poll().unwrap().iter() {
      for m in ms.messages() {
        println!("{:?}",  String::from_utf8(m.value.to_vec()).unwrap());
      }
      consumer.consume_messageset(ms).unwrap();
    }
    consumer.commit_consumed().unwrap();
  }
}
