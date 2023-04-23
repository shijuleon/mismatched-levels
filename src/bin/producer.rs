use kafka::producer::{Producer, Record, RequiredAcks};
use std::time::Duration;
use rand::prelude::*;
use serde_derive::{Serialize, Deserialize};
use serde_json::json;

#[derive(Debug, Serialize, Deserialize)]
enum EventType {
  Android,
  Ios,
  Web
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Event {
  eid: u32, 
  etype: EventType,
}

fn main() {
  let mut producer = Producer::from_hosts(vec!["localhost:9092".to_owned()])
    .with_ack_timeout(Duration::from_secs(1))
    .with_required_acks(RequiredAcks::One)
    .create()
    .unwrap();

  let mut buf = String::with_capacity(2);
  for i in 1..=20 {
    let mut rng = rand::thread_rng();
    let event_type = match rng.gen_range(0..3) {
      0 => EventType::Android,
      1 => EventType::Ios,
      _ => EventType::Web,
    };

    let event = Event {
      eid: i,
      etype: event_type,
    };
    let j = json!(event);
    println!("{}", j);

    producer
      .send(&Record::from_value("mismatched-levels", j.to_string().as_bytes()))
      .unwrap();
    buf.clear();
  }
}
