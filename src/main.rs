use rkyv::{api::high::to_bytes_in, to_bytes, util::AlignedVec, Archive, Deserialize, Serialize};

#[monoio::main(worker_threads = 1, driver = "io_uring", entries = 1024)]
async fn main() {
    let messages = generate_messages(69);
    let batch = Batch {
        messages: messages.clone(),
    };
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&batch).unwrap();
    let archived = rkyv::access::<ArchivedBatch, rkyv::rancor::Error>(&bytes).unwrap();

    for (idx, message) in archived.messages.iter().enumerate() {
        let original_message = &messages[idx];
        println!("original_message offset: {}, new_message offset: {}, original_message timestamp: {}, new_message timestamp: {}"
    , original_message.base_offset, message.base_offset, original_message.base_timestamp, message.base_timestamp);
        assert_eq!(&messages[idx], message);
    }
}

fn generate_messages(size: usize) -> Vec<Message> {
    let mut result = Vec::new();
    for iter in 0..size {
        result.push(Message::new(iter as u64, iter as u64));
    }
    result
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
struct Batch {
    pub messages: Vec<Message>,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone)]
#[rkyv(compare(PartialEq), derive(Debug))]
struct Message {
    pub base_offset: u64,
    pub base_timestamp: u64,
    payload: Vec<u8>,
}

impl Message {
    pub fn new(base_offset: u64, base_timestamp: u64) -> Self {
        let payload = generate_random_bytes(2392);
        Self {
            base_offset,
            base_timestamp,
            payload,
        }
    }
}

// Simple Linear Congruential Generator (LCG) for pseudorandom number generation
fn lcg(seed: u64) -> u64 {
    const A: u64 = 1664525;
    const C: u64 = 1013904223;
    const M: u64 = u64::MAX;
    (A.wrapping_mul(seed).wrapping_add(C)) % M
}

// Seed the generator using the current time
fn seed_from_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs()
}

use std::time::{SystemTime, UNIX_EPOCH};

fn generate_random_bytes(size: usize) -> Vec<u8> {
    let mut random_bytes = Vec::with_capacity(size);
    let mut seed = seed_from_time();

    for _ in 0..size {
        seed = lcg(seed);
        random_bytes.push((seed & 0xFF) as u8); // Take the least significant byte
    }

    random_bytes
}
