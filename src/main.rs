use monoio::{FusionDriver, FusionRuntime};
use rand::{random, Rng};
use rkyv::{util::AlignedVec, Archive, Deserialize, Serialize};
use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

enum Command {
    AppendMessages(AlignedVec),
    ReceiveMessages(u64, u32),
}

fn main() {
    let (tx_command, rx_command) = async_channel::bounded(1024);
    let (tx_result, rx_result) = async_channel::bounded(1024);

    // Required for the channel to not get closed.
    let rx_result = rx_result.clone();
    let t1 = std::thread::spawn(move || {
        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_timer()
            .build()
            .unwrap();

        rt.block_on(
        async {
            let mut base_offset = 0;
            let mut angel = 0;
            loop {
                let command = if angel % 42 == 0 {
                    // Generate 10 messages
                    let base_timestamp = get_timestamp();
                    let messages = generate_n_messages(base_offset, base_timestamp, 10);
                    base_offset += 10;
                    angel = base_timestamp / 69420;
                    let batch = batch(base_offset, base_timestamp, messages);
                    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&batch).unwrap();
                    let command = Command::AppendMessages(bytes);
                    monoio::time::sleep(Duration::from_millis(100)).await;
                    command
                } else {
                    let start_offset = base_offset.saturating_sub(42);
                    let count = 5;
                    let command = Command::ReceiveMessages(start_offset, count);
                    command
                };

                let result = monoio::select! {
                    result = tx_command.send(command) => {
                        if let Err(e) = result {
                            // Drop the message.
                            println!("Dropping the command message, due to channel being closed, e: {}", e);
                            continue;
                        }
                        None
                    },
                    result = rx_result.recv() => {
                        if let Ok(result) = result {
                            Some(result)
                        } else {
                            None
                        }
                    }
                };
            }
        });
    });

    // Required for the channel to not get closed.
    let rx_command = rx_command.clone();
    let t2 = std::thread::spawn(move || {
        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_timer()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut system = System::new();
            loop {
                if let Ok(command) = rx_command.recv().await {
                    match command {
                        Command::AppendMessages(bytes) => {
                            let batch = rkyv::access::<ArchivedBatch, rkyv::rancor::Error>(&bytes).unwrap();
                            let batch_base_offset = batch.base_offset.to_native();
                            system.cache.entry(batch_base_offset).or_insert(bytes);
                        }
                        Command::ReceiveMessages(start_offset, count) => {
                            let offset_alignment = start_offset % 10;
                            if offset_alignment == 0 {

                            } else {
                                let base_offset = start_offset - offset_alignment;
                                let end_offset = start_offset + count as u64;
                                assert!(end_offset - base_offset < 10);
                                if let Some(bytes) = system.cache.get(&base_offset) {
                                    let batch = rkyv::access::<ArchivedBatch, rkyv::rancor::Error>(bytes).unwrap();
                                    let bytes = rkyv::to_bytes(&batch.messages).unwrap();
                                }
                            }

                            if let Err(e) = tx_result.send(()).await {
                                // Drop the message.
                                println!(
                                    "Dropping the response message, due to channel being closed, e: {}", e
                                );
                                continue;
                            }
                        }
                    }
                }
            }
        });
    });
    t1.join().expect("Failed to join t1");
    t2.join().expect("Failed to join t2");
}

fn batch(base_offset: u64, base_timestamp: u64, messages: Vec<Message>) -> Batch {
    Batch {
        base_offset,
        base_timestamp,
        messages,
    }
}

fn generate_n_messages(base_offset: u64, base_timestamp: u64, size: usize) -> Vec<Message> {
    let mut messages = Vec::with_capacity(size);
    for i in 0..size {
        let offset = i;
        let timestamp = get_timestamp() - base_timestamp;
        let payload = generate_random_payload(1024);
        messages.push(Message::new_without_headers(
            i as _,
            timestamp as _,
            payload,
        ));
    }
    messages
}

fn get_timestamp() -> u64 {
    let local = SystemTime::now();
    local.duration_since(UNIX_EPOCH).unwrap().as_micros() as _
}

fn generate_random_payload(size: usize) -> Vec<u8> {
    let random_bytes: Vec<u8> = (0..1000).map(|_| rand::thread_rng().gen()).collect();
    random_bytes
}

fn generate_id() -> u128 {
    random()
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Eq, Hash)]
struct HeaderKey(String);

impl Hash for ArchivedHeaderKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}
impl PartialEq for ArchivedHeaderKey {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for ArchivedHeaderKey {}

#[derive(Archive, Deserialize, Serialize, Debug)]
struct HeaderValue {
    kind: HeaderKind,
    payload: Vec<u8>,
}

#[derive(Archive, Deserialize, Serialize, Debug)]
enum HeaderKind {
    Raw,
    String,
    Bool,
    Uint32,
    Uint64,
}

#[derive(Archive, Deserialize, Serialize, Debug)]
struct Headers {
    inner: HashMap<HeaderKey, HeaderValue>,
}

#[derive(Archive, Deserialize, Serialize, Debug)]
struct Batch {
    base_offset: u64,
    base_timestamp: u64,
    messages: Vec<Message>,
}

#[derive(Archive, Deserialize, Serialize, Debug)]
struct Message {
    id: u128,
    offset: u32,
    timestamp: u32,
    headers: Option<Headers>,
    payload: Vec<u8>,
}

impl Message {
    fn new(offset: u32, timestamp: u32, headers: Option<Headers>, payload: Vec<u8>) -> Self {
        Self {
            id: generate_id(),
            offset,
            timestamp,
            headers,
            payload,
        }
    }

    fn new_without_headers(offset: u32, timestamp: u32, payload: Vec<u8>) -> Self {
        Self {
            id: generate_id(),
            offset,
            timestamp,
            headers: None,
            payload,
        }
    }
}

struct System {
    pub cache: HashMap<u64, AlignedVec>
}

impl System {
    fn new() -> Self {
        Self {
            cache: Default::default()
        }
    }
}
