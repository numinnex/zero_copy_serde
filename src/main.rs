use rand::{random, Rng};
use rkyv::{
    boxed::ArchivedBox, rancor::Fallible, rend::u32_le, ser::{Allocator, Writer}, util::AlignedVec, vec::ArchivedVec, with::{InlineAsBox, With}, Archive, Deserialize, DeserializeUnsized, Serialize
};
use std::{
    collections::HashMap, ops::{Deref, DerefMut}, time::{SystemTime, UNIX_EPOCH}
};

#[derive(Archive, Serialize, Deserialize)]
struct Example {
    name: String,
    value: Vec<i32>,
}

struct Test<'a> {
    message: &'a mut ArchivedMessage
}

impl<'a> Deref for Test<'a> {
    type Target = ArchivedMessage;

    fn deref(&self) -> &Self::Target {
        self.message
    }
}

impl<'a> DerefMut for Test<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.message
    }
}


fn main() {
        let base_offset = 0;
        let base_timestamp = get_timestamp();
        let messages = generate_n_messages(base_offset, base_timestamp, 10);
        let batch = batch(base_offset, base_timestamp, messages);

        let mut bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&batch).unwrap();
        let mut serde_batch = unsafe { rkyv::access_unchecked_mut::<ArchivedBatch>(&mut bytes) };
        println!("offset before: {}", &serde_batch.messages[0].offset);
        rkyv::munge::munge!(let ArchivedBatch {base_offset, base_timestamp, mut messages} = serde_batch);
        let len = messages.len();
        let ptr = messages.as_ptr();
        let slice = unsafe {std::slice::from_raw_parts_mut(ptr as *mut ArchivedMessage, len)};
        for message in slice {
            message.offset = u32_le::from_native(32);
        }
        let mut serde_batch = unsafe { rkyv::access_unchecked::<ArchivedBatch>(&bytes) };
        println!("offset after: {}", &serde_batch.messages[0].offset);
    }

struct Foo {
    offset: u64,
    timestamp: u64,
}

struct ServerBatch {
    base_offset: u64,
    base_timestamp: u64,
    messages: Vec<Message>,
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
        messages.push(Message::new(
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
    let random_bytes = vec![69u8; size];
    random_bytes
}

fn generate_id() -> u128 {
    69420u128
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
    payload: Vec<u8>,
}

impl Archive for ArchivedMessage {
    type Archived = Self;

    type Resolver = MessageResolver;

    fn resolve(&self, resolver: Self::Resolver, out: rkyv::Place<Self::Archived>) {
        rkyv::munge::munge!(
            let Self { id, offset, timestamp, payload } = out);

        self.id.resolve(resolver.id, id);
        self.offset.resolve(resolver.offset, offset);
        self.timestamp.resolve(resolver.timestamp, timestamp);
        ArchivedVec::resolve_from_len(self.payload.len(), resolver.payload, payload);
    }
}

impl<S: Fallible + Writer + Allocator + ?Sized> Serialize<S> for ArchivedMessage {
    fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        Ok(MessageResolver {
            id: self.id.serialize(serializer)?,
            offset: self.offset.serialize(serializer)?,
            timestamp: self.timestamp.serialize(serializer)?,
            payload: ArchivedVec::serialize_from_slice(self.payload.as_slice(), serializer)?,
        })
    }
}

impl Message {
    fn new(offset: u32, timestamp: u32, payload: Vec<u8>) -> Self {
        Self {
            id: generate_id(),
            offset,
            timestamp,
            payload,
        }
    }
}

struct System {
    pub cache: HashMap<u64, AlignedVec>,
}

impl System {
    fn new() -> Self {
        Self {
            cache: Default::default(),
        }
    }
}
