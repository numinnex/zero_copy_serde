use rkyv::{ptr_meta::from_raw_parts, util::AlignedVec, Archive, Deserialize, Serialize};
use core::arch;
use std::{
    collections::HashMap, fs::OpenOptions, io::Write, time::{SystemTime, UNIX_EPOCH}
};

fn main() {
    let file = OpenOptions::new().create(true).read(true).write(true).open("test").unwrap();
    let value = Foo {
        offset: 42,
        timestamp: 62,
        range: 92,
    };
    let mut vec = AlignedVec::<512>::with_capacity(512);
    unsafe {vec.set_len(512)};
    println!("vec: {:?}", vec);
    let mut bytes = rkyv::api::high::to_bytes_in::<AlignedVec<512>, rkyv::rancor::Error>(&value, vec).unwrap();
    println!("bytes: {:?}", bytes);
    let val = rkyv::access::<ArchivedFoo, rkyv::rancor::Error>(&bytes).unwrap();
    println!("val: {:?}", val);
}

pub fn val_align_up(value: u64, alignment: u64) -> u64 {
    (value + alignment - 1) & !(alignment - 1)
}

#[derive(Serialize, Deserialize, Archive)]
#[rkyv(
    compare(PartialEq),
    derive(Debug)
)]
struct Foo {
    offset: u64,
    timestamp: u64,
    range: u32,
}

#[derive(Serialize, Deserialize, Archive)]
struct Batch {
    header: Header,
    payload: Vec<u8>,
}

#[derive(Serialize, Deserialize, Archive, Debug)]
#[rkyv(
    compare(PartialEq),
    derive(Debug)
)]
struct Header {
    base_offset: u64,
    base_timestmap: u64,
    len: u32,
}

fn align_up(value: u64, align: u64) -> u64 {
    (value + align - 1) & !(align - 1)
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
struct Message {
    offset: u32,
    timestamp: u32,
    payload: Vec<u8>,
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
