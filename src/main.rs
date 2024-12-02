use std::{
    marker::PhantomData,
    os::unix::prelude::{MetadataExt, OpenOptionsExt},
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

use dma_buf::{DmaBuf, IoBuf};
use futures::{Future, FutureExt, Stream};
use futures_util::{StreamExt, TryStreamExt};
use monoio::{
    buf::IoBufMut,
    fs::{File, OpenOptions},
    task::JoinHandle,
};
use pin_project::pin_project;
use rand::Rng;
use storage::DmaStorage;
use stream::{LogReader, MessageStream};

pub mod dma_buf;
pub mod storage;
pub mod stream;
const O_DIRECT: i32 = 0x4000;

#[monoio::main(worker_threads = 1, driver = "io_uring", entries = 1024)]
async fn main() {
    let file_path = "test";
    let block_size = 100 * 4096;

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(O_DIRECT)
        .create(true)
        .open(file_path)
        .await
        .unwrap();

    let (messages, max_offset) = generate_messages_1gb();
    let mut position = 0;
    let bytes = messages.into_iter().map(|msg| {
        let size = msg.total_length();
        let size_aligned = dma_buf::val_align_up(size as u64, 4096);
        let mut buf = DmaBuf::new(size_aligned as usize);
        msg.extend(buf.as_mut());
        let return_val = (position, buf);
        position += size_aligned;
        return_val
    });
    let file = Rc::new(file);
    futures_util::stream::iter(bytes)
        .for_each_concurrent(None, |(position, bytes)| {
            let file = file.clone();
            async move {
                let file = file.clone();
                let (result, _) = file.write_all_at(bytes, position).await;
                result.unwrap();
            }
        })
        .await;
    drop(file);

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(O_DIRECT)
        .create(true)
        .open(file_path)
        .await
        .unwrap();

    let limit = file.metadata().await.unwrap().size();
    let storage = Storage::new(file);
    let log: Log<Storage, DmaBuf> = Log::new(storage, block_size);
    let reader = log.read_blocks(0, limit).into_async_read();
    let stream = MessageStream::new(reader, 4096);
    let _messages = stream
        .inspect_ok(|msg| println!("message offset: {}, max_offset: {}", msg.offset, max_offset))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    println!("done reading messages");
}

pub struct Message {
    pub length: u32,
    pub id: u128,
    pub offset: u64,
    pub timestamp: u64,
    pub bytes: Vec<u8>,
}

impl Message {
    fn total_length(&self) -> u32 {
        16 + 8 + 8 + 4 + self.bytes.len() as u32
    }

    fn extend(&self, buffer: &mut [u8]) {
        // Serialize and append each field to the buffer.
        let len = self.total_length() as usize;
        buffer[0..4].copy_from_slice(&self.length.to_le_bytes());
        buffer[4..20].copy_from_slice(&self.id.to_le_bytes());
        buffer[20..28].copy_from_slice(&self.offset.to_le_bytes());
        buffer[28..36].copy_from_slice(&self.timestamp.to_le_bytes());
        buffer[36..len].copy_from_slice(&self.bytes);
    }

    fn from_bytes(data: &[u8]) -> Self {
        let id = u128::from_le_bytes(data[0..16].try_into().unwrap());
        let offset = u64::from_le_bytes(data[16..24].try_into().unwrap());
        let timestamp = u64::from_le_bytes(data[24..32].try_into().unwrap());

        let bytes = data[32..].to_vec();

        Message {
            length: (16 + 8 + 8 + bytes.len()) as _,
            id,
            offset,
            timestamp,
            bytes,
        }
    }
}

fn generate_messages_1gb() -> (Vec<Message>, u64) {
    let mut rng = rand::thread_rng();
    let mut total_size: usize = 0;
    let mut messages = Vec::new();

    let mut i = 0;
    while total_size < 1_073_741_824 {
        i += 1;
        let message_length = rng.gen_range(1024..8192);
        let bytes: Vec<u8> = (0..message_length).map(|_| rng.gen()).collect();
        let message = Message {
            length: message_length as u32 + 16 + 8 + 8,
            id: i as u128,
            offset: i,
            timestamp: i,
            bytes,
        };

        total_size += message.total_length() as usize;
        messages.push(message);
    }

    (messages, i)
}

pub struct Storage {
    file: Rc<File>,
}

impl<T> DmaStorage<T> for Storage
where
    T: IoBufMut + IoBuf,
{
    type ReadResult = ReadExactAt<T>;

    fn read_sectors(&self, buf: T, position: u64) -> Self::ReadResult {
        let file = self.file.clone();
        let handle = monoio::spawn(async move { file.read_exact_at(buf, position).await });
        ReadExactAt { handle }
    }
}

impl Storage {
    pub fn new(file: File) -> Self {
        let file = Rc::new(file);
        Self { file }
    }
}

#[pin_project]
pub struct ReadExactAt<T>
where
    T: IoBufMut,
{
    #[pin]
    handle: JoinHandle<(std::io::Result<()>, T)>,
}

impl<T> Future for ReadExactAt<T>
where
    T: IoBufMut,
{
    type Output = (std::io::Result<()>, T);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let result = futures::ready!(this.handle.poll_unpin(cx));
        Poll::Ready(result)
    }
}

#[pin_project]
struct BlockStream<'a, S, Buf>
where
    Buf: IoBufMut,
    S: DmaStorage<Buf>,
{
    position: u64,
    limit: u64,
    size: usize,
    storage: &'a S,
    #[pin]
    future: Option<S::ReadResult>,
}

impl<'a, S, Buf> Stream for BlockStream<'a, S, Buf>
where
    Buf: IoBufMut + IoBuf,
    S: DmaStorage<Buf>,
{
    type Item = Result<Buf, std::io::Error>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if *this.position >= *this.limit {
            println!(
                "reached here, with position: {}, limit: {}",
                *this.position, *this.limit
            );
            return Poll::Ready(None);
        }

        if let Some(fut) = this.future.as_mut().as_pin_mut() {
            let (result, buf) = futures::ready!(fut.poll(cx));
            match result {
                Ok(_) => {
                    *this.position += buf.len() as u64;
                    this.future.set(None);
                    return Poll::Ready(Some(Ok(buf)));
                }
                Err(e) => {
                    return Poll::Ready(Some(Err(e)));
                }
            }
        } else {
            let buf_size = std::cmp::min(*this.size, (*this.limit - *this.position) as usize);
            let buf = Buf::new(buf_size);
            let mut fut = this.storage.read_sectors(buf, *this.position);
            match fut.poll_unpin(cx) {
                Poll::Ready((result, buf)) => match result {
                    Ok(_) => {
                        *this.position += buf_size as u64;
                        this.future.set(None);
                        return Poll::Ready(Some(Ok(buf)));
                    }
                    Err(e) => {
                        return Poll::Ready(Some(Err(e)));
                    }
                },
                Poll::Pending => {
                    this.future.set(Some(fut));
                    Poll::Pending
                }
            }
        }
    }
}

struct Log<S, Buf>
where
    Buf: IoBufMut + IoBuf,
    S: DmaStorage<Buf>,
{
    storage: S,
    block_size: usize,
    _phantom: PhantomData<Buf>,
}

impl<S, Buf> Log<S, Buf>
where
    Buf: IoBufMut + IoBuf,
    S: DmaStorage<Buf>,
{
    pub fn new(storage: S, block_size: usize) -> Self {
        Self {
            storage,
            block_size,
            _phantom: PhantomData,
        }
    }
}

impl<S, Buf> LogReader<Buf> for Log<S, Buf>
where
    Buf: IoBufMut + IoBuf,
    S: DmaStorage<Buf>,
{
    fn read_blocks(
        &self,
        position: u64,
        limit: u64,
    ) -> impl Stream<Item = Result<Buf, std::io::Error>> {
        BlockStream {
            position,
            limit,
            future: None,
            storage: &self.storage,
            size: self.block_size,
        }
    }
}
