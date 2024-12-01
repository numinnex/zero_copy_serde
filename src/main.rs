use std::{
    marker::PhantomData,
    os::unix::prelude::OpenOptionsExt,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

use dma_buf::{DmaBuf, IoBuf};
use futures::{Future, FutureExt, Stream};
use futures_util::TryStreamExt;
use monoio::{
    buf::IoBufMut,
    fs::{File, OpenOptions},
    task::JoinHandle,
};
use pin_project::pin_project;
use rand::Rng;
use storage::DmaStorage;
use stream::LogReader;

pub mod dma_buf;
pub mod storage;
pub mod stream;
const O_DIRECT: i32 = 0x4000;

#[monoio::main(worker_threads = 1, driver = "io_uring")]
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
    let storage = Storage::new(file);
    let log: Log<Storage, DmaBuf> = Log::new(storage, block_size);
    let reader = log.read_blocks(0, 1000).into_async_read();
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
        buffer[0..4].copy_from_slice(&self.length.to_le_bytes());
        buffer[4..20].copy_from_slice(&self.id.to_le_bytes());
        buffer[20..28].copy_from_slice(&self.offset.to_le_bytes());
        buffer[28..36].copy_from_slice(&self.timestamp.to_le_bytes());
        buffer[36..].copy_from_slice(&self.bytes);
    }

    fn from_bytes(data: &[u8]) -> Self {
        let id = u128::from_be_bytes(data[0..16].try_into().unwrap());
        let offset = u64::from_be_bytes(data[16..24].try_into().unwrap());
        let timestamp = u64::from_be_bytes(data[24..32].try_into().unwrap());

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

fn generate_messages_1gb() -> Vec<Message> {
    let mut rng = rand::thread_rng();
    let mut total_size: usize = 0;
    let mut messages = Vec::new();

    while total_size < 1_073_741_824 {
        let message_length = rng.gen_range(1024..8192);
        let bytes: Vec<u8> = (0..message_length).map(|_| rng.gen()).collect();
        let message = Message {
            length: message_length as u32 + 16 + 8 + 8,
            id: rng.gen(),
            offset: total_size as u64,
            timestamp: rng.gen(),
            bytes,
        };

        total_size += message.total_length() as usize;
        messages.push(message);
    }

    messages
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
        Self {
            file: Rc::new(file),
        }
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
        if *this.position > *this.limit {
            return Poll::Ready(None);
        }

        if let Some(fut) = this.future.as_mut().as_pin_mut() {
            let (result, buf) = futures::ready!(fut.poll(cx));
            match result {
                Ok(_) => {
                    *this.position += *this.size as u64;
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
                        *this.position += *this.size as u64;
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
