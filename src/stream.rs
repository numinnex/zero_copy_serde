use std::{pin::Pin, task::Poll};

use futures::Stream;
use futures_util::AsyncBufRead;
use monoio::buf::IoBufMut;
use pin_project::pin_project;

use crate::{
    dma_buf::{self, IoBuf},
    Message,
};

pub trait LogReader<Buf>
where
    Buf: IoBufMut + IoBuf,
{
    fn read_blocks(
        &self,
        position: u64,
        limit: u64,
    ) -> impl Stream<Item = Result<Buf, std::io::Error>>;
}

#[pin_project]
pub struct RetainedMessageStream<R>
where
    R: AsyncBufRead + Unpin,
{
    sector_size: u64,
    read_bytes: u64,
    message_length: u32,
    state: State,
    #[pin]
    reader: R,
}

impl<R> MessageStream<R>
where
    R: AsyncBufRead + Unpin,
{
    pub fn new(reader: R, sector_size: u64) -> Self {
        Self {
            read_bytes: 0,
            state: State::Ready,
            message_length: 0,
            sector_size,
            reader,
        }
    }
}

#[derive(Copy, Clone)]
enum Reading {
    Length,
    Message,
}

enum State {
    Ready,
    Pending(Reading, usize, Vec<u8>),
}

impl<R> Stream for MessageStream<R>
where
    R: AsyncBufRead + Unpin,
{
    type Item = Result<Message, std::io::Error>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let state = std::mem::replace(this.state, State::Ready);

        let mut read_exact = |reading: Reading,
                              buf: &mut [u8],
                              cx: &mut std::task::Context<'_>|
         -> Poll<Result<(), std::io::Error>> {
            let mut read_offset = 0;
            while read_offset < buf.len() {
                let n = match this.reader.read(&mut buf[read_offset..]).poll_unpin(cx)? {
                    Poll::Ready(val) => val,
                    Poll::Pending => {
                        let len = buf.len();
                        let mut new_buf = vec![0; len];
                        new_buf.copy_from_slice(&buf);
                        *this.state = State::Pending(reading, read_offset, new_buf);
                        return Poll::Pending;
                    }
                };
                read_offset += n;
            }
            Poll::Ready(Ok(()))
        };

        match state {
            State::Ready => {}
            State::Pending(reading, read, mut buf) => {
                match reading {
                    Reading::Length => {
                        if let Err(e) =
                            futures::ready!(read_exact(Reading::Length, &mut buf[read..], cx))
                        {
                            return Some(Err(e.into())).into();
                        }
                        let length = u32::from_le_bytes(buf[0..4].try_into().unwrap());
                        *this.message_length = length;

                        let mut payload = Vec::with_capacity(length as _);
                        payload.fill(0);
                        if let Err(e) =
                            futures::ready!(read_exact(Reading::Message, &mut payload, cx))
                        {
                            return Some(Err(e.into())).into();
                        }
                        *this.read_bytes += length as u64 + 4;
                        if this.read_bytes >= this.message_length {
                            // This is a temp solution, to the padding that Direct I/O requires.
                            // Later on, we could encode that information in our batch header
                            // for example Header { batch_length: usize, padding: usize }
                            // and use the padding to advance the reader further.
                            /*
                            let total_batch_length = *this.batch_length + RETAINED_BATCH_OVERHEAD;
                            let adjusted_size = io::val_align_up(total_batch_length, *this.sector_size);
                            */
                            let total_batch_length = *this.message_length + 4;
                            let adjusted_size =
                                dma_buf::val_align_up(total_batch_length, *this.sector_size);
                            let diff = adjusted_size - total_batch_length;
                            this.reader.consume_unpin(diff as _);
                            *this.header_read = false;
                        }

                        let message = RetainedMessage::try_from_bytes(payload.freeze()).unwrap();
                        return Poll::Ready(Some(Ok(message)));
                    }
                    Reading::Message => {
                        if let Err(e) =
                            futures::ready!(read_exact(Reading::Message, &mut buf[read..], cx))
                        {
                            return Some(Err(e.into())).into();
                        }
                        *this.read_bytes += *this.message_length as u64 + 4;
                        if this.read_bytes >= this.message_length {
                            // This is a temp solution, to the padding that Direct I/O requires.
                            // Later on, we could encode that information in our batch header
                            // for example Header { batch_length: usize, padding: usize }
                            // and use the padding to advance the reader further.
                            /*
                            let total_batch_length = *this.batch_length + RETAINED_BATCH_OVERHEAD;
                            let adjusted_size = io::val_align_up(total_batch_length, *this.sector_size);
                            */
                            let total_batch_length = *this.message_length + 4;
                            let sectors = total_batch_length.div_ceil(*this.sector_size);
                            let adjusted_size =
                                dma_buf::val_align_up(total_batch_length, *this.sector_size);
                            let diff = adjusted_size - total_batch_length;
                            this.reader.consume_unpin(diff as _);
                            *this.header_read = false;
                        }

                        let message = RetainedMessage::try_from_bytes(buf.into()).unwrap();
                        return Poll::Ready(Some(Ok(message)));
                    }
                }
            }
        }

        let mut buf = [0u8; 4];
        if let Err(e) = futures::ready!(read_exact(Reading::Length, &mut buf, cx)) {
            return Some(Err(e.into())).into();
        }
        let length = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        *this.message_length = length;

        let mut payload = Vec::with_capacity(length as _);
        payload.fill(0);
        if let Err(e) = futures::ready!(read_exact(Reading::Message, &mut payload, cx)) {
            return Some(Err(e.into())).into();
        }
        *this.read_bytes += length as u64 + 4;
        if this.read_bytes >= this.message_length {
            // This is a temp solution, to the padding that Direct I/O requires.
            // Later on, we could encode that information in our batch header
            // for example Header { batch_length: usize, padding: usize }
            // and use the padding to advance the reader further.
            /*
            let total_batch_length = *this.batch_length + RETAINED_BATCH_OVERHEAD;
            let adjusted_size = io::val_align_up(total_batch_length, *this.sector_size);
            */
            let total_batch_length = *this.message_length + 4;
            let adjusted_size = dma_buf::val_align_up(total_batch_length, *this.sector_size);
            let diff = adjusted_size - total_batch_length;
            this.reader.consume_unpin(diff as _);
            *this.header_read = false;
        }

        let message = RetainedMessage::try_from_bytes(payload.freeze()).unwrap();
        Poll::Ready(Some(Ok(message)))
    }
}
