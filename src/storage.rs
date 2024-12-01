use futures::Future;
use monoio::buf::IoBufMut;

pub trait DmaStorage<T: IoBufMut> {
    type ReadResult: Future<Output = (std::io::Result<()>, T)> + Unpin;
    fn read_sectors(&self, buf: T, position: u64) -> Self::ReadResult;
}
