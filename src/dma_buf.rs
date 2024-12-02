use futures::Future;
pub trait Storage<Buf>
where
    Buf: IoBuf,
{
    //TODO: support taking as an input Iterator<Item = B> and used write_vectored.
    // At this moment the mutable borrow of self is unnecessary, but I'll leave it like that,
    // in case when `File` would be stored directly in the storage, rather than being created every time
    // this method is called.
    fn write_sectors(&mut self, buf: Buf) -> impl Future<Output = Result<u32, std::io::Error>>;
    fn read_sectors(
        &self,
        position: u64,
        buf: Buf,
    ) -> impl Future<Output = (std::io::Result<()>, Buf)> + Sized;
}

use std::{
    alloc::{self, Layout},
    ptr,
};

unsafe impl monoio::buf::IoBuf for DmaBuf {
    fn read_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }
}

unsafe impl monoio::buf::IoBufMut for DmaBuf {
    fn write_ptr(&mut self) -> *mut u8 {
        self.as_ptr_mut()
    }

    fn bytes_total(&mut self) -> usize {
        self.len()
    }

    unsafe fn set_init(&mut self, _pos: usize) {}
}

#[derive(Debug)]
pub struct DmaBuf {
    data: ptr::NonNull<u8>,
    layout: Layout,
    size: usize,
}

impl DmaBuf {}

// SAFETY: fuck safety.
unsafe impl Send for DmaBuf {}
unsafe impl Sync for DmaBuf {}

impl Drop for DmaBuf {
    fn drop(&mut self) {
        unsafe {
            alloc::dealloc(self.data.as_ptr(), self.layout);
        }
    }
}

impl IoBuf for DmaBuf {
    fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    fn as_ptr_mut(&mut self) -> *mut u8 {
        self.data.as_ptr()
    }

    fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.size) }
    }

    fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.as_ptr_mut(), self.size) }
    }

    fn len(&self) -> usize {
        self.size
    }

    fn new(size: usize) -> Self {
        assert!(size > 0);
        assert!(size % 512 == 0);
        let layout =
            Layout::from_size_align(size, 4096).expect("Falied to create layout for DmaBuf");
        let data_ptr = unsafe { alloc::alloc(layout) };
        let data = ptr::NonNull::new(data_ptr).expect("DmaBuf data_ptr is not null");

        Self { data, layout, size }
    }
}

impl AsRef<[u8]> for DmaBuf {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsMut<[u8]> for DmaBuf {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_bytes_mut()
    }
}

pub trait IoBuf: AsRef<[u8]> + AsMut<[u8]> {
    fn new(size: usize) -> Self;
    fn len(&self) -> usize;
    fn as_ptr(&self) -> *const u8;
    fn as_ptr_mut(&mut self) -> *mut u8;
    fn as_bytes(&self) -> &[u8];
    fn as_bytes_mut(&mut self) -> &mut [u8];
}

pub fn val_align_up(value: u64, alignment: u64) -> u64 {
    (value + alignment - 1) & !(alignment - 1)
}

pub fn val_align_down(value: u64, alignment: u64) -> u64 {
    value & !(alignment - 1)
}
