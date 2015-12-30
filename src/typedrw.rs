use std::mem;
use std::slice;
use std::ops;
use std::fs::File;
use std::marker::PhantomData;
use memmap::{ Mmap, Protection, MmapViewSync };

pub struct TypedMemoryMap<T:Copy> {
    map:    MmapViewSync,       // mapped file
    len:    usize,              // in bytes (needed because map extends to full block)
    phn:    PhantomData<T>,
}

impl<T:Copy> TypedMemoryMap<T> {
    pub fn new(filename: String) -> TypedMemoryMap<T> {
        let file = File::open(filename).unwrap();
        let size = file.metadata().unwrap().len() as usize;
        TypedMemoryMap {
            map: Mmap::open(&file, Protection::Read).unwrap().into_view_sync(),
            len: size / mem::size_of::<T>(),
            phn: PhantomData,
        }
    }
}

impl<T:Copy> ops::Index<ops::RangeFull> for TypedMemoryMap<T> {
    type Output = [T];
    #[inline]
    fn index(&self, _index: ops::RangeFull) -> &[T] {
        unsafe { slice::from_raw_parts(self.map.ptr() as *const T, self.len) }
    }
}
