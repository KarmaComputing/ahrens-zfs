use bytes::Bytes;
use more_asserts::*;
use serde::Deserialize;
use serde::Serialize;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Result;
use std::ops::Deref;

/// exists just to reduce Debug output on fields we don't really care about
#[derive(Serialize, Deserialize, Clone)]
pub struct TerseVec<T>(pub Vec<T>);

impl<T> Debug for TerseVec<T> {
    fn fmt(&self, fmt: &mut Formatter) -> Result {
        fmt.write_fmt(format_args!("[...{} elements...]", self.0.len()))
    }
}

impl<T> From<Vec<T>> for TerseVec<T> {
    fn from(vec: Vec<T>) -> Self {
        Self(vec)
    }
}

pub struct AlignedBytes {
    alignment: usize,
    bytes: Bytes,
}

impl AlignedBytes {
    pub fn copy_from_slice(slice: &[u8], alignment: usize) -> Self {
        let mut vec = AlignedVec::with_capacity(slice.len(), alignment);
        vec.extend_from_slice(slice);
        vec.into()
    }

    pub fn alignment(&self) -> usize {
        self.alignment
    }
}

impl Deref for AlignedBytes {
    type Target = Bytes;
    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

impl From<Bytes> for AlignedBytes {
    fn from(bytes: Bytes) -> Self {
        AlignedBytes {
            alignment: 1,
            bytes,
        }
    }
}

impl From<AlignedBytes> for Bytes {
    fn from(aligned_bytes: AlignedBytes) -> Self {
        aligned_bytes.bytes
    }
}

impl From<AlignedVec> for AlignedBytes {
    fn from(aligned_vec: AlignedVec) -> Self {
        let unaligned_bytes: Bytes = aligned_vec.vec.into();
        let bytes = unaligned_bytes.slice(aligned_vec.pad..);
        assert_eq!(bytes.as_ptr().align_offset(aligned_vec.alignment), 0);
        AlignedBytes {
            alignment: aligned_vec.alignment,
            bytes,
        }
    }
}

pub struct AlignedVec {
    alignment: usize,
    vec: Vec<u8>,
    pad: usize,
}

impl AlignedVec {
    pub fn with_capacity(capacity: usize, alignment: usize) -> Self {
        assert_ne!(alignment, 0);
        let mut vec: Vec<u8> = Vec::with_capacity(capacity + alignment);
        let pad = vec.as_ptr().align_offset(alignment);
        assert_lt!(pad, alignment);
        vec.resize(pad, 0);
        let aligned_vec = AlignedVec {
            alignment,
            vec,
            pad,
        };
        aligned_vec.verify();
        aligned_vec
    }

    fn verify(&self) {
        assert_eq!(self.as_ptr().align_offset(self.alignment), 0);
    }

    pub fn extend_from_slice(&mut self, slice: &[u8]) {
        // We can't allow the vec to be resized, as that could violate the alignment constraint.
        assert_le!(slice.len(), self.vec.capacity() - self.vec.len());
        self.vec.extend_from_slice(slice);
        self.verify();
    }

    pub fn len(&self) -> usize {
        self.vec.len() - self.pad
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.vec[self.pad..].as_ptr()
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.vec[self.pad..].as_mut_ptr()
    }

    /// # Safety
    ///
    /// See `Vec::set_len()`
    /// - `new_len` must be <= the capacity specified by `.with_capacity()`.
    /// - The elements at `old_len..new_len` must be initialized.
    pub unsafe fn set_len(&mut self, new_len: usize) {
        assert_le!(self.pad + new_len, self.vec.capacity());
        self.vec.set_len(self.pad + new_len);
    }
}
