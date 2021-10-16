use crate::base_types::DiskLocation;
use crate::base_types::Extent;
use log::*;
use more_asserts::*;
use serde::{Deserialize, Serialize};
use std::mem;
use util::RangeTree;

#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
pub struct ExtentAllocatorPhys {
    pub first_valid_offset: u64,
    pub last_valid_offset: u64,
}

pub struct ExtentAllocator {
    state: std::sync::Mutex<ExtentAllocatorState>,
}

struct ExtentAllocatorState {
    phys: ExtentAllocatorPhys,
    allocatable: RangeTree,
    freeing: RangeTree, // not yet available for reallocation until this checkpoint completes
}

pub struct ExtentAllocatorBuilder {
    phys: ExtentAllocatorPhys,
    allocatable: RangeTree,
}

impl ExtentAllocatorBuilder {
    pub fn new(phys: ExtentAllocatorPhys) -> ExtentAllocatorBuilder {
        let mut metadata_allocatable = RangeTree::new();
        metadata_allocatable.add(
            phys.first_valid_offset,
            phys.last_valid_offset - phys.first_valid_offset,
        );
        ExtentAllocatorBuilder {
            phys,
            allocatable: metadata_allocatable,
        }
    }

    pub fn claim(&mut self, extent: &Extent) {
        self.allocatable.remove(extent.location.offset, extent.size);
    }

    pub fn allocatable_bytes(&self) -> u64 {
        self.allocatable.space()
    }
}

impl ExtentAllocator {
    /// Since the on-disk representation doesn't indicate which extents are
    /// allocated, they must all be .claim()ed first, via the
    /// ExtentAllocatorBuilder.
    pub fn open(builder: ExtentAllocatorBuilder) -> ExtentAllocator {
        ExtentAllocator {
            state: std::sync::Mutex::new(ExtentAllocatorState {
                phys: builder.phys,
                allocatable: builder.allocatable,
                freeing: RangeTree::new(),
            }),
        }
    }

    pub fn get_phys(&self) -> ExtentAllocatorPhys {
        self.state.lock().unwrap().phys
    }

    pub fn allocatable_bytes(&self) -> u64 {
        self.state.lock().unwrap().allocatable.space()
    }

    pub fn checkpoint_done(&self) {
        let mut state = self.state.lock().unwrap();

        // Space freed during this checkpoint is now available for reallocation.
        for (start, size) in mem::take(&mut state.freeing).iter() {
            state.allocatable.add(*start, *size);
        }
    }

    pub fn allocate(&self, min_size: u64, max_size: u64) -> Extent {
        let mut state = self.state.lock().unwrap();

        // find first segment where this fits, or largest free segment.
        // XXX keep size-sorted tree as well?
        let mut best_size = 0;
        let mut best_offset = 0;
        for (offset, size) in state.allocatable.iter() {
            if *size > best_size {
                best_size = *size;
                best_offset = *offset;
            }
            if *size >= max_size {
                best_size = max_size;
                break;
            }
        }
        assert_le!(best_size, max_size);

        if best_size < min_size {
            /*
            best_offset = state.phys.last_valid_offset;
            best_size = max_size64;
            state.phys.last_valid_offset += max_size64;
            */
            // XXX the block allocator will keep using this, and overwriting our
            // metadata, until we notify it.
            panic!(
                "no extents of at least {} bytes available; need to overwrite {} bytes of data blocks at offset {}",
                min_size, max_size, state.phys.last_valid_offset
            );
        } else {
            // remove segment from allocatable
            state.allocatable.remove(best_offset, best_size);
        }

        let this = Extent {
            location: DiskLocation {
                offset: best_offset,
            },
            size: best_size,
        };
        debug!("allocated {:?} for min={} max={}", this, min_size, max_size);
        this
    }

    /// extent can be a subset of what was previously allocated
    pub fn free(&self, extent: &Extent) {
        let mut state = self.state.lock().unwrap();
        state.freeing.add(extent.location.offset, extent.size);
    }
}
