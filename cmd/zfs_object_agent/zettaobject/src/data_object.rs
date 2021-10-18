use crate::base_types::*;
use crate::object_access::{ObjectAccess, ObjectAccessStatType};
use anyhow::{Context, Result};
use bytes::Bytes;
use log::*;
use more_asserts::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::fmt::Display;
use std::time::Instant;
use zettacache::base_types::*;

const NUM_DATA_PREFIXES: u64 = 64;

#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
pub struct DataObjectHeader {
    pub guid: PoolGuid,      // redundant with key, for verification
    pub object: ObjectId,    // redundant with key, for verification
    pub blocks_size: u32,    // sum of blocks.values().len()
    pub min_block: BlockId,  // inclusive (all blocks are >= min_block)
    pub next_block: BlockId, // exclusive (all blocks are < next_block)

    // Note: if this object was rewritten to consolidate adjacent objects, the
    // blocks in this object may have been originally written over a range of
    // TXG's.
    pub min_txg: Txg,
    pub max_txg: Txg, // inclusive
}

#[derive(Serialize, Deserialize, Debug)]
struct DataObjectPhys<'a> {
    header: DataObjectHeader,
    #[serde(borrow)]
    blocks: Vec<(u64, &'a serde_bytes::Bytes)>,
}

#[derive(Debug)]
pub struct DataObject {
    pub header: DataObjectHeader,
    pub blocks: HashMap<BlockId, Bytes>,
}

impl DataObject {
    pub fn key(guid: PoolGuid, object: ObjectId) -> String {
        format!(
            "zfs/{}/data/{:03}/{}",
            guid,
            object.0 % NUM_DATA_PREFIXES,
            object
        )
    }

    // Could change this to return an Iterator
    pub fn prefixes(guid: PoolGuid) -> Vec<String> {
        let mut vec = Vec::new();
        for x in 0..NUM_DATA_PREFIXES {
            vec.push(format!("zfs/{}/data/{:03}/", guid, x));
        }
        vec
    }

    pub fn new(guid: PoolGuid, object: ObjectId, next_block: BlockId, txg: Txg) -> Self {
        DataObject {
            header: DataObjectHeader {
                guid,
                object,
                min_block: next_block,
                next_block,
                min_txg: txg,
                max_txg: txg,
                blocks_size: 0,
            },
            blocks: Default::default(),
        }
    }

    async fn get_impl<F: FnOnce() -> String>(
        object_access: &ObjectAccess,
        key: String,
        stat_type: ObjectAccessStatType,
        bypass_cache: bool,
        context: F,
    ) -> Result<Self> {
        let bytes = match bypass_cache {
            true => object_access.get_object_uncached(key, stat_type).await?,
            false => object_access.get_object(key, stat_type).await?,
        };
        let begin = Instant::now();
        let borrowed: DataObjectPhys = bincode::deserialize(&bytes).with_context(context)?;
        let data_object = DataObject {
            header: borrowed.header,
            blocks: borrowed
                .blocks
                .into_iter()
                .map(|(block, slice)| (BlockId(block), bytes.slice_ref(slice)))
                .collect(),
        };

        trace!(
            "{:?}: deserialized {} blocks from {} bytes in {}us",
            borrowed.header.object,
            data_object.blocks.len(),
            bytes.len(),
            begin.elapsed().as_micros()
        );

        data_object.verify();
        Ok(data_object)
    }

    pub async fn get_from_key(
        object_access: &ObjectAccess,
        key: String,
        stat_type: ObjectAccessStatType,
        bypass_cache: bool,
    ) -> Result<Self> {
        Self::get_impl(object_access, key.clone(), stat_type, bypass_cache, || {
            format!("Failed to decode contents of {}", key)
        })
        .await
    }

    pub async fn get(
        object_access: &ObjectAccess,
        guid: PoolGuid,
        object: ObjectId,
        stat_type: ObjectAccessStatType,
        bypass_cache: bool,
    ) -> Result<Self> {
        // We use get_impl() rather than get_from_key() to avoid allocating and
        // copying an additional String for the key in the common case.
        Self::get_impl(
            object_access,
            Self::key(guid, object),
            stat_type,
            bypass_cache,
            || format!("Failed to decode contents of {}", Self::key(guid, object)),
        )
        .await
    }

    pub async fn put(&self, object_access: &ObjectAccess, stat_type: ObjectAccessStatType) {
        let begin = Instant::now();

        let blocks = self
            .blocks
            .iter()
            .map(|(block, bytes)| (block.0, serde_bytes::Bytes::new(bytes)))
            .collect::<Vec<_>>();

        let borrowed = DataObjectPhys {
            header: self.header,
            blocks,
        };

        let contents = bincode::serialize(&borrowed).unwrap();
        trace!(
            "{:?}: serialized {} blocks in {} bytes in {}ms",
            self.header.object,
            self.blocks.len(),
            contents.len(),
            begin.elapsed().as_millis()
        );
        self.verify();
        object_access
            .put_object(
                Self::key(self.header.guid, self.header.object),
                contents.into(),
                stat_type,
            )
            .await;
    }

    pub fn calculate_blocks_size(&self) -> u32 {
        self.blocks
            .values()
            .map(|block| block.len())
            .sum::<usize>()
            .try_into()
            .unwrap()
    }

    fn verify(&self) {
        assert_eq!(self.header.blocks_size, self.calculate_blocks_size());
        assert_le!(self.header.min_txg, self.header.max_txg);
        assert_le!(self.header.min_block, self.header.next_block);
        if !self.blocks.is_empty() {
            assert_le!(self.header.min_block, self.blocks.keys().min().unwrap());
            assert_gt!(self.header.next_block, self.blocks.keys().max().unwrap());
        }
    }

    pub fn get_block(&self, block: BlockId) -> Bytes {
        self.blocks.get(&block).unwrap().clone()
    }

    pub fn blocks_len(&self) -> u32 {
        u32::try_from(self.blocks.len()).unwrap()
    }

    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }
}

impl Display for DataObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?}: blocks={} bytes={} BlockId[{},{}) TXG[{},{}]",
            self.header.object,
            self.blocks.len(),
            self.header.blocks_size,
            self.header.min_block,
            self.header.next_block,
            self.header.min_txg.0,
            self.header.max_txg.0,
        )
    }
}
