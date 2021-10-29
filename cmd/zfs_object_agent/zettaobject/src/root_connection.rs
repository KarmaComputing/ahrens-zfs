use crate::base_types::*;
use crate::features::FeatureError;
use crate::object_access::{ObjectAccess, StatMapValue};
use crate::pool::*;
use crate::pool_destroy;
use crate::server::handler_return_ok;
use crate::server::HandlerReturn;
use crate::server::SerialHandlerReturn;
use crate::server::Server;
use anyhow::anyhow;
use anyhow::Result;
use cstr_argument::CStrArgument;
use lazy_static::lazy_static;
use log::*;
use nvpair::{NvData, NvList, NvListRef};
use std::sync::Arc;
use util::get_tunable;
use util::maybe_die_with;
use util::AlignedBytes;
use uuid::Uuid;
use zettacache::base_types::*;
use zettacache::ZettaCache;

lazy_static! {
    pub static ref DIE_BEFORE_END_TXG_RESPONSE_PCT: f64 =
        get_tunable("die_before_end_txg_response_pct", 0.0);
}

pub struct RootServerState {
    cache: Option<ZettaCache>,
    id: Uuid,
}

#[derive(Default)]
struct RootConnectionState {
    pool: Option<Arc<Pool>>,
    cache: Option<ZettaCache>,
    id: Uuid,
}

impl RootServerState {
    fn connection_handler(&self) -> RootConnectionState {
        RootConnectionState {
            cache: self.cache.as_ref().cloned(),
            id: self.id,
            ..Default::default()
        }
    }

    pub fn start(socket_dir: &str, cache: Option<ZettaCache>) {
        let socket_path = format!("{}/zfs_root_socket", socket_dir);
        let mut server = Server::new(
            &socket_path,
            0o600,
            RootServerState {
                cache,
                id: Uuid::new_v4(),
            },
            Box::new(Self::connection_handler),
        );

        RootConnectionState::register(&mut server);
        server.start();
    }
}

impl RootConnectionState {
    fn register(server: &mut Server<RootServerState, RootConnectionState>) {
        server.register_serial_handler("create pool", Box::new(Self::create_pool));
        server.register_serial_handler("open pool", Box::new(Self::open_pool));
        server.register_serial_handler("resume complete", Box::new(Self::resume_complete));
        server.register_handler("begin txg", Box::new(Self::begin_txg));
        server.register_handler("flush writes", Box::new(Self::flush_writes));
        server.register_handler("end txg", Box::new(Self::end_txg));
        server.register_handler("write block", Box::new(Self::write_block));
        server.register_handler("free blocks", Box::new(Self::free_blocks));
        server.register_handler("read block", Box::new(Self::read_block));
        server.register_handler("get stats", Box::new(Self::get_stats));
        server.register_handler("close pool", Box::new(Self::close_pool));
        server.register_handler("exit agent", Box::new(Self::exit_agent));
        server.register_handler("enable feature", Box::new(Self::enable_feature));
        server.register_handler("resume destroy pool", Box::new(Self::resume_destroy_pool));
    }

    fn get_object_access(nvl: &NvListRef) -> Result<Arc<ObjectAccess>> {
        let bucket_name = nvl.lookup_string("bucket")?;
        let region_str = nvl.lookup_string("region")?;
        let endpoint = nvl.lookup_string("endpoint")?;
        let readonly = nvl.exists("readonly");
        let credentials_profile: Option<String> = nvl
            .lookup_string("credentials_profile")
            .ok()
            .map(|s| s.to_string_lossy().to_string());
        Ok(ObjectAccess::new(
            endpoint.to_str().unwrap(),
            region_str.to_str().unwrap(),
            bucket_name.to_str().unwrap(),
            credentials_profile,
            readonly,
        ))
    }

    fn create_pool(&mut self, nvl: NvList) -> SerialHandlerReturn {
        info!("got request: {:?}", nvl);
        Box::pin(async move {
            let guid = PoolGuid(nvl.lookup_uint64("GUID")?);
            let name = nvl.lookup_string("name")?;
            let object_access = Self::get_object_access(&nvl)?;

            Pool::create(&object_access, name.to_str()?, guid).await;
            let mut response = NvList::new_unique_names();
            response.insert("Type", "pool create done").unwrap();
            response.insert("GUID", &guid.0).unwrap();

            maybe_die_with(|| format!("before sending response: {:?}", response));
            debug!("sending response: {:?}", response);
            Ok(Some(response))
        })
    }

    fn open_pool(&mut self, nvl: NvList) -> SerialHandlerReturn {
        info!("got request: {:?}", nvl);
        Box::pin(async move {
            let guid = PoolGuid(nvl.lookup_uint64("GUID")?);
            let rollback = bool_value(&nvl, "rollback")?;

            let object_access = Self::get_object_access(&nvl)?;
            let cache = self.cache.as_ref().cloned();
            let txg = nvl.lookup_uint64("TXG").ok().map(Txg);
            let syncing_txg = nvl.lookup_uint64("syncing_txg").ok().map(Txg);
            let mut response = NvList::new_unique_names();

            let (pool, phys_opt, next_block) = match Pool::open(
                object_access,
                guid,
                txg,
                cache,
                self.id,
                syncing_txg,
                rollback,
            )
            .await
            {
                Err(PoolOpenError::Mmp(hostname)) => {
                    response.insert("Type", "pool open failed").unwrap();
                    response.insert("cause", "MMP").unwrap();
                    response.insert("hostname", hostname.as_str()).unwrap();
                    debug!("sending response: {:?}", response);
                    return Ok(Some(response));
                }
                Err(PoolOpenError::Feature(FeatureError { features, readonly })) => {
                    response.insert("Type", "pool open failed").unwrap();
                    response.insert("cause", "feature").unwrap();
                    let mut feature_nvl = NvList::new_unique_names();
                    for feature in features {
                        feature_nvl.insert(feature.name, "").unwrap();
                    }
                    response.insert("features", feature_nvl.as_ref()).unwrap();
                    response.insert("can_readonly", &readonly).unwrap();
                    debug!("sending response: {:?}", response);
                    return Ok(Some(response));
                }
                Err(PoolOpenError::Get(e)) => {
                    /*
                     * It would be really nice to bring up the exact error type from the
                     * object_access layer here and case on it properly. Unfortunately,
                     * attempting to do so is best described as... fraught. Errors come from a
                     * number of sources, and are implicitly converted frequently. Ultimately,
                     * the blocker is that most of the error types produced by our dependencies
                     * do not implement Clone, and so cannot be easily propogated up the chain
                     * when multiple people may be fetching the same object.
                     *
                     * If we ever decide to implement our own error types instead of using the
                     * underlying ones, we could handle this situation more cleanly. Until
                     * then, we just pass the root cause error message back to the kernel, and
                     * hope that it can present a usable error to the user.
                     */
                    response.insert("Type", "pool open failed").unwrap();
                    response.insert("cause", "IO").unwrap();
                    response
                        .insert("message", e.root_cause().to_string().as_str())
                        .unwrap();
                    debug!("sending response: {:?}", response);
                    return Ok(Some(response));
                }
                Err(PoolOpenError::NoCheckpoint) => {
                    response.insert("Type", "pool open failed").unwrap();
                    response.insert("cause", "checkpoint").unwrap();
                    debug!("sending response: {:?}", response);
                    return Ok(Some(response));
                }
                Ok(x) => x,
            };

            response.insert("Type", "pool open done").unwrap();
            response.insert("GUID", &guid.0).unwrap();
            if let Some(phys) = phys_opt {
                response
                    .insert("uberblock", &phys.get_zfs_uberblock()[..])
                    .unwrap();
                response
                    .insert("config", &phys.get_zfs_config()[..])
                    .unwrap();
                let mut feature_nvl = NvList::new_unique_names();
                for (feature, refcount) in phys.features() {
                    feature_nvl.insert(&feature.name, refcount).unwrap();
                }
                response.insert("features", feature_nvl.as_ref()).unwrap();
            }

            response.insert("next_block", &next_block.0).unwrap();

            self.pool = Some(Arc::new(pool));
            maybe_die_with(|| format!("before sending response: {:?}", response));
            debug!("sending response: {:?}", response);
            Ok(Some(response))
        })
    }

    fn begin_txg(&mut self, nvl: NvList) -> HandlerReturn {
        debug!("got request: {:?}", nvl);
        let txg = Txg(nvl.lookup_uint64("TXG")?);
        let pool = self.pool.as_ref().ok_or_else(|| anyhow!("no pool open"))?;
        pool.begin_txg(txg);

        handler_return_ok(None)
    }

    fn resume_complete(&mut self, nvl: NvList) -> SerialHandlerReturn {
        info!("got request: {:?}", nvl);

        // This is .await'ed by the server's thread, so we can't see any new writes
        // come in while it's in progress.
        Box::pin(async move {
            let pool = self.pool.as_ref().ok_or_else(|| anyhow!("no pool open"))?;
            pool.resume_complete().await;
            Ok(None)
        })
    }

    fn flush_writes(&mut self, nvl: NvList) -> HandlerReturn {
        debug!("got request: {:?}", nvl);
        let pool = self.pool.as_ref().ok_or_else(|| anyhow!("no pool open"))?;
        let block = BlockId(nvl.lookup_uint64("block")?);
        pool.initiate_flush(block);
        handler_return_ok(None)
    }

    // sends response when completed
    async fn end_txg_impl(
        pool: Arc<Pool>,
        uberblock: Vec<u8>,
        config: Vec<u8>,
        checkpoint_txg: Option<Txg>,
    ) -> Result<Option<NvList>> {
        let (stats, features) = pool.end_txg(uberblock, config, checkpoint_txg).await;
        let mut feature_nvl = NvList::new_unique_names();
        for (feature, refcount) in features {
            feature_nvl.insert(feature.name, &refcount).unwrap();
        }
        let mut response = NvList::new_unique_names();
        response.insert("Type", "end txg done").unwrap();
        response
            .insert("blocks_count", &stats.blocks_count)
            .unwrap();
        response
            .insert("blocks_bytes", &stats.blocks_bytes)
            .unwrap();
        response
            .insert("pending_frees_count", &stats.pending_frees_count)
            .unwrap();
        response
            .insert("pending_frees_bytes", &stats.pending_frees_bytes)
            .unwrap();
        response
            .insert("objects_count", &stats.objects_count)
            .unwrap();
        response.insert("features", feature_nvl.as_ref()).unwrap();
        maybe_die_with(|| format!("before sending response: {:?}", response));
        debug!("sending response: {:?}", response);
        Ok(Some(response))
    }

    fn end_txg(&mut self, nvl: NvList) -> HandlerReturn {
        debug!("got request: {:?}", nvl);

        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| anyhow!("no pool open"))?
            .clone();
        Ok(Box::pin(async move {
            let uberblock = u8_array_value(&nvl, "uberblock")?.to_vec();
            let config = u8_array_value(&nvl, "config")?.to_vec();
            let checkpoint_txg = match nvl.lookup_uint64("checkpoint").ok() {
                Some(0) | None => None,
                Some(t) => Some(Txg(t)),
            };

            Self::end_txg_impl(pool, uberblock, config, checkpoint_txg).await
        }))
    }

    /// queue write, sends response when completed (persistent).
    /// completion may not happen until flush_pool() is called
    fn write_block(&mut self, nvl: NvList) -> HandlerReturn {
        let block = BlockId(nvl.lookup_uint64("block")?);
        let slice = u8_array_value(&nvl, "data")?;
        let request_id = nvl.lookup_uint64("request_id")?;
        let token = nvl.lookup_uint64("token")?;
        trace!(
            "got write request id={}: {:?} len={}",
            request_id,
            block,
            slice.len()
        );

        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| anyhow!("no pool open"))?
            .clone();
        let alignment = match self.cache.as_ref() {
            Some(cache) => cache.sector_size(),
            None => 1,
        };
        // XXX copying data
        let bytes = AlignedBytes::copy_from_slice(slice, alignment);
        Ok(Box::pin(async move {
            pool.write_block(block, bytes).await;
            let mut response = NvList::new_unique_names();
            response.insert("Type", "write done").unwrap();
            response.insert("block", &block.0).unwrap();
            response.insert("request_id", &request_id).unwrap();
            response.insert("token", &token).unwrap();
            trace!("sending response: {:?}", response);
            Ok(Some(response))
        }))
    }

    fn free_blocks(&mut self, nvl: NvList) -> HandlerReturn {
        trace!("got request: {:?}", nvl);
        let blocks = u64_array_value(&nvl, "block")?;
        let sizes = u32_array_value(&nvl, "size")?;

        let pool = self.pool.as_ref().ok_or_else(|| anyhow!("no pool open"))?;
        pool.free_blocks(blocks, sizes);
        maybe_die_with(|| "after free block request".to_string());
        handler_return_ok(None)
    }

    fn read_block(&mut self, nvl: NvList) -> HandlerReturn {
        trace!("got request: {:?}", nvl);
        let block = BlockId(nvl.lookup_uint64("block")?);
        let request_id = nvl.lookup_uint64("request_id")?;
        let token = nvl.lookup_uint64("token")?;
        let heal = bool_value(&nvl, "heal")?;

        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| anyhow!("no pool open"))?
            .clone();
        Ok(Box::pin(async move {
            let data = pool.read_block(block, heal).await;
            let mut nvl = NvList::new_unique_names();
            nvl.insert("Type", "read done").unwrap();
            nvl.insert("block", &block.0).unwrap();
            nvl.insert("request_id", &request_id).unwrap();
            nvl.insert("token", &token).unwrap();
            nvl.insert("data", data.as_ref()).unwrap();
            trace!(
                "sending read done response: block={} req={} data=[{} bytes]",
                block,
                request_id,
                data.len()
            );
            Ok(Some(nvl))
        }))
    }

    fn get_stats(&mut self, nvl: NvList) -> HandlerReturn {
        trace!("got request: {:?}", nvl);
        let token = nvl.lookup_uint64("token")?;

        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| anyhow!("no pool open"))?
            .clone();

        //
        // Build an nvlist from the stats hash map
        // Each map entry can be a Counter, a CounterMap, or a Histogram
        //
        let stats = pool.state.shared_state.object_access.collect_stats();
        let mut nvl = NvList::new_unique_names();
        for (name, stat_value) in stats.iter() {
            match stat_value {
                StatMapValue::Counter(timestamp) => nvl.insert(name, timestamp).unwrap(),
                StatMapValue::CounterMap(cm) => {
                    let mut contents = NvList::new_unique_names();
                    for (key, value) in cm.iter() {
                        // e.g. "operations": 459
                        contents.insert(key, value).unwrap();
                    }
                    // e.g. "MetadataPut": {"operations": 459, "total_bytes": 350280, "active": 2}
                    nvl.insert(name, contents.as_ref()).unwrap();
                }
                StatMapValue::Histogram(histogram) => nvl.insert(name, &histogram[..]).unwrap(),
            }
        }

        let mut response = NvList::new_unique_names();
        response.insert("Type", "get stats done").unwrap();
        response.insert("token", &token).unwrap();
        response.insert("stats", nvl.as_ref()).unwrap();

        trace!("sending stats done response: {:?}", response);
        handler_return_ok(Some(response))
    }

    fn close_pool(&mut self, nvl: NvList) -> HandlerReturn {
        info!("got request: {:?}", nvl);
        let destroy = match nvl.lookup("destroy").unwrap().data() {
            NvData::BoolV(destroy) => destroy,
            _ => panic!("destroy not expected type"),
        };

        let pool_opt = self.pool.take();
        Ok(Box::pin(async move {
            if let Some(pool) = pool_opt {
                Arc::try_unwrap(pool)
                    .map_err(|_| {
                        anyhow!("pool close request while there are other operations in progress")
                    })?
                    .close(destroy)
                    .await;
            }
            let mut response = NvList::new_unique_names();
            response.insert("Type", "pool close done").unwrap();
            maybe_die_with(|| format!("before sending response: {:?}", response));
            debug!("sending response: {:?}", response);
            Ok(Some(response))
        }))
    }

    // XXX This doesn't actually exit the agent, it just closes the connection,
    // which the kernel could do from its end.  It's unclear what the kernel
    // really wants.
    fn exit_agent(&mut self, nvl: NvList) -> HandlerReturn {
        info!("got request: {:?}", nvl);
        Err(anyhow!("exit requested"))
    }

    fn enable_feature(&mut self, nvl: NvList) -> HandlerReturn {
        debug!("got request: {:?}", nvl);
        let pool = self
            .pool
            .as_ref()
            .expect("Attempted to set feature with no pool")
            .clone();
        let feature_name = nvl.lookup_string("feature").unwrap().into_string().unwrap();
        pool.enable_feature(&feature_name);
        let mut response = NvList::new_unique_names();
        response.insert("Type", "enable feature done").unwrap();
        response.insert("feature", feature_name.as_str()).unwrap();
        debug!("sending response: {:?}", response);
        handler_return_ok(Some(response))
    }

    fn resume_destroy_pool(&mut self, nvl: NvList) -> HandlerReturn {
        Ok(Box::pin(async move {
            debug!("got request: {:?}", nvl);

            let guid = PoolGuid(nvl.lookup_uint64("GUID").unwrap());
            let object_access = RootConnectionState::get_object_access(&nvl).unwrap();

            let mut response = NvList::new_unique_names();

            match pool_destroy::resume_destroy(object_access, guid).await {
                Ok(_) => {
                    response.insert("Type", "resume destroy pool done").unwrap();
                }
                Err(error) => {
                    error!("resume destroy pool failed, {:?}", error);
                    response
                        .insert("Type", "resume destroy pool failed")
                        .unwrap();
                }
            };

            debug!("sending response: {:?}", response);
            Ok(Some(response))
        }))
    }
}

/// Get the BoolV type value, or if not present then default to false.
/// Return Err if value is present but not BoolV type.
fn bool_value<S>(nvl: &NvListRef, name: S) -> Result<bool>
where
    S: CStrArgument,
{
    match nvl.lookup(name) {
        Ok(pair) => {
            if let NvData::BoolV(resume) = pair.data() {
                Ok(resume)
            } else {
                Err(anyhow!("pair {:?} not expected type (boolean_value)", pair))
            }
        }
        Err(_) => Ok(false),
    }
}

/// Get the uint8_array type value.
/// Return Err if value is not present, or not the expected type.
fn u8_array_value<S>(nvl: &NvListRef, name: S) -> Result<&[u8]>
where
    S: CStrArgument,
{
    let pair = nvl.lookup(name)?;
    if let NvData::Uint8Array(slice) = pair.data() {
        Ok(slice)
    } else {
        Err(anyhow!("pair {:?} not expected type (uint8_array)", pair))
    }
}

/// Get the uint32_array type value.
/// Return Err if value is not present, or not the expected type.
fn u32_array_value<S>(nvl: &NvListRef, name: S) -> Result<&[u32]>
where
    S: CStrArgument,
{
    let pair = nvl.lookup(name)?;
    if let NvData::Uint32Array(slice) = pair.data() {
        Ok(slice)
    } else {
        Err(anyhow!("pair {:?} not expected type (uint32_array)", pair))
    }
}

/// Get the uint64_array type value.
/// Return Err if value is not present, or not the expected type.
fn u64_array_value<S>(nvl: &NvListRef, name: S) -> Result<&[u64]>
where
    S: CStrArgument,
{
    let pair = nvl.lookup(name)?;
    if let NvData::Uint64Array(slice) = pair.data() {
        Ok(slice)
    } else {
        Err(anyhow!("pair {:?} not expected type (uint64_array)", pair))
    }
}
