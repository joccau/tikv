// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, sync::Arc, time::Duration};

use kvproto::{
    errorpb::{Error as PbError, *},
    metapb::Region,
};
use pd_client::PdClient;
use tikv_util::{info, worker::Scheduler};
use txn_types::TimeStamp;

use crate::{
    errors::{ContextualResultExt, Error, Result},
    metadata::{store::MetaStore, CheckpointProvider, MetadataClient},
    metrics,
    subscription_track::SubscriptionTracer,
    try_send, RegionCheckpointOperation, Task,
};

/// A manager for mataining the last flush ts.
#[derive(Debug, Default)]
pub struct CheckpointManager {
    items: HashMap<u64, LastFlushTsOfRegion>,
}

#[derive(Debug)]
pub enum GetCheckpointResult {
    Ok {
        region: Region,
        checkpoint: TimeStamp,
    },
    NotFound {
        id: VersionedRegionId,
        err: PbError,
    },
    EpochNotMatch {
        region: Region,
        err: PbError,
    },
}

impl GetCheckpointResult {
    /// create an "ok" variant with region.
    pub fn ok(region: Region, checkpoint: TimeStamp) -> Self {
        Self::Ok { region, checkpoint }
    }

    fn not_found(id: VersionedRegionId) -> Self {
        Self::NotFound {
            id,
            err: not_leader(id.region_id),
        }
    }

    /// create a epoch not match variant with region
    fn epoch_not_match(provided: VersionedRegionId, real: &Region) -> Self {
        Self::EpochNotMatch {
            region: real.clone(),
            err: epoch_not_match(
                provided.region_id,
                provided.region_epoch_version,
                real.get_region_epoch().get_version(),
            ),
        }
    }
}

impl CheckpointManager {
    /// clear the manager.
    pub fn clear(&mut self) {
        self.items.clear();
    }

    /// upadte a region checkpoint in need.
    pub fn update_region_checkpoint(&mut self, region: &Region, checkpoint: TimeStamp) {
        let e = self.items.entry(region.get_id());
        let old_cp = e.or_insert_with(|| LastFlushTsOfRegion {
            checkpoint,
            region: region.clone(),
        });
        if old_cp.checkpoint < checkpoint
            || old_cp.region.get_region_epoch().get_version()
                < region.get_region_epoch().get_version()
        {
            *old_cp = LastFlushTsOfRegion {
                checkpoint,
                region: region.clone(),
            };
        }
    }

    /// get checkpoint from a region.
    pub fn get_from_region(&self, region: VersionedRegionId) -> GetCheckpointResult {
        let checkpoint = self.items.get(&region.region_id);
        if checkpoint.is_none() {
            return GetCheckpointResult::not_found(region);
        }
        let checkpoint = checkpoint.unwrap();
        if checkpoint.region.get_region_epoch().get_version() != region.region_epoch_version {
            return GetCheckpointResult::epoch_not_match(region, &checkpoint.region);
        }
        GetCheckpointResult::ok(checkpoint.region.clone(), checkpoint.checkpoint)
    }

    /// get all checkpoints stored.
    pub fn get_all(&self) -> Vec<LastFlushTsOfRegion> {
        self.items.values().map(|v| v.clone()).collect()
    }
}

fn not_leader(r: u64) -> PbError {
    let mut err = PbError::new();
    let mut nl = NotLeader::new();
    nl.set_region_id(r);
    err.set_not_leader(nl);
    err.set_message(
        format!("the region {} isn't in the region_manager of log backup, maybe not leader or not flushed yet.", r));
    err
}

fn epoch_not_match(id: u64, sent: u64, real: u64) -> PbError {
    let mut err = PbError::new();
    let en = EpochNotMatch::new();
    err.set_epoch_not_match(en);
    err.set_message(format!(
        "the region {} has recorded version {}, but you sent {}",
        id, real, sent,
    ));
    err
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
/// A simple region id, but versioned.
pub struct VersionedRegionId {
    pub region_id: u64,
    pub region_epoch_version: u64,
}

#[derive(Debug, Clone)]
pub struct LastFlushTsOfRegion {
    pub region: Region,
    pub checkpoint: TimeStamp,
}

// Allow some type to
#[async_trait::async_trait]
pub trait FlushObserver: Send + 'static {
    /// The callback when the flush has advanced the resolver.
    async fn before(&mut self, checkpoints: Vec<(Region, TimeStamp)>);
    /// The callback when the flush is done. (Files are fully written to external storage.)
    async fn after(&mut self, task: &str, rts: u64) -> Result<()>;
    /// The optional callback to rewrite the resolved ts of this flush.
    /// Because the default method (collect all leader resolved ts in the store, and use the minimal TS.)
    /// may lead to resolved ts rolling back, if we desire a stronger consistency, we can rewrite a safer resolved ts here.
    /// Note the new resolved ts cannot be greater than the old resolved ts.
    async fn rewrite_resolved_ts(
        &mut self,
        #[allow(unused_variables)] task: &str,
    ) -> Option<TimeStamp> {
        None
    }
}

pub struct BasicFlushObserver<PD, S> {
    pd_cli: Arc<PD>,
    meta_cli: MetadataClient<S>,
    store_id: u64,
}

impl<PD, S> BasicFlushObserver<PD, S> {
    pub fn new(meta_cli: MetadataClient<S>, pd_cli: Arc<PD>, store_id: u64) -> Self {
        Self {
            pd_cli,
            meta_cli,
            store_id,
        }
    }
}

#[async_trait::async_trait]
impl<PD: PdClient + 'static, S: MetaStore + 'static> FlushObserver for BasicFlushObserver<PD, S> {
    async fn before(&mut self, _checkpoints: Vec<(Region, TimeStamp)>) {}

    async fn after(&mut self, task: &str, rts: u64) -> Result<()> {
        if let Err(err) = self
            .pd_cli
            .update_service_safe_point(
                format!("backup-stream-{}-{}", task, self.store_id),
                TimeStamp::new(rts),
                // Add a service safe point for 30 mins (6x the default flush interval).
                // It would probably be safe.
                Duration::from_secs(1800),
            )
            .await
        {
            Error::from(err).report("failed to update service safe point!");
            // don't give up?
        }

        // we can advance the progress at next time.
        // return early so we won't be mislead by the metrics.
        self.meta_cli
            .set_local_task_checkpoint(&task, rts)
            .await
            .context(format_args!("on flushing task {}", task))?;

        // Currently, we only support one task at the same time,
        // so use the task as label would be ok.
        metrics::STORE_CHECKPOINT_TS
            .with_label_values(&[task])
            .set(rts as _);
        Ok(())
    }
}

pub struct CheckpointV2FlushObserver<S, F, O> {
    resolvers: SubscriptionTracer,
    meta_cli: MetadataClient<S>,

    fresh_regions: Vec<Region>,
    checkpoints: Vec<(Region, TimeStamp)>,
    can_advance: Option<F>,
    base: O,
}

impl<S, F, O> CheckpointV2FlushObserver<S, F, O> {
    pub fn new(
        meta_cli: MetadataClient<S>,
        can_advance: F,
        resolvers: SubscriptionTracer,
        base: O,
    ) -> Self {
        Self {
            resolvers,
            meta_cli,
            fresh_regions: vec![],
            checkpoints: vec![],
            can_advance: Some(can_advance),
            base,
        }
    }
}

#[async_trait::async_trait]
impl<S, F, O> FlushObserver for CheckpointV2FlushObserver<S, F, O>
where
    S: MetaStore + 'static,
    F: FnOnce() -> bool + Send + 'static,
    O: FlushObserver,
{
    async fn before(&mut self, _checkpoints: Vec<(Region, TimeStamp)>) {
        let fresh_regions = self.resolvers.collect_fresh_subs();
        let removal = self.resolvers.collect_removal_subs();
        let checkpoints = removal
            .into_iter()
            .map(|sub| (sub.meta, sub.resolver.resolved_ts()))
            .collect::<Vec<_>>();
        self.checkpoints = checkpoints;
        self.fresh_regions = fresh_regions;
    }

    async fn after(&mut self, task: &str, rts: u64) -> Result<()> {
        if !self.can_advance.take().map(|f| f()).unwrap_or(true) {
            let cp_now = self
                .meta_cli
                .get_local_task_checkpoint(&task)
                .await
                .context(format_args!(
                    "during checking whether we should skip advencing ts to {}.",
                    rts
                ))?;
            // if we need to roll back checkpoint ts, don't prevent it.
            if rts >= cp_now.into_inner() {
                info!("skipping advance checkpoint."; "rts" => %rts, "old_rts" => %cp_now);
                return Ok(());
            }
        }
        self.base.after(task, rts).await?;
        // Optionally upload the region checkpoint.
        // Unless in some exteme condition, skipping upload the region checkpoint won't lead to data loss.
        if let Err(err) = self
            .meta_cli
            .upload_region_checkpoint(&task, &self.checkpoints)
            .await
        {
            err.report("failed to upload region checkpoint");
        }
        self.meta_cli
            .clear_region_checkpoint(&task, &self.fresh_regions)
            .await
            .context(format_args!("on clearing the checkpoint for task {}", task))?;
        Ok(())
    }
}

pub struct CheckpointV3FlushObserver<S, O> {
    sched: Scheduler<Task>,
    meta_cli: MetadataClient<S>,
    checkpoints: Vec<(Region, TimeStamp)>,

    /// We should modify the rts (the local rts isn't right.)
    /// This should be a BasicFlushObserver or something likewise.
    baseline: O,
}

impl<S, O> CheckpointV3FlushObserver<S, O> {
    pub fn new(sched: Scheduler<Task>, meta_cli: MetadataClient<S>, baseline: O) -> Self {
        Self {
            sched,
            meta_cli,
            checkpoints: vec![],
            baseline,
        }
    }
}

#[async_trait::async_trait]
impl<S, O> FlushObserver for CheckpointV3FlushObserver<S, O>
where
    S: MetaStore + 'static,
    O: FlushObserver + Send,
{
    async fn before(&mut self, checkpoints: Vec<(Region, TimeStamp)>) {
        self.checkpoints = checkpoints;
    }

    async fn after(&mut self, task: &str, _rts: u64) -> Result<()> {
        let t = Task::RegionCheckpointsOp(RegionCheckpointOperation::Update(std::mem::take(
            &mut self.checkpoints,
        )));
        try_send!(self.sched, t);
        let global_checkpoint = self.meta_cli.global_checkpoint_of_task(task).await?;
        info!("getting global checkpoint for updating."; "checkpoint" => ?global_checkpoint);
        self.baseline
            .after(task, global_checkpoint.ts.into_inner())
            .await?;
        Ok(())
    }

    async fn rewrite_resolved_ts(&mut self, task: &str) -> Option<TimeStamp> {
        let global_checkpoint = self
            .meta_cli
            .global_checkpoint_of_task(task)
            .await
            .map_err(|err| err.report("failed to get resolved ts for rewritting"))
            .ok()?;
        info!("getting global checkpoint for updating."; "checkpoint" => ?global_checkpoint);
        matches!(global_checkpoint.provider, CheckpointProvider::Global)
            .then(|| global_checkpoint.ts)
    }
}
