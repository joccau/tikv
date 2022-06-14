// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;

use kvproto::{errorpb::*, metapb::Region};
use txn_types::TimeStamp;

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
        err: Error,
    },
    EpochNotMatch {
        region: Region,
        err: Error,
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

fn not_leader(r: u64) -> Error {
    let mut err = Error::new();
    let mut nl = NotLeader::new();
    nl.set_region_id(r);
    err.set_not_leader(nl);
    err.set_message(
        format!("the region {} isn't in the region_manager of log backup, maybe not leader or not flushed yet.", r));
    err
}

fn epoch_not_match(id: u64, sent: u64, real: u64) -> Error {
    let mut err = Error::new();
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

impl VersionedRegionId {
    fn from_region(region: &Region) -> Self {
        Self {
            region_id: region.get_id(),
            region_epoch_version: region.get_region_epoch().get_version(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LastFlushTsOfRegion {
    pub region: Region,
    pub checkpoint: TimeStamp,
}
