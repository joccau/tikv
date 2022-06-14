// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashSet;

use futures::executor::block_on;
use grpcio::RpcContext;
use kvproto::{logbackuppb::*, metapb::Region};
use tikv_util::{warn, worker::Scheduler};

use crate::{
    checkpoint_manager::{GetCheckpointResult, VersionedRegionId},
    endpoint::{RegionCheckpointOperation, RegionSet},
    try_send, Task,
};

#[derive(Clone)]
pub struct Service {
    endpoint: Scheduler<Task>,
}

impl Service {
    pub fn new(endpoint: Scheduler<Task>) -> Self {
        Self { endpoint }
    }
}

fn id_of(region: &Region) -> RegionIdentity {
    let mut id = RegionIdentity::new();
    id.set_id(region.get_id());
    id.set_epoch_version(region.get_region_epoch().get_version());
    id
}

impl Into<RegionIdentity> for VersionedRegionId {
    fn into(self) -> RegionIdentity {
        let mut id = RegionIdentity::new();
        id.set_id(self.region_id);
        id.set_epoch_version(self.region_epoch_version);
        id
    }
}

impl LogBackup for Service {
    fn get_last_flush_ts_of_region(
        &mut self,
        _ctx: RpcContext<'_>,
        mut req: GetLastFlushTsOfRegionRequest,
        sink: grpcio::UnarySink<GetLastFlushTsOfRegionResponse>,
    ) {
        let regions = req
            .take_regions()
            .into_iter()
            .map(|id| (id.id, id.epoch_version))
            .collect::<HashSet<_>>();
        let t = Task::RegionCheckpointsOp(RegionCheckpointOperation::Get(
            RegionSet::Regions(regions),
            Box::new(move |rs| {
                let mut resp = GetLastFlushTsOfRegionResponse::new();
                resp.set_checkpoints(
                    rs.into_iter()
                        .map(|r| match r {
                            GetCheckpointResult::Ok { region, checkpoint } => {
                                let mut r = RegionCheckpoint::new();
                                let id = id_of(&region);
                                r.set_region(id);
                                r.set_checkpoint(checkpoint.into_inner());
                                r
                            }
                            GetCheckpointResult::NotFound { id, err } => {
                                let mut r = RegionCheckpoint::new();
                                r.set_region(id.into());
                                r.set_err(err);
                                r
                            }
                            GetCheckpointResult::EpochNotMatch { region, err } => {
                                let mut r = RegionCheckpoint::new();
                                r.set_region(id_of(&region));
                                r.set_err(err);
                                r
                            }
                        })
                        .collect(),
                );
                let r = block_on(sink.success(resp));
                if let Err(e) = r {
                    warn!("failed to reply grpc resonse."; "err" => %e)
                }
            }),
        ));
        try_send!(self.endpoint, t);
    }
}
