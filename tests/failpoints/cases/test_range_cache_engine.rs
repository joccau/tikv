use std::{
    fs::File,
    io::Read,
    sync::{mpsc::sync_channel, Arc, Mutex},
    time::Duration,
};

use engine_rocks::RocksSstWriterBuilder;
use engine_traits::{
    CacheRegion, EvictReason, RangeCacheEngine, SstWriter, SstWriterBuilder, CF_DEFAULT,
};
use file_system::calc_crc32_bytes;
use keys::{data_key, DATA_MAX_KEY, DATA_MIN_KEY};
use kvproto::{
    import_sstpb::SstMeta,
    raft_cmdpb::{CmdType, RaftCmdRequest, RaftRequestHeader, Request},
};
use protobuf::Message;
use range_cache_memory_engine::test_util::new_region;
use tempfile::tempdir;
use test_coprocessor::{handle_request, init_data_with_details_pd_client, DagSelect, ProductTable};
use test_raftstore::{
    get_tso, new_peer, new_server_cluster_with_hybrid_engine_with_no_range_cache, Cluster,
    ServerCluster,
};
use tikv_util::{mpsc::unbounded, HandyRwLock};
use tipb::SelectResponse;
use txn_types::Key;

fn must_copr_point_get(cluster: &mut Cluster<ServerCluster>, table: &ProductTable, row_id: i64) {
    let key = table.get_table_prefix();
    let table_key = Key::from_raw(&key).into_encoded();
    let ctx = cluster.get_ctx(&table_key);
    let endpoint = cluster.sim.rl().copr_endpoints[&1].clone();
    let key_range = table.get_record_range_one(row_id);
    let req = DagSelect::from(table)
        .key_ranges(vec![key_range])
        .build_with(ctx, &[0]);
    let cop_resp = handle_request(&endpoint, req);
    let mut resp = SelectResponse::default();
    resp.merge_from_bytes(cop_resp.get_data()).unwrap();
    assert!(!cop_resp.has_region_error(), "{:?}", cop_resp);
    assert!(cop_resp.get_other_error().is_empty(), "{:?}", cop_resp);
}

fn must_copr_load_data(cluster: &mut Cluster<ServerCluster>, table: &ProductTable, row_id: i64) {
    let key = table.get_table_prefix();
    let table_key = Key::from_raw(&key).into_encoded();
    let ctx = cluster.get_ctx(&table_key);
    let engine = cluster.sim.rl().storages[&1].clone();
    let _ = init_data_with_details_pd_client(
        ctx.clone(),
        engine,
        table,
        &[(row_id, Some(&format!("name:{}", row_id)), row_id)],
        true,
        &cluster.cfg.tikv.server,
        Some(cluster.pd_client.clone()),
    );
}

#[test]
fn test_put_copr_get() {
    let mut cluster = new_server_cluster_with_hybrid_engine_with_no_range_cache(0, 1);
    cluster.cfg.raft_store.apply_batch_system.pool_size = 1;
    cluster.run();

    let current_ts = get_tso(&cluster.pd_client);
    let range_cache_engine = cluster.sim.rl().get_range_cache_engine(1);
    // FIXME: load is not implemented, so we have to insert range manually
    {
        let region = cluster.get_region(b"k");
        let rid = region.id;
        let mut core = range_cache_engine.core().write();
        core.mut_range_manager()
            .new_region(CacheRegion::from_region(&region));
        core.mut_range_manager().set_safe_point(rid, current_ts);
    }

    let product = ProductTable::new();
    must_copr_load_data(&mut cluster, &product, 1);
    let (tx, rx) = unbounded();
    fail::cfg_callback("on_range_cache_iterator_seek", move || {
        tx.send(true).unwrap();
    })
    .unwrap();

    must_copr_point_get(&mut cluster, &product, 1);

    // verify it's read from range cache engine
    rx.try_recv().unwrap();
}

#[test]
fn test_load() {
    let mut cluster = new_server_cluster_with_hybrid_engine_with_no_range_cache(0, 1);
    cluster.cfg.raft_store.apply_batch_system.pool_size = 2;
    cluster.run();

    let mut tables = vec![];
    for _ in 0..3 {
        let product = ProductTable::new();
        tables.push(product.clone());
        must_copr_load_data(&mut cluster, &product, 1);
    }

    let mut split_keys = vec![];
    for table in &tables[1..] {
        let key = table.get_table_prefix();
        let split_key = Key::from_raw(&key).into_encoded();
        let r = cluster.get_region(&split_key);
        cluster.must_split(&r, &split_key);
        split_keys.push(split_key);
    }

    let (tx, rx) = unbounded();
    fail::cfg_callback("on_snapshot_load_finished", move || {
        tx.send(true).unwrap();
    })
    .unwrap();

    // load range
    {
        let r = cluster.get_region(b"");
        let r1 = cluster.get_region(&split_keys[0]);
        let r2 = cluster.get_region(&split_keys[1]);
        let range_cache_engine = cluster.sim.rl().get_range_cache_engine(1);
        let mut core = range_cache_engine.core().write();
        core.mut_range_manager()
            .load_region(CacheRegion::from_region(&r))
            .unwrap();
        core.mut_range_manager()
            .load_region(CacheRegion::from_region(&r1))
            .unwrap();
        core.mut_range_manager()
            .load_region(CacheRegion::from_region(&r2))
            .unwrap();
    }

    // put key to trigger load task
    for table in &tables {
        must_copr_load_data(&mut cluster, table, 1);
    }

    // ensure the snapshot is loaded
    rx.recv_timeout(Duration::from_secs(5)).unwrap();
    rx.recv_timeout(Duration::from_secs(5)).unwrap();
    rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let (tx, rx) = unbounded();
    fail::cfg_callback("on_range_cache_iterator_seek", move || {
        tx.send(true).unwrap();
    })
    .unwrap();

    for table in &tables {
        must_copr_point_get(&mut cluster, table, 1);

        // verify it's read from range cache engine
        assert!(rx.try_recv().unwrap());
    }
}

// It tests that after we schedule the pending range to load snapshot, the range
// splits.
#[test]
fn test_load_with_split() {
    let mut cluster = new_server_cluster_with_hybrid_engine_with_no_range_cache(0, 1);
    cluster.cfg.raft_store.apply_batch_system.pool_size = 2;
    cluster.run();

    let mut tables = vec![];
    for _ in 0..3 {
        let product = ProductTable::new();
        tables.push(product.clone());
        must_copr_load_data(&mut cluster, &product, 1);
    }

    let (tx, rx) = sync_channel(0);
    // let channel to make load process block at finishing loading snapshot
    let (tx2, rx2) = sync_channel(0);
    let rx2 = Arc::new(Mutex::new(rx2));
    fail::cfg_callback("on_snapshot_load_finished", move || {
        tx.send(true).unwrap();
        let _ = rx2.lock().unwrap().recv().unwrap();
    })
    .unwrap();

    // load range
    {
        let range_cache_engine = cluster.sim.rl().get_range_cache_engine(1);
        let mut core = range_cache_engine.core().write();
        // Load the whole range as if it is not splitted. Loading process should handle
        // it correctly.
        let cache_range = new_region(1, "", "");
        core.mut_range_manager()
            .load_region(CacheRegion::from_region(&cache_range))
            .unwrap();
    }

    rx.recv_timeout(Duration::from_secs(5)).unwrap();
    // Now, the snapshot load is finished, and blocked before consuming cached
    // write batches. Let split the range.

    for table in &tables[1..] {
        let key = table.get_table_prefix();
        let split_key = Key::from_raw(&key).into_encoded();
        let r = cluster.get_region(&split_key);
        cluster.must_split(&r, &split_key);
    }
    // Now, we have 3 regions: [min, table1), [table1, table2), [table2, max)

    for table in &tables {
        must_copr_load_data(&mut cluster, table, 1);
    }

    // unblock loading task
    tx2.send(true).unwrap();

    let (tx, rx) = unbounded();
    fail::cfg_callback("on_range_cache_iterator_seek", move || {
        tx.send(true).unwrap();
    })
    .unwrap();

    for table in &tables {
        must_copr_point_get(&mut cluster, table, 1);

        // verify it's read from range cache engine
        assert!(rx.try_recv().unwrap());
    }
}

// It tests race between split and load.
// Takes table1-table2 as an example:
// We want to load table1-table2 where table1-table2 is already split into
// ["", table2), and [table2, "").
// And before we `load_region` table1-table2, ["", table2) has cached some
// writes, say table1_1, in write_batch which means table1_1 cannot be loaded
// from snapshot. Now, `load_region` table1-table2 is called, and [table2, "")
// calls prepare_for_apply and the snapshot is acquired and load task of
// table1-table2 is scheduled.
#[test]
fn test_load_with_split2() {
    let mut cluster = new_server_cluster_with_hybrid_engine_with_no_range_cache(0, 1);
    cluster.cfg.raft_store.apply_batch_system.pool_size = 4;
    cluster.run();
    let range_cache_engine = cluster.sim.rl().get_range_cache_engine(1);

    let product1 = ProductTable::new();
    let product2 = ProductTable::new();

    for table in [&product1, &product2] {
        must_copr_load_data(&mut cluster, table, 1);
    }

    let key = product2.get_table_prefix();
    let split_key = Key::from_raw(&key).into_encoded();
    let r = cluster.get_region(&split_key);
    cluster.must_split(&r, &split_key);
    let r_split = cluster.get_region(&split_key);

    let (handle_put_tx, handle_put_rx) = unbounded();
    let (handle_put_pause_tx, handle_put_pause_rx) = unbounded::<()>();
    fail::cfg_callback("on_handle_put", move || {
        handle_put_tx.send(()).unwrap();
        let _ = handle_put_pause_rx.recv();
    })
    .unwrap();
    let mut async_put = |table: &ProductTable, row_id| {
        let engine = cluster.sim.rl().storages[&1].clone();
        let cfg = cluster.cfg.tikv.server.clone();
        let pd_client = cluster.pd_client.clone();
        let key = table.get_table_prefix();
        let split_key = Key::from_raw(&key).into_encoded();
        let ctx = cluster.get_ctx(&split_key);
        let table_ = table.clone();
        let (tx, rx) = unbounded();
        let handle = std::thread::spawn(move || {
            tx.send(()).unwrap();
            let _ = init_data_with_details_pd_client(
                ctx,
                engine,
                &table_,
                &[(row_id, Some(&format!("name:{}", row_id)), row_id)],
                true,
                &cfg,
                Some(pd_client),
            );
        });
        rx.recv_timeout(Duration::from_secs(5)).unwrap();
        handle
    };
    let handle1 = async_put(&product1, 2);
    handle_put_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    std::thread::sleep(Duration::from_secs(1));
    {
        let mut core = range_cache_engine.core().write();
        // try to load a region with old epoch and bigger range,
        // it should be updated to the real region range.
        core.mut_range_manager()
            .load_region(CacheRegion::new(r_split.id, 0, DATA_MIN_KEY, DATA_MAX_KEY))
            .unwrap();
    }

    let (tx, rx) = sync_channel(1);
    fail::cfg_callback("on_snapshot_load_finished", move || {
        tx.send(true).unwrap();
    })
    .unwrap();

    let handle2 = async_put(&product2, 9);
    let _ = rx.recv_timeout(Duration::from_secs(5)).unwrap();

    drop(handle_put_pause_tx);

    {
        let range_cache_engine = cluster.sim.rl().get_range_cache_engine(1);
        let core = range_cache_engine.core().read();
        let meta = core.range_manager().region_meta(r_split.id).unwrap();
        let split_range = CacheRegion::from_region(&r_split);
        assert_eq!(&split_range, meta.get_region());
    }

    fail::remove("on_handle_put");
    std::thread::sleep(Duration::from_secs(1));
    handle1.join().unwrap();
    handle2.join().unwrap();

    let (tx, rx) = unbounded();
    fail::cfg_callback("on_range_cache_iterator_seek", move || {
        tx.send(true).unwrap();
    })
    .unwrap();
    must_copr_point_get(&mut cluster, &product2, 9);
    assert!(rx.try_recv().unwrap());

    // ["", table2) should not cached.
    must_copr_load_data(&mut cluster, &product1, 3);
    let (tx, rx) = unbounded();
    fail::remove("on_range_cache_iterator_seek");
    fail::cfg_callback("on_range_cache_iterator_seek", move || {
        tx.send(true).unwrap();
    })
    .unwrap();
    must_copr_point_get(&mut cluster, &product1, 2);
    rx.try_recv().unwrap_err();
}

// It tests that for a apply delegate, at the time it prepares to apply
// something, the range of it is in pending range. When it begins to write the
// write batch to engine, the range has finished the loading, became a normal
// range, and even been evicted.
#[test]
fn test_load_with_eviction() {
    let mut cluster = new_server_cluster_with_hybrid_engine_with_no_range_cache(0, 1);
    cluster.run();
    // load range
    {
        let range_cache_engine = cluster.sim.rl().get_range_cache_engine(1);
        let mut core = range_cache_engine.core().write();
        // Load the whole range as if it is not splitted. Loading process should handle
        // it correctly.
        let cache_range = CacheRegion::new(1, 0, DATA_MIN_KEY, DATA_MAX_KEY);
        core.mut_range_manager().load_region(cache_range).unwrap();
    }

    let product1 = ProductTable::new();
    let product2 = ProductTable::new();

    let key = product2.get_table_prefix();
    let split_key = Key::from_raw(&key).into_encoded();
    let r = cluster.get_region(&split_key);
    cluster.must_split(&r, &split_key);

    fail::cfg("on_range_cache_write_batch_write_impl", "pause").unwrap();
    let mut async_put = |table: &ProductTable, row_id| {
        let engine = cluster.sim.rl().storages[&1].clone();
        let cfg = cluster.cfg.tikv.server.clone();
        let pd_client = cluster.pd_client.clone();
        let key = table.get_table_prefix();
        let split_key = Key::from_raw(&key).into_encoded();
        let ctx = cluster.get_ctx(&split_key);
        let table_ = table.clone();
        std::thread::spawn(move || {
            let _ = init_data_with_details_pd_client(
                ctx,
                engine,
                &table_,
                &[(row_id, Some(&format!("name:{}", row_id)), row_id)],
                true,
                &cfg,
                Some(pd_client),
            );
        })
    };
    let handle1 = async_put(&product1, 1);
    let handle2 = async_put(&product2, 15);

    {
        let range_cache_engine = cluster.sim.rl().get_range_cache_engine(1);
        let mut tried_count = 0;
        while range_cache_engine
            .snapshot(CacheRegion::from_region(&r), u64::MAX, u64::MAX)
            .is_err()
            && tried_count < 5
        {
            std::thread::sleep(Duration::from_millis(100));
            tried_count += 1;
        }
        // Now, the range ["", "") should be cached
        let region = new_region(1, split_key, "");
        range_cache_engine.evict_region(
            &CacheRegion::from_region(&region),
            EvictReason::AutoEvict,
            None,
        );
    }

    fail::remove("on_range_cache_write_batch_write_impl");
    handle1.join().unwrap();
    handle2.join().unwrap();

    for (table, is_cached) in &[(product1, true), (product2, false)] {
        fail::remove("on_range_cache_iterator_seek");
        let (tx, rx) = unbounded();
        fail::cfg_callback("on_range_cache_iterator_seek", move || {
            tx.send(true).unwrap();
        })
        .unwrap();

        let key = table.get_table_prefix();
        let table_key = Key::from_raw(&key).into_encoded();
        let ctx = cluster.get_ctx(&table_key);
        let endpoint = cluster.sim.rl().copr_endpoints[&1].clone();
        let req = DagSelect::from(table).build_with(ctx, &[0]);
        let cop_resp = handle_request(&endpoint, req);
        let mut resp = SelectResponse::default();
        resp.merge_from_bytes(cop_resp.get_data()).unwrap();
        assert!(!cop_resp.has_region_error(), "{:?}", cop_resp);
        assert!(cop_resp.get_other_error().is_empty(), "{:?}", cop_resp);

        if *is_cached {
            rx.try_recv().unwrap();
        } else {
            rx.try_recv().unwrap_err();
        }
    }
}

#[test]
fn test_evictions_after_transfer_leader() {
    let mut cluster = new_server_cluster_with_hybrid_engine_with_no_range_cache(0, 2);
    cluster.run();

    let r = cluster.get_region(b"");
    cluster.must_transfer_leader(r.id, new_peer(1, 1));

    let cache_region = CacheRegion::new(1, 0, DATA_MIN_KEY, DATA_MAX_KEY);
    let range_cache_engine = {
        let range_cache_engine = cluster.sim.rl().get_range_cache_engine(1);
        let mut core = range_cache_engine.core().write();
        core.mut_range_manager().new_region(cache_region.clone());
        drop(core);
        range_cache_engine
    };

    range_cache_engine
        .snapshot(cache_region.clone(), 100, 100)
        .unwrap();

    cluster.must_transfer_leader(r.id, new_peer(2, 2));
    range_cache_engine
        .snapshot(cache_region, 100, 100)
        .unwrap_err();
}

#[test]
fn test_eviction_after_merge() {
    let mut cluster = new_server_cluster_with_hybrid_engine_with_no_range_cache(0, 1);
    cluster.run();
    let r = cluster.get_region(b"");
    cluster.must_split(&r, b"key1");

    let r = cluster.get_region(b"");
    let range1 = CacheRegion::from_region(&r);
    let r2 = cluster.get_region(b"key1");
    let range2 = CacheRegion::from_region(&r2);

    let range_cache_engine = {
        let range_cache_engine = cluster.sim.rl().get_range_cache_engine(1);
        let mut core = range_cache_engine.core().write();
        core.mut_range_manager().new_region(range1.clone());
        core.mut_range_manager().new_region(range2.clone());
        drop(core);
        range_cache_engine
    };

    range_cache_engine
        .snapshot(range1.clone(), 100, 100)
        .unwrap();
    range_cache_engine
        .snapshot(range2.clone(), 100, 100)
        .unwrap();

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.must_merge(r.get_id(), r2.get_id());

    range_cache_engine.snapshot(range1, 100, 100).unwrap_err();
    range_cache_engine.snapshot(range2, 100, 100).unwrap_err();
}

#[test]
fn test_eviction_after_ingest_sst() {
    let mut cluster = new_server_cluster_with_hybrid_engine_with_no_range_cache(0, 1);
    cluster.run();

    // Generate a sst file.
    let tmp_dir = tempdir().unwrap();
    let sst_file_path = tmp_dir.path().join("test.sst");
    let mut writer = RocksSstWriterBuilder::new()
        .build(sst_file_path.to_str().unwrap())
        .unwrap();
    writer.put(&data_key(b"key"), b"value").unwrap();
    writer.finish().unwrap();

    // Add region r to cache.
    let region = cluster.get_region(b"");
    let cache_region = CacheRegion::from_region(&region);
    let range_cache_engine = {
        let range_cache_engine = cluster.sim.rl().get_range_cache_engine(1);
        let mut core = range_cache_engine.core().write();
        core.mut_range_manager().new_region(cache_region.clone());
        drop(core);
        range_cache_engine
    };

    range_cache_engine
        .snapshot(cache_region.clone(), 100, 100)
        .unwrap();

    // Ingest the sst file.
    //
    // Build sst meta.
    let mut sst_meta = SstMeta::default();
    sst_meta.region_id = region.get_id();
    sst_meta.set_region_epoch(region.get_region_epoch().clone());
    sst_meta.set_uuid(uuid::Uuid::new_v4().as_bytes().to_vec());
    let mut content = vec![];
    let mut sst_file = File::open(&sst_file_path).unwrap();
    sst_file.read_to_end(&mut content).unwrap();
    sst_meta.crc32 = calc_crc32_bytes(&content);
    sst_meta.length = content.len() as _;
    sst_meta.cf_name = CF_DEFAULT.to_owned();

    // Prepare ingest.
    let importer = cluster.sim.rl().importers.get(&1).unwrap().clone();
    let mut f = importer.create(&sst_meta).unwrap();
    f.append(&content).unwrap();
    f.finish().unwrap();

    // Make ingest command.
    let mut ingest = Request::default();
    ingest.set_cmd_type(CmdType::IngestSst);
    ingest.mut_ingest_sst().set_sst(sst_meta.clone());
    let mut header = RaftRequestHeader::default();
    let leader = cluster.leader_of_region(region.get_id()).unwrap();
    header.set_peer(leader);
    header.set_region_id(region.get_id());
    header.set_region_epoch(region.get_region_epoch().clone());
    let mut cmd = RaftCmdRequest::default();
    cmd.set_header(header);
    cmd.mut_requests().push(ingest);
    let resp = cluster
        .call_command_on_leader(cmd, Duration::from_secs(5))
        .unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);

    range_cache_engine
        .snapshot(cache_region, 100, 100)
        .unwrap_err();
}
