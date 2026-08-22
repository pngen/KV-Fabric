use kvfabric::api::{KvFabric, KvFabricOptions};
use kvfabric::compat::CompatibilitySpec;
use kvfabric::error::KvResult;

fn main() -> KvResult<()> {
    let mut o = KvFabricOptions::default();
    o.data_dir = "./run/example-d".into();
    o.node_count = 2;
    let f = KvFabric::new(o)?;
    let spec = CompatibilitySpec { model_identity: "llama".into(), ..Default::default() };
    let ns = "default";
    let compat = spec.key();
    let tokens: Vec<u32> = (0..1024).collect();

    // Node A builds and commits reusable state.
    let meta = f.new_object_metadata(ns, &spec);
    let id = meta.object_id;
    f.create_object(ns, meta)?;
    for i in 0..tokens.len().div_ceil(64) {
        f.append_block(id, i as u32, &vec![i as u8; 512])?;
    }
    f.commit(ns, &compat, &tokens, id)?;
    println!("node A committed object {} ({} blocks)", id, tokens.len().div_ceil(64));

    // Node B requests the same prefix. It has nothing local yet.
    let b_before = f.node(1).unwrap().lookup(ns, &compat, &tokens);
    println!("node B lookup before transfer: hit={}", b_before.is_some());
    // The planner decides: recomputation cost vs transfer cost. Transfer is
    // cheaper, so the runtime transfers a replica then serves it.
    f.replicate_to(id, 1)?;
    // Re-associate the prefix with the transferred blocks on node B.
    f.index_prefix(ns, &compat, &tokens, id)?;
    let b_after = f.node(1).unwrap().lookup(ns, &compat, &tokens);
    println!("node B lookup after transfer: hit={} len={}", b_after.is_some(), b_after.map(|m| m.token_len).unwrap_or(0));

    // Migrate ownership A -> B (disciplined plan/transfer/verify/commit).
    f.migrate(id, 0, 1)?;
    let owner = f.coordinator().owner_of(id).map(|(n, _, _)| n).unwrap_or(kvfabric::id::NodeId(0));
    println!("after migration, canonical owner = node {}", owner.to_u64());
    let s = f.stats();
    println!("telemetry: migrations={} bytes_transferred={}", s.migrations, s.bytes_transferred);
    let _ = std::fs::remove_dir_all(&f.options().data_dir);
    Ok(())
}