use kvfabric::api::{KvFabric, KvFabricOptions};
use kvfabric::compat::CompatibilitySpec;
use kvfabric::error::KvResult;

fn main() -> KvResult<()> {
    let mut o = KvFabricOptions::default();
    o.data_dir = "./run/example-c".into();
    o.accelerator_capacity_bytes = Some(64 << 20);
    let f = KvFabric::new(o)?;
    let spec = CompatibilitySpec { model_identity: "llama".into(), ..Default::default() };
    let ns = "default";
    let compat = spec.key();
    let tokens: Vec<u32> = (0..2048).collect();

    let meta = f.new_object_metadata(ns, &spec);
    let id = meta.object_id;
    f.create_object(ns, meta)?;
    for i in 0..tokens.len().div_ceil(64) {
        f.append_block(id, i as u32, &vec![i as u8; 512])?;
    }
    f.commit(ns, &compat, &tokens, id)?;
    println!("committed object {} ({} bytes)", id, f.node(0).unwrap().object_metadata(id).unwrap().byte_size);

    // hot (accelerator resident) -> host -> storage -> restore.
    f.promote(id)?;
    let m = f.node(0).unwrap().object_metadata(id).unwrap();
    println!("after promote: residency={}", m.residency.as_str());
    f.demote(id)?;
    let m = f.node(0).unwrap().object_metadata(id).unwrap();
    println!("after demote: residency={}", m.residency.as_str());
    f.persist(id)?;
    let m = f.node(0).unwrap().object_metadata(id).unwrap();
    println!("after persist: lifecycle={} persistence={:?}", m.lifecycle.as_str(), m.persistence.code());
    f.reclaim(id)?;
    let m = f.node(0).unwrap().object_metadata(id).unwrap();
    println!("after reclaim: lifecycle={}", m.lifecycle.as_str());
    f.restore(id)?;
    let m = f.node(0).unwrap().object_metadata(id).unwrap();
    println!("after restore: lifecycle={} residency={}", m.lifecycle.as_str(), m.residency.as_str());
    f.verify(id)?;
    println!("integrity verified");
    let s = f.stats();
    println!("telemetry: promotions={} demotions={} restores={}", s.promotions, s.demotions, s.storage_restores);
    let _ = std::fs::remove_dir_all(&f.options().data_dir);
    Ok(())
}