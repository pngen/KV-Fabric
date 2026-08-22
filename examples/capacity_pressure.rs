use kvfabric::api::{KvFabric, KvFabricOptions};
use kvfabric::compat::CompatibilitySpec;
use kvfabric::error::KvResult;

fn main() -> KvResult<()> {
    let mut o = KvFabricOptions::default();
    o.data_dir = "./run/example-e".into();
    o.accelerator_capacity_bytes = Some(0);
    o.host_capacity_bytes = Some(1 << 10);
    let f = KvFabric::new(o)?;
    let spec = CompatibilitySpec { model_identity: "llama".into(), ..Default::default() };
    let ns = "default";
    let compat = spec.key();

    // Deliberately tiny capacity so admission must make a decision.
    // We drive capacity by persisting many objects; note that persistence is
    // bounded by the storage tier and reclamation frees resident bytes.
    let mut count = 0;
    let mut over = 0;
    for i in 0..20 {
        let meta = f.new_object_metadata(ns, &spec);
        let id = meta.object_id;
        let attempt = (|| -> KvResult<()> {
            f.create_object(ns, meta)?;
            f.append_block(id, 0, &vec![i as u8; 64])?;
            f.commit(ns, &compat, &vec![i, i + 1], id)?;
            Ok(())
        })();
        match attempt {
            Ok(()) => count += 1,
            Err(e) if e.kind() == kvfabric::error::ErrorKind::Capacity => { over += 1; break; },
            Err(_) => { over += 1; },
        }
    }
    println!("admitted {} objects under pressure; capacity rejections = {}", count, over);

    // Reclamation frees resident memory and restores capacity.
    let stats = f.stats();
    println!("before reclaim: objects={} host_used={}", stats.object_count, stats.host_used);
    for st in f.coordinator().objects_list() {
        let _ = f.reclaim(st.meta.object_id);
    }
    let stats2 = f.stats();
    println!("after reclaim: objects={} host_used={} (resident bytes freed)", stats2.object_count, stats2.host_used);
    let _ = std::fs::remove_dir_all(&f.options().data_dir);
    Ok(())
}