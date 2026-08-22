use kvfabric::api::{KvFabric, KvFabricOptions};
use kvfabric::compat::CompatibilitySpec;
use kvfabric::error::KvResult;

fn main() -> KvResult<()> {
    let mut o = KvFabricOptions::default();
    o.data_dir = "./run/example-a".into();
    let f = KvFabric::new(o)?;
    let spec = CompatibilitySpec { model_identity: "llama".into(), model_revision: "8b".into(), ..Default::default() };
    let ns = "default";
    let compat = spec.key();

    // Build and commit a long shared system prefix.
    let prefix: Vec<u32> = (1000..2000).collect();
    let meta = f.new_object_metadata(ns, &spec);
    let id = meta.object_id;
    f.create_object(ns, meta)?;
    for i in 0..prefix.len().div_ceil(64) {
        f.append_block(id, i as u32, &vec![i as u8; 512])?;
    }
    let m = f.commit(ns, &compat, &prefix, id)?;
    println!("committed shared system prefix: object {} blocks {} tokens {}", id, m.block_count, prefix.len());

    // A later request that contains the shared prefix.
    let request: Vec<u32> = prefix.iter().chain(&[3001, 3002, 3003]).copied().collect();
    let hit = f.lookup(ns, &compat, &request)?;
    println!("request of {} tokens: longest reusable prefix = {} tokens", request.len(), hit.token_len);
    let stats = f.stats();
    println!("reused {} tokens, avoided recomputation; stats hits={} misses={}", hit.token_len, stats.hits, stats.misses);
    let _ = std::fs::remove_dir_all(&f.options().data_dir);
    Ok(())
}