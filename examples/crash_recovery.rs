use kvfabric::api::{KvFabric, KvFabricOptions};
use kvfabric::compat::CompatibilitySpec;
use kvfabric::error::KvResult;

fn main() -> KvResult<()> {
    let data_dir = std::env::temp_dir().join("kvfabric-crash-recover").to_string_lossy().into_owned();
    let _ = std::fs::remove_dir_all(&data_dir);
    let spec = CompatibilitySpec { model_identity: "llama".into(), ..Default::default() };
    let compat = spec.key();
    let tokens: Vec<u32> = (0..512).collect();

    // First process: create, commit and persist an object durably.
    let mut o = KvFabricOptions::default();
    o.data_dir = data_dir.clone();
    let id;
    {
        let f = KvFabric::new(o.clone())?;
        let meta = f.new_object_metadata("default", &spec);
        id = meta.object_id;
        f.create_object("default", meta)?;
        f.append_block(id, 0, &vec![7u8; 512])?;
        f.commit("default", &compat, &tokens, id)?;
        f.persist(id)?;
        println!("first process persisted object {} {}", id, "in block 0");
    }

    // Hard termination of the process; drop all in-memory state. Simulate a
    // restart by constructing a fresh runtime over the same durable directory.
    println!("simulating crash...");
    let f2 = KvFabric::new(o)?;
    // Re-associate the prefix with the recovered durable blocks.
    f2.index_prefix("default", &compat, &tokens, id)?;
    let recovered = f2.node(0).unwrap().object_metadata(id);
    match recovered {
        Some(m) => println!("recovered object {}: lifecycle={} persistence={:?} bytes={}", m.object_id, m.lifecycle.as_str(), m.persistence.code(), m.byte_size),
        None => println!("object NOT recovered (unexpected)"),
    }
    let hit = f2.lookup("default", &compat, &tokens)?;
    println!("after recovery, lookup hit={} len={}", hit.token_len > 0, hit.token_len);
    let _ = std::fs::remove_dir_all(&data_dir);
    Ok(())
}