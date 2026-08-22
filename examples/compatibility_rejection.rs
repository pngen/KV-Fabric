use kvfabric::api::{KvFabric, KvFabricOptions};
use kvfabric::compat::CompatibilitySpec;
use kvfabric::error::KvResult;

fn main() -> KvResult<()> {
    let mut o = KvFabricOptions::default();
    o.data_dir = "./run/example-g".into();
    let f = KvFabric::new(o)?;
    let ns = "default";
    let tokens: Vec<u32> = vec![1, 2, 3, 4, 5, 6, 7, 8];

    // Build state under a specific model identity + revision.
    let spec = CompatibilitySpec { model_identity: "llama".into(), model_revision: "3.1-8b".into(), ..Default::default() };
    let compat = spec.key();
    let meta = f.new_object_metadata(ns, &spec);
    let id = meta.object_id;
    f.create_object(ns, meta)?;
    f.append_block(id, 0, &vec![9u8; 512])?;
    f.commit(ns, &compat, &tokens, id)?;

    // Same tokens, but a different model family: reuse must be rejected.
    let other = CompatibilitySpec { model_identity: "gemma".into(), model_revision: "2b".into(), ..Default::default() };
    let hit_other = f.lookup(ns, &other.key(), &tokens)?;
    println!("same tokens + incompatible spec -> hit len = {} (must be 0)", hit_other.token_len);

    // A revision bump is also incompatible with the canonical key.
    let rev = CompatibilitySpec { model_identity: "llama".into(), model_revision: "3.1-70b".into(), ..Default::default() };
    let hit_rev = f.lookup(ns, &rev.key(), &tokens)?;
    println!("same model, different revision -> hit len = {} (must be 0)", hit_rev.token_len);

    // The exact compatible request still hits.
    let hit_ok = f.lookup(ns, &compat, &tokens)?;
    println!("exact compatible -> hit len = {} (must be {})", hit_ok.token_len, tokens.len());
    let _ = std::fs::remove_dir_all(&f.options().data_dir);
    Ok(())
}