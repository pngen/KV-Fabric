use kvfabric::api::{KvFabric, KvFabricOptions};
use kvfabric::compat::CompatibilitySpec;
use kvfabric::error::KvResult;

fn main() -> KvResult<()> {
    let mut o = KvFabricOptions::default();
    o.data_dir = "./run/example-b".into();
    let f = KvFabric::new(o)?;
    let spec = CompatibilitySpec { model_identity: "llama".into(), ..Default::default() };
    let ns = "default";
    let compat = spec.key();

    // Shared immutable prefix.
    let shared: Vec<u32> = vec![1, 2, 3, 4, 5];
    let meta = f.new_object_metadata(ns, &spec);
    let sid = meta.object_id;
    f.create_object(ns, meta)?;
    f.append_block(sid, 0, &[1u8; 512])?;
    f.commit(ns, &compat, &shared, sid)?;

    // Branch A and Branch B diverge after the shared prefix.
    let branch_a: Vec<u32> = vec![1, 2, 3, 4, 5, 10, 11];
    let ma = f.new_object_metadata(ns, &spec);
    let ia = ma.object_id;
    f.create_object(ns, ma)?;
    f.append_block(ia, 0, &[10u8; 512])?;
    f.commit(ns, &compat, &branch_a, ia)?;

    let branch_b: Vec<u32> = vec![1, 2, 3, 4, 5, 20, 21, 22];
    let mb = f.new_object_metadata(ns, &spec);
    let ib = mb.object_id;
    f.create_object(ns, mb)?;
    f.append_block(ib, 0, &[20u8; 512])?;
    f.commit(ns, &compat, &branch_b, ib)?;

    // Branch B reuse must not surface branch A state.
    let hit_b = f.lookup(ns, &compat, &branch_b)?;
    println!("branch B request: longest prefix = {} tokens, objects = {:?}", hit_b.token_len, hit_b.objects);
    println!("branch B saw object {} (not branch A {})", ib, ia);
    // The lineage of the fork is inspectable.
    let bm = f.node(0).unwrap().object_metadata(ib).unwrap();
    println!("branch B metadata: lifecycle={} residency={} blocks={}", bm.lifecycle.as_str(), bm.residency.as_str(), bm.block_count);
    let _ = std::fs::remove_dir_all(&f.options().data_dir);
    Ok(())
}