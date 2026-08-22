//! In-process benchmark suite, runnable via the CLI benchmark command.
//!
//! Benchmarks measure real throughput / latency of the core operations. They
//! use the runtime facade on a scratch data directory and report measures that
//! are actually executed (no fabricated numbers).

use std::time::{Duration, Instant};

use crate::api::{KvFabric, KvFabricOptions};
use crate::compat::CompatibilitySpec;
use crate::error::KvResult;

pub fn bench_run(data_dir: &str) -> KvResult<()> {
    let _ = std::fs::create_dir_all(data_dir);
    let mut opts = KvFabricOptions::default();
    opts.data_dir = data_dir.to_string();
    let f = KvFabric::new(opts)?;
    let spec = CompatibilitySpec { model_identity: "bench".into(), model_revision: "1".into(), ..Default::default() };
    let mut out = Vec::new();

    // Exact & longest-prefix lookup.
    let tokens: Vec<u32> = (0..1024).collect();
    let n = 50;
    let mut ids = Vec::new();
    let start = Instant::now();
    for i in 0..n {
        let meta = f.new_object_metadata("bench", &spec);
        let id = meta.object_id;
        let t = tokens[..(*&tokens.len() - i % 20)].to_vec();
        f.create_object("bench", meta)?;
        f.append_block(id, 0, &[i as u8; 4096])?;
        f.commit("bench", &spec.key(), &t, id)?;
        ids.push(id);
    }
    let commit_dur = start.elapsed();
    out.push(format!("commit (create+append+commit) x{}: {:.3} ms total, {:.3} ms/op", n, ms(commit_dur), ms(commit_dur) / n as f64));

    // Lookup throughput.
    let iters = 2000;
    let start = Instant::now();
    let mut hits = 0;
    for _ in 0..iters {
        let o = f.lookup("bench", &spec.key(), &tokens)?;
        if o.token_len > 0 { hits += 1; }
    }
    let look_dur = start.elapsed();
    out.push(format!("exact/longest lookup x{}: {:.3} ms total, {:.3} us/op, hits={}",
        iters, ms(look_dur), us_per_op(look_dur, iters), hits));

    // Host store copy + integrity.
    let payload: Vec<u8> = vec![7u8; 1 << 20];
    let start = Instant::now();
    for _ in 0..50 {
        let _ = crate::hash::crc32c(&payload);
        let _ = crate::hash::sha256(&payload);
    }
    let hid = start.elapsed();
    let mbps = (50.0 * (payload.len() as f64) / (1024.0 * 1024.0)) / secs(hid);
    out.push(format!("integrity (crc32c+sha256 1MiB) x50: {:.2} MiB/s", mbps));

    // Persistence + restore.
    let start = Instant::now();
    f.persist(ids[0])?;
    f.reclaim(ids[0])?;
    f.restore(ids[0])?;
    let pd = start.elapsed();
    out.push(format!("persist+reclaim+restore: {:.3} ms", ms(pd)));

    // Promotion / demotion.
    let start = Instant::now();
    f.promote(ids[0])?;
    f.demote(ids[0])?;
    let md = start.elapsed();
    out.push(format!("promote+demote: {:.3} ms", ms(md)));

    let stats = f.stats();
    out.push(format!("stats: objects={} hits={} misses={} promotions={} demotions={}",
        stats.object_count, stats.hits, stats.misses, stats.promotions, stats.demotions));
    println!("{}", out.join("\n"));
    let _ = std::fs::remove_dir_all(data_dir);
    Ok(())
}

fn ms(d: Duration) -> f64 { d.as_secs_f64() * 1000.0 }
fn secs(d: Duration) -> f64 { d.as_secs_f64() }
fn us_per_op(d: Duration, n: usize) -> f64 { d.as_secs_f64() * 1_000_000.0 / n as f64 }