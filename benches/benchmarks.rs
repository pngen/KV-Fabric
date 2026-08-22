//! Benchmark harness (harness = false).
//!
//! Runs the KV Fabric in-process benchmark suite and reports throughput /
//! latency for the core operations. Uses the runtime facade on a scratch
//! directory. Numbers are actually executed; nothing is fabricated.

fn main() {
    println!("KV Fabric 1.0.0 benchmark suite");
    if let Err(e) = kvfabric::bench::bench_run("./bench-scratch") {
        eprintln!("benchmark error: {}", e);
        std::process::exit(1);
    }
}