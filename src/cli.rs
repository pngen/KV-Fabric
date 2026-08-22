//! Command-line interface for KV Fabric.
//!
//! The CLI can run a coordinator and node as separate processes (a real
//! multi-process control plane over framed TCP), query cluster status, and
//! drive the runtime lifecycle against a durable local store. Object
//! manipulation commands open the durable store for the configured data dir.

use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;

use crate::api::{KvFabric, KvFabricOptions};
use crate::coordinator::Coordinator;
use crate::error::{KvError, KvResult};
use crate::id::{ObjectId, Token};
use crate::protocol::*;
use crate::time::SystemClock;

const HELP: &str = "\
kvfabric 1.0.0 — vendor-neutral runtime for reusable KV inference state

USAGE:
  kvfabric <command> [options]

COMMANDS:
  coordinator --listen <addr> --data <dir>   Run the coordinator control plane
  node --id <n> --coordinator <addr> --listen <addr> --data <dir>   Run a node
  status --coordinator <addr>                 Query the cluster
  nodes  [--data <dir>]                       List registered nodes
  objects [--data <dir>]                      List objects in the durable store
  lookup --ns <ns> --model <m> --tokens <csv> [--data <dir>]
  inspect <id> [--data <dir>]                 Inspect an object
  create --ns <ns> --model <m> --tokens <csv> --size <n> [--data <dir>]  Create+commit
  promote|demote|persist|restore|reclaim|retire|verify <id> [--data <dir>]
  migrate --object <id> --from <a> --to <b> [--data <dir>]
  benchmark [--data <dir>]                    Run the in-process benchmark suite
  version                                     Print the version
  help                                        Print this help
";

/// Entry point. Returns the process exit code (0 = success).
pub fn run(args: Vec<String>) -> i32 {
    match run_inner(args) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("kvfabric: {}", e);
            1
        }
    }
}

fn arg(args: &[String], key: &str) -> Option<String> {
    let mut it = args.iter();
    loop {
        match it.next() {
            Some(a) if a == key => return it.next().cloned(),
            Some(_) => {},
            None => return None,
        }
    }
}

fn run_inner(args: Vec<String>) -> KvResult<()> {
    let cmd = args.first().cloned().unwrap_or_else(|| "help".into());
    match cmd.as_str() {
        "help" | "--help" | "-h" => { print!("{}", HELP); Ok(()) }
        "version" | "--version" => { println!("kvfabric 1.0.0"); Ok(()) }
        "coordinator" => run_coordinator(&args),
        "node" => run_node(&args),
        "status" => cmd_status(&args),
        "nodes" => cmd_nodes(&args),
        "objects" => cmd_objects(&args),
        "lookup" => cmd_lookup(&args),
        "inspect" => cmd_inspect(&args),
        "create" => cmd_create(&args),
        "promote" => cmd_op(&args, "promote"),
        "demote" => cmd_op(&args, "demote"),
        "persist" => cmd_op(&args, "persist"),
        "restore" => cmd_op(&args, "restore"),
        "reclaim" => cmd_op(&args, "reclaim"),
        "retire" => cmd_op(&args, "retire"),
        "verify" => cmd_op(&args, "verify"),
        "migrate" => cmd_migrate(&args),
        "benchmark" => cmd_benchmark(&args),
        other => Err(KvError::invalid_arg(format!("unknown command: {}", other))),
    }
}

fn default_data_dir(args: &[String]) -> String {
    arg(args, "--data").unwrap_or_else(|| "./kvfabric-data".into())
}

fn open_fabric(args: &[String]) -> KvResult<KvFabric> {
    let mut opts = KvFabricOptions::default();
    opts.data_dir = default_data_dir(args);
    let _ = std::fs::create_dir_all(&opts.data_dir);
    KvFabric::new(opts)
}

fn run_coordinator(args: &[String]) -> KvResult<()> {
    let listen: SocketAddr = arg(args, "--listen").or_else(|| None).unwrap_or_else(|| "127.0.0.1:7200".into()).parse()?;
    let data = arg(args, "--data").unwrap_or_else(|| "./kvfabric-coordinator".into());
    let cfg = Arc::new(crate::config::Config::default());
    let coord = Arc::new(Coordinator::new(cfg, Arc::new(SystemClock), Some(PathBuf::from(data)))?);
    let listener = TcpListener::bind(listen).map_err(|e| KvError::network(format!("bind: {}", e)))?;
    println!("coordinator listening on {}", listen);
    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                let c = coord.clone();
                std::thread::spawn(move || { let _ = handle_coord_conn(&c, s); });
            }
            Err(_) => continue,
        }
    }
    Ok(())
}

fn handle_coord_conn(coord: &Coordinator, mut stream: TcpStream) -> KvResult<()> {
    loop {
        let frame = match read_frame(&mut stream) {
            Ok(f) => f,
            Err(_) => return Ok(()),
        };
        match frame.op {
            Op::Register => match decode_register(&frame.payload) {
                Ok(m) => {
                    let addr: SocketAddr = match format!("127.0.0.1:{}", m.listen_port).parse() {
                        Ok(a) => a,
                        Err(_) => { let _ = coerr(&mut stream, frame.request_id, &KvError::protocol("bad addr")); continue; }
                    };
                    match coord.register(m.node_id, m.boot_id, addr, m.capabilities) {
                        Ok(res) => {
                            let mut b = crate::bytes::Buffer::new();
                            b.u64(res.epoch);
                            let _ = write_frame(&mut stream, frame.request_id, Op::Reply, &b.into_vec());
                        }
                        Err(e) => { let _ = coerr(&mut stream, frame.request_id, &e); }
                    }
                }
                Err(_) => { let _ = coerr(&mut stream, frame.request_id, &KvError::protocol("bad register")); }
            },
            Op::Heartbeat => match decode_register(&frame.payload) {
                Ok(m) => match coord.heartbeat(m.node_id, m.boot_id) {
                    Ok(epoch) => {
                        let mut b = crate::bytes::Buffer::new();
                        b.u64(epoch);
                        let _ = write_frame(&mut stream, frame.request_id, Op::Reply, &b.into_vec());
                    }
                    Err(e) => { let _ = coerr(&mut stream, frame.request_id, &e); }
                },
                Err(_) => { let _ = coerr(&mut stream, frame.request_id, &KvError::protocol("bad heartbeat")); }
            },
            Op::Status => {
                let mut b = crate::bytes::Buffer::new();
                b.len_str(&format!("epoch={} nodes={} objects={}", coord.epoch(), coord.nodes().len(), coord.object_count()));
                let _ = write_frame(&mut stream, frame.request_id, Op::Reply, &b.into_vec());
            }
            _ => { let _ = coerr(&mut stream, frame.request_id, &KvError::protocol("unhandled op")); }
        }
    }
}

fn run_node(args: &[String]) -> KvResult<()> {
    let id: u64 = arg(args, "--id").unwrap_or_else(|| "1".into()).parse()?;
    let coord_addr: SocketAddr = arg(args, "--coordinator").ok_or_else(|| KvError::invalid_arg("--coordinator required"))?.parse()?;
    let listen: SocketAddr = arg(args, "--listen").unwrap_or_else(|| "127.0.0.1:7300".into()).parse()?;
    let data = arg(args, "--data").unwrap_or_else(|| format!("./kvfabric-node-{}", id));
    let mut cfg = crate::config::Config::default();
    cfg.persistence_enabled = true;
    cfg.data_dir = PathBuf::from(&data);
    let boot = crate::rng::os_rng().next_u64() as u128;
    let node = crate::node::NodeRuntime::new(Arc::new(cfg), crate::id::NodeId(id), boot, PathBuf::from(&data))?;
    let mut s = TcpStream::connect(coord_addr).map_err(|e| KvError::network(format!("connect: {}", e)))?;
    let payload = encode_register(&RegisterMsg { node_id: crate::id::NodeId(id), boot_id: boot, listen_port: listen.port(), capabilities: vec!["cpu".into()] });
    write_frame(&mut s, 1, Op::Register, &payload)?;
    let _ = read_frame(&mut s);
    println!("node {} registered with {} and listening on {}", id, coord_addr, listen);
    let listener = TcpListener::bind(listen).map_err(|e| KvError::network(format!("bind: {}", e)))?;
    let node = Arc::new(node);
    for stream in listener.incoming() {
        match stream {
            Ok(st) => {
                let n = node.clone();
                std::thread::spawn(move || { let _ = n.serve_conn(st); });
            }
            Err(_) => continue,
        }
    }
    Ok(())
}

fn cmd_status(args: &[String]) -> KvResult<()> {
    let addr: SocketAddr = arg(args, "--coordinator").ok_or_else(|| KvError::invalid_arg("--coordinator required"))?.parse()?;
    let mut s = TcpStream::connect(addr).map_err(|e| KvError::network(format!("connect: {}", e)))?;
    write_frame(&mut s, 1, Op::Status, b"")?;
    let f = read_frame(&mut s)?;
    if f.op == Op::Reply {
        let mut c = crate::bytes::Cursor::new(&f.payload);
        let text = c.len_str(4096)?;
        println!("{}", text);
        Ok(())
    } else {
        Err(KvError::protocol("bad status reply"))
    }
}

fn cmd_nodes(args: &[String]) -> KvResult<()> {
    let f = open_fabric(args)?;
    for r in f.coordinator().nodes() {
        println!("node={} boot={:016x} addr={}", r.node_id, r.boot_id, r.addr);
    }
    Ok(())
}

fn cmd_objects(args: &[String]) -> KvResult<()> {
    let f = open_fabric(args)?;
    for i in 0..f.node_count() {
        let node = f.node(i).unwrap();
        for id in node.object_ids() {
            if let Some(m) = node.object_metadata(id) {
                println!("node={} object={} gen={} ns={} lifecycle={} residency={} durable={} bytes={}",
                    i, m.object_id, m.generation, m.namespace, m.lifecycle.as_str(), m.residency.as_str(),
                    m.persistence.code() == 2, m.byte_size);
            }
        }
    }
    Ok(())
}

fn parse_tokens(args: &[String]) -> KvResult<Vec<Token>> {
    let csv = arg(args, "--tokens").ok_or_else(|| KvError::invalid_arg("--tokens required"))?;
    csv.split(",").filter(|s| !s.trim().is_empty()).map(|s| s.trim().parse::<u32>()).collect::<Result<_, _>>()
        .map_err(|e| KvError::invalid_arg(format!("bad token: {}", e)))
}

fn cmd_lookup(args: &[String]) -> KvResult<()> {
    let ns = arg(args, "--ns").unwrap_or_else(|| "default".into());
    let model = arg(args, "--model").unwrap_or_else(|| "llama".into());
    let tokens = parse_tokens(args)?;
    let spec = crate::compat::CompatibilitySpec { model_identity: model, ..Default::default() };
    let compat = spec.key();
    let f = open_fabric(args)?;
    // Re-associate recovered durable objects for the requested sequence.
    let _ = f.reassociate(&ns, &compat, &tokens)?;
    let out = f.lookup(&ns, &compat, &tokens)?;
    if out.token_len > 0 {
        println!("hit prefix={:x} len={} objects={}", out.prefix_id.map(|p| p.to_u64()).unwrap_or(0), out.token_len, out.objects.len());
    } else {
        println!("miss");
    }
    Ok(())
}

fn cmd_inspect(args: &[String]) -> KvResult<()> {
    let id: ObjectId = arg(args, "--id").or_else(|| args.get(1).cloned()).ok_or_else(|| KvError::invalid_arg("object id required"))?.parse()?;
    let f = open_fabric(args)?;
    let m = f.coordinator().object_metadata(id).or_else(|| f.node(0).and_then(|n| n.object_metadata(id)))
        .ok_or_else(|| KvError::not_found("object not found"))?;
    println!("object={} gen={} ns={} lifecycle={} residency={} blocks={} bytes={} dtype={}",
        m.object_id, m.generation, m.namespace, m.lifecycle.as_str(), m.residency.as_str(), m.block_count, m.byte_size, m.dtype);
    println!("  compat={}", crate::hash::hex_encode(&m.compat_key));
    println!("  content_digest={}", crate::hash::hex_encode(&m.content_digest));
    Ok(())
}

fn cmd_create(args: &[String]) -> KvResult<()> {
    let ns = arg(args, "--ns").unwrap_or_else(|| "default".into());
    let model = arg(args, "--model").unwrap_or_else(|| "llama".into());
    let tokens = parse_tokens(args)?;
    let size: usize = arg(args, "--size").unwrap_or_else(|| "4096".into()).parse()?;
    let spec = crate::compat::CompatibilitySpec { model_identity: model, ..Default::default() };
    let f = open_fabric(args)?;
    let meta = f.new_object_metadata(&ns, &spec);
    let id = meta.object_id;
    f.create_object(&ns, meta)?;
    let nblocks = tokens.len().max(1).div_ceil(2) as u32;
    for i in 0..nblocks {
        let payload: Vec<u8> = (0..size).map(|_| i as u8).collect();
        f.append_block(id, i, &payload)?;
    }
    f.commit(&ns, &spec.key(), &tokens, id)?;
    f.index_prefix(&ns, &spec.key(), &tokens, id)?;
    f.persist(id)?;
    println!("committed object {} with {} blocks lasting {} tokens", id, nblocks, tokens.len());
    Ok(())
}

fn cmd_op(args: &[String], op: &str) -> KvResult<()> {
    let id: ObjectId = args.get(1).ok_or_else(|| KvError::invalid_arg(format!("object id required for {}", op)))?.parse()?;
    let f = open_fabric(args)?;
    match op {
        "promote" => f.promote(id)?,
        "demote" => f.demote(id)?,
        "persist" => f.persist(id)?,
        "restore" => f.restore(id)?,
        "reclaim" => f.reclaim(id)?,
        "retire" => f.retire(id)?,
        "verify" => f.verify(id)?,
        _ => return Err(KvError::invalid_arg("unknown op")),
    }
    println!("{}: object {} ok", op, id);
    Ok(())
}

fn cmd_migrate(args: &[String]) -> KvResult<()> {
    let id: ObjectId = arg(args, "--object").or_else(|| args.get(1).cloned()).ok_or_else(|| KvError::invalid_arg("--object required"))?.parse()?;
    let from: usize = arg(args, "--from").unwrap_or_else(|| "0".into()).parse()?;
    let to: usize = arg(args, "--to").unwrap_or_else(|| "1".into()).parse()?;
    let mut opts = KvFabricOptions::default();
    opts.data_dir = default_data_dir(args);
    opts.node_count = 2;
    let _ = std::fs::create_dir_all(&opts.data_dir);
    let f = KvFabric::new(opts)?;
    f.migrate(id, from, to)?;
    println!("migrated {} from node {} to node {} (owner={})", id, from, to, f.coordinator().owner_of(id).map(|(n, _, _)| n).unwrap_or(crate::id::NodeId(0)));
    Ok(())
}

fn coerr<W: std::io::Write>(w: &mut W, request_id: u64, e: &KvError) -> KvResult<()> {
    let mut b = crate::bytes::Buffer::new();
    b.u32(e.kind() as u32);
    b.len_str(&e.message);
    write_frame(w, request_id, Op::ErrCode, &b.into_vec())
}

fn cmd_benchmark(args: &[String]) -> KvResult<()> {
    let data = default_data_dir(args);
    crate::bench::bench_run(&data)?;
    Ok(())
}
