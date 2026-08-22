//! Multi-process test: run a real coordinator and node as separate OS processes
//! communicating over framed TCP, then query the coordinator from a client.

use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use kvfabric::protocol::{read_frame, write_frame, Op};

struct ChildGuard {
    children: Vec<Child>,
}

impl ChildGuard {
    fn new() -> ChildGuard { ChildGuard { children: Vec::new() } }
    fn spawn(&mut self, bin: &str, args: &[&str]) {
        let c = Command::new(bin).args(args).stdout(Stdio::null()).stderr(Stdio::null())
            .spawn().expect("spawn child");
        self.children.push(c);
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        for c in self.children.iter_mut() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

fn status_ready(addr: &str, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Ok(mut s) = TcpStream::connect(addr) {
            if write_frame(&mut s, 1, Op::Status, b"").is_ok() {
                if let Ok(f) = read_frame(&mut s) {
                    if f.op == Op::Reply { return true; }
                }
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

#[test]
fn multi_process_coordinator_node_status() {
    let bin = env!("CARGO_BIN_EXE_kvfabric");
    let coord_dir = std::env::temp_dir().join(format!("kvfabric-mp-coord-{}", std::process::id()));
    let node_dir = std::env::temp_dir().join(format!("kvfabric-mp-node-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&coord_dir);
    let _ = std::fs::remove_dir_all(&node_dir);
    let mut guard = ChildGuard::new();

    guard.spawn(bin, &["coordinator", "--listen", "127.0.0.1:7801", "--data", coord_dir.to_str().unwrap()]);
    assert!(status_ready("127.0.0.1:7801", Duration::from_secs(5)), "coordinator did not come up");

    guard.spawn(bin, &["node", "--id", "7", "--coordinator", "127.0.0.1:7801", "--listen", "127.0.0.1:7802", "--data", node_dir.to_str().unwrap()]);

    // Give the node a moment to register, then query the coordinator.
    std::thread::sleep(Duration::from_millis(600));
    let mut s = TcpStream::connect("127.0.0.1:7801").expect("connect to coordinator");
    write_frame(&mut s, 1, Op::Status, b"").unwrap();
    let frame = read_frame(&mut s).expect("read status reply");
    assert_eq!(frame.op, Op::Reply);
    let mut c = kvfabric::bytes::Cursor::new(&frame.payload);
    let text = c.len_str(4096).unwrap();
    assert!(text.contains("epoch="), "status must report the authority epoch: {}", text);
    assert!(text.contains("nodes=1"), "status must report the registered node: {}", text);
    assert!(text.contains("objects=0"), "status must report object count: {}", text);
    drop(guard);
    let _ = std::fs::remove_dir_all(&coord_dir);
    let _ = std::fs::remove_dir_all(&node_dir);
}