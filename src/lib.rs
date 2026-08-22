//! KV Fabric — a vendor-neutral distributed runtime for governing reusable
//! KV inference state across accelerator memory, system memory, persistent
//! storage, processes, and execution nodes.
//!
//! This crate is intentionally dependency-free (Rust standard library only)
//! so that it builds offline, reproducibly, and without proprietary inputs.

#![forbid(unsafe_code)]

pub mod error;
pub mod hash;
pub mod rng;
pub mod time;
pub mod id;
pub mod bytes;
pub mod compat;
pub mod metadata;
pub mod lifecycle;
pub mod block;
pub mod prefix;
pub mod economy;
pub mod admission;
pub mod reclaim;
pub mod lease;
pub mod integrity;
pub mod telemetry;
pub mod storage;
pub mod accelerator;
pub mod protocol;
pub mod config;
pub mod persistence;
pub mod coordinator;
pub mod node;
pub mod migration;
pub mod replica;
pub mod api;
pub mod cli;
pub mod bench;

pub use error::{KvError, KvResult};
pub use id::{BlockId, Generation, NodeId, ObjectId, PrefixId};
