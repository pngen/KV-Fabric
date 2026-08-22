//! Runtime configuration: tier capacities, quotas, policy versions, endpoints
//! and timeouts. Defaults are safe and self-contained so a bare Config::default
//! produces a working local single-process runtime.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use crate::block::BlockSize;
use crate::error::{KvError, KvResult};
use crate::id::NodeId;

/// Version of each versioned policy. Persist/expose these so behavior can be
/// audited and so a change in policy semantics invalidates assumptions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PolicyVersions {
    pub placement: u32,
    pub admission: u32,
    pub eviction: u32,
    pub planner: u32,
    pub compat_key: u32,
}

impl Default for PolicyVersions {
    fn default() -> Self {
        PolicyVersions {
            placement: 1,
            admission: 1,
            eviction: 1,
            planner: 1,
            compat_key: crate::compat::COMPAT_KEY_VERSION,
        }
    }
}

/// Per-namespace quota and priority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NamespaceQuota {
    pub max_objects: u64,
    pub max_bytes: u64,
    pub max_blocks: u64,
    /// Higher is more important under pressure.
    pub priority: i32,
}

impl Default for NamespaceQuota {
    fn default() -> Self {
        NamespaceQuota {
            max_objects: u64::MAX,
            max_bytes: u64::MAX,
            max_blocks: u64::MAX,
            priority: 0,
        }
    }
}

/// Admission-control knobs.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AdmissionConfig {
    pub enabled: bool,
    pub min_recompute_cost: u64,
    pub min_predicted_reuse: f64,
}

impl Default for AdmissionConfig {
    fn default() -> Self {
        AdmissionConfig {
            enabled: true,
            min_recompute_cost: 1,
            min_predicted_reuse: 0.0,
        }
    }
}

/// Planner cost weights. The planner is fully deterministic for a given input.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PlannerConfig {
    pub transfer_latency_weight: f64,
    pub accelerator_pressure_weight: f64,
    pub host_pressure_weight: f64,
    pub epsilon: f64,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        PlannerConfig {
            transfer_latency_weight: 1.0,
            accelerator_pressure_weight: 1.0,
            host_pressure_weight: 1.0,
            epsilon: 1e-6,
        }
    }
}

/// Reclamation knobs.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReclaimConfig {
    pub pressure_start_fraction: f64,
    pub reclaim_target_fraction: f64,
    pub allow_retire_cheap: bool,
}

impl Default for ReclaimConfig {
    fn default() -> Self {
        ReclaimConfig {
            pressure_start_fraction: 0.9,
            reclaim_target_fraction: 0.05,
            allow_retire_cheap: false,
        }
    }
}

/// Top-level configuration.
#[derive(Debug, Clone)]
pub struct Config {
    pub node_id: Option<NodeId>,
    pub data_dir: PathBuf,
    pub listen_addr: SocketAddr,
    pub coordinator_addr: Option<SocketAddr>,
    pub host_capacity_bytes: u64,
    pub accelerator_capacity_bytes: Option<u64>,
    pub storage_capacity_bytes: u64,
    pub block_size: BlockSize,
    pub namespace_quotas: HashMap<String, NamespaceQuota>,
    pub default_quota: NamespaceQuota,
    pub policy_versions: PolicyVersions,
    pub admission: AdmissionConfig,
    pub planner: PlannerConfig,
    pub reclaim: ReclaimConfig,
    pub lease_ttl: Duration,
    pub epoch_timeout: Duration,
    pub connect_timeout: Duration,
    pub rpc_timeout: Duration,
    pub max_frame_bytes: usize,
    pub max_message_bytes: usize,
    pub max_tokens: usize,
    pub max_blocks_per_object: usize,
    pub allow_non_replicated_reads: bool,
    pub persistence_enabled: bool,
    pub cuda_enabled: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            node_id: None,
            data_dir: PathBuf::from("./kvfabric-data"),
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            coordinator_addr: None,
            host_capacity_bytes: 512 << 20,
            accelerator_capacity_bytes: None,
            storage_capacity_bytes: 4 << 30,
            block_size: BlockSize::default_block_size(),
            namespace_quotas: HashMap::new(),
            default_quota: NamespaceQuota::default(),
            policy_versions: PolicyVersions::default(),
            admission: AdmissionConfig::default(),
            planner: PlannerConfig::default(),
            reclaim: ReclaimConfig::default(),
            lease_ttl: Duration::from_secs(60),
            epoch_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(5),
            rpc_timeout: Duration::from_secs(15),
            max_frame_bytes: 1 << 30,
            max_message_bytes: 64 << 20,
            max_tokens: 1 << 20,
            max_blocks_per_object: 1 << 20,
            allow_non_replicated_reads: false,
            persistence_enabled: false,
            cuda_enabled: false,
        }
    }
}

impl Config {
    pub fn effective_node_id(&mut self, rng: &mut crate::rng::Xoshiro256) -> NodeId {
        *self.node_id.get_or_insert_with(|| NodeId(rng.next_u64()))
    }

    pub fn quota_for(&self, namespace: &str) -> NamespaceQuota {
        self.namespace_quotas.get(namespace).copied().unwrap_or(self.default_quota)
    }

    pub fn validate(&self) -> KvResult<()> {
        if self.host_capacity_bytes == 0 {
            return Err(KvError::invalid_arg("host capacity must be nonzero"));
        }
        if self.storage_capacity_bytes == 0 {
            return Err(KvError::invalid_arg("storage capacity must be nonzero"));
        }
        if self.block_size.elements == 0 || self.block_size.bytes_per_element == 0 {
            return Err(KvError::invalid_arg("block size must be nonzero"));
        }
        if let Some(a) = self.accelerator_capacity_bytes {
            if a == 0 {
                return Err(KvError::invalid_arg("accelerator capacity must be nonzero"));
            }
        }
        if self.max_frame_bytes == 0 || self.max_message_bytes == 0 {
            return Err(KvError::invalid_arg("frame/message limits must be nonzero"));
        }
        if self.lease_ttl.is_zero() {
            return Err(KvError::invalid_arg("lease TTL must be nonzero"));
        }
        if self.reclaim.pressure_start_fraction <= 0.0 || self.reclaim.pressure_start_fraction > 1.0 {
            return Err(KvError::invalid_arg("pressure_start_fraction must be in (0,1]"));
        }
        Ok(())
    }
}

pub const DEFAULT_NAMESPACE: &str = "default";

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn default_is_valid() {
        assert!(Config::default().validate().is_ok());
    }
    #[test]
    fn invalid_capacity_rejected() {
        let mut c = Config::default();
        c.host_capacity_bytes = 0;
        assert!(c.validate().is_err());
    }
    #[test]
    fn quota_lookup() {
        let mut c = Config::default();
        c.namespace_quotas.insert("team-a".into(), NamespaceQuota { max_objects: 5, ..Default::default() });
        assert_eq!(c.quota_for("team-a").max_objects, 5);
        assert_eq!(c.quota_for("missing").max_objects, u64::MAX);
    }
}