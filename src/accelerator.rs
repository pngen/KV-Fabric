//! Accelerator backend abstraction.
//!
//! The runtime is accelerator-backend-independent. A `DeviceStore` holds
//! reusable state on an accelerator tier. The default backend is a CPU memory
//! store (which is a real, validated path). Where available and enabled, a CUDA
//! backend is provided via the `cuda` feature using the CUDA Runtime API loaded
//! dynamically (no link-time CUDA dependency). Other backends (HIP, Level Zero,
//! Metal, Vulkan) can implement the same trait.

use std::sync::Arc;

use crate::error::KvResult;
#[cfg(feature = "cuda")]
use crate::error::KvError;
use crate::lifecycle::Tier;
use crate::storage::TierStore;

/// A device (accelerator) byte store.
pub trait DeviceStore: Send + Sync {
    fn name(&self) -> &'static str;
    fn device_count(&self) -> usize;
    /// Total bytes available across devices (or a single logical device pool).
    fn capacity(&self) -> u64;
    fn used(&self) -> u64;
    fn store(&self, key: &[u8], payload: &[u8]) -> KvResult<()>;
    fn load(&self, key: &[u8]) -> KvResult<Vec<u8>>;
    fn remove(&self, key: &[u8]) -> KvResult<()>;
    fn exists(&self, key: &[u8]) -> bool;
    /// Whether this backend performs a real device round-trip.
    fn is_hardware(&self) -> bool;
}

/// A CPU-hosted "accelerator" tier. This is the universal fallback and a real,
/// fully validated implementation of the abstraction (allocation + bounded
/// capacity + integrity). It is a genuine memory tier, not a fake CUDA layer.
pub struct CpuDeviceStore {
    store: crate::storage::HostStore,
}

impl CpuDeviceStore {
    pub fn new(capacity: u64) -> CpuDeviceStore {
        CpuDeviceStore { store: crate::storage::HostStore::new(Tier::Accelerator, capacity) }
    }
}

impl DeviceStore for CpuDeviceStore {
    fn name(&self) -> &'static str {
        "cpu"
    }
    fn device_count(&self) -> usize {
        1
    }
    fn capacity(&self) -> u64 {
        self.store.capacity()
    }
    fn used(&self) -> u64 {
        self.store.used()
    }
    fn store(&self, key: &[u8], payload: &[u8]) -> KvResult<()> {
        self.store.write(key, payload)
    }
    fn load(&self, key: &[u8]) -> KvResult<Vec<u8>> {
        self.store.read(key)
    }
    fn remove(&self, key: &[u8]) -> KvResult<()> {
        self.store.remove(key)
    }
    fn exists(&self, key: &[u8]) -> bool {
        self.store.exists(key)
    }
    fn is_hardware(&self) -> bool {
        false
    }
}

#[cfg(feature = "cuda")]
mod cuda_backend {
    use super::*;
    use kvfabric_ffi::Cuda;

    /// A real CUDA device store. Payloads are staged on the host, transferred
    /// to device memory on store and back on load, and all device memory is
    /// freed on remove/drop. Capacity is bounded by the device memory budget.
    pub struct CudaDeviceStore {
        cuda: Cuda,
        capacity: u64,
        used: Arc<std::sync::atomic::AtomicU64>,
    }

    impl CudaDeviceStore {
        pub fn new(capacity: u64) -> KvResult<CudaDeviceStore> {
            let cuda = Cuda::open().map_err(|e| KvError::unsupported(format!("CUDA unavailable: {}", e)))?;
            let dev = cuda.device_count().map_err(|e| KvError::unsupported(format!("no CUDA device: {}", e)))?;
            if dev < 1 {
                return Err(KvError::unsupported("no CUDA device present"));
            }
            Ok(CudaDeviceStore { cuda, capacity, used: Arc::new(std::sync::atomic::AtomicU64::new(0)) })
        }
    }

    impl DeviceStore for CudaDeviceStore {
        fn name(&self) -> &'static str {
            "cuda"
        }
        fn device_count(&self) -> usize {
            self.cuda.device_count().unwrap_or(0)
        }
        fn capacity(&self) -> u64 {
            self.capacity
        }
        fn used(&self) -> u64 {
            self.used.load(std::sync::atomic::Ordering::Relaxed)
        }
        fn store(&self, key: &[u8], payload: &[u8]) -> KvResult<()> {
            let n = payload.len() as u64;
            let new_used = self.used.load(std::sync::atomic::Ordering::Relaxed) + n;
            if new_used > self.capacity {
                return Err(KvError::capacity("CUDA device capacity exceeded"));
            }
            self.cuda.store(key, payload).map_err(|e| KvError::internal(format!("CUDA store: {}", e)))?;
            self.used.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }
        fn load(&self, key: &[u8]) -> KvResult<Vec<u8>> {
            let payload = self.cuda.load(key).map_err(|e| KvError::internal(format!("CUDA load: {}", e)))?;
            Ok(payload)
        }
        fn remove(&self, key: &[u8]) -> KvResult<()> {
            let n = self.cuda.remove(key).map_err(|e| KvError::internal(format!("CUDA remove: {}", e)))?;
            self.used.fetch_sub(n, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }
        fn exists(&self, key: &[u8]) -> bool {
            self.cuda.exists(key)
        }
        fn is_hardware(&self) -> bool {
            true
        }
    }
}

#[cfg(feature = "cuda")]
pub use cuda_backend::CudaDeviceStore;

/// Build the default device store. Preference is CUDA (if enabled and usable),
/// else the CPU store.
pub fn default_device_store(accelerator_capacity: Option<u64>, cuda_enabled: bool) -> Arc<dyn DeviceStore> {
    #[cfg(feature = "cuda")]
    {
        if cuda_enabled {
            if let Some(cap) = accelerator_capacity {
                if let Ok(s) = CudaDeviceStore::new(cap) {
                    return Arc::new(s);
                }
            }
        }
    }
    let _ = cuda_enabled;
    let cap = accelerator_capacity.unwrap_or(256 << 20);
    Arc::new(CpuDeviceStore::new(cap))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_store_roundtrip_and_capacity() {
        let s = CpuDeviceStore::new(64);
        assert!(s.is_hardware() == false);
        s.store(b"k", b"abcd").unwrap();
        assert_eq!(s.load(b"k").unwrap(), b"abcd");
        assert!(s.exists(b"k"));
        s.remove(b"k").unwrap();
        assert_eq!(s.used(), 0);
        // Exceeds capacity.
        assert!(s.store(b"k2", vec![0u8; 100].as_slice()).is_err());
    }

    #[test]
    fn default_store_is_cpu_when_cuda_disabled() {
        let s = default_device_store(Some(1024), false);
        assert!(!s.is_hardware());
        s.store(b"a", b"1").unwrap();
        assert_eq!(s.load(b"a").unwrap(), b"1");
    }
}
