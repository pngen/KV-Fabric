//! A contained, audited CUDA Runtime API loader for KV Fabric.
//!
//! This crate is the only place in KV Fabric that interfaces with C code. It
//! loads the CUDA Runtime API dynamically (no link-time CUDA dependency), wraps
//! it in safe, checked Rust functions, and exposes a small surface: device
//! count, memory info, device allocation, host<->device copy and free. The
//! dynamic module handle is retained and all device allocations are freed on
//! drop.

#![deny(unsafe_op_in_unsafe_fn)]

use std::collections::HashMap;
use std::ffi::c_void;
use std::sync::Mutex;

pub type CudaResult<T> = Result<T, String>;

// cudaMemcpyKind values (CUDA Runtime).
const CUDA_MEMCPY_HOST_TO_DEVICE: u32 = 1;
const CUDA_MEMCPY_DEVICE_TO_HOST: u32 = 2;

type CudaErrorT = i32;
type DevPtr = *mut c_void;

type FnGetDeviceCount = unsafe extern "C" fn(*mut i32) -> CudaErrorT;
type FnMalloc = unsafe extern "C" fn(*mut DevPtr, usize) -> CudaErrorT;
type FnMemcpy = unsafe extern "C" fn(*mut c_void, *const c_void, usize, u32) -> CudaErrorT;
type FnFree = unsafe extern "C" fn(*mut c_void) -> CudaErrorT;
type FnMemGetInfo = unsafe extern "C" fn(*mut usize, *mut usize) -> CudaErrorT;

unsafe impl Send for Cuda {}
unsafe impl Sync for Cuda {}

/// Optional dynamic library handle (Windows HMODULE).
#[derive(Debug)]
enum LibHandle {
    Windows(isize),
}

impl LibHandle {
    fn symbol(&self, name: *const u8) -> *mut c_void {
        match self {
            LibHandle::Windows(h) => {
                unsafe { GetProcAddress(*h, name) }
            }
        }
    }
}

fn load_typed<T: Copy>(lib: &LibHandle, name: &[u8]) -> Option<T> {
    let p = lib.symbol(name.as_ptr());
    if p.is_null() {
        None
    } else {
        Some(unsafe { std::mem::transmute_copy::<*mut c_void, T>(&p) })
    }
}

/// Loaded function pointers.
struct Fns {
    get_device_count: Option<FnGetDeviceCount>,
    malloc: Option<FnMalloc>,
    memcpy: Option<FnMemcpy>,
    free: Option<FnFree>,
    mem_get_info: Option<FnMemGetInfo>,
}

/// A loaded CUDA runtime handle with per-key device allocations.
pub struct Cuda {
    _lib: LibHandle,
    fns: Fns,
    device_allocs: Mutex<HashMap<Vec<u8>, (DevPtr, usize)>>,
}

impl Cuda {
    pub fn open() -> CudaResult<Cuda> {
        let lib = load_cudart()?;
        let fns = Fns {
            get_device_count: load_typed(&lib, b"cudaGetDeviceCount\0"),
            malloc: load_typed(&lib, b"cudaMalloc\0"),
            memcpy: load_typed(&lib, b"cudaMemcpy\0"),
            free: load_typed(&lib, b"cudaFree\0"),
            mem_get_info: load_typed(&lib, b"cudaMemGetInfo\0"),
        };
        if fns.malloc.is_none() || fns.memcpy.is_none() || fns.free.is_none() {
            return Err("cudart is missing required symbols (cudaMalloc/cudaMemcpy/cudaFree)".into());
        }
        Ok(Cuda { _lib: lib, fns, device_allocs: Mutex::new(HashMap::new()) })
    }

    pub fn device_count(&self) -> CudaResult<usize> {
        let f = self.fns.get_device_count.ok_or_else(|| "cudaGetDeviceCount unavailable".to_string())?;
        let mut count: i32 = 0;
        let rc = unsafe { f(&mut count) };
        if rc != 0 {
            return Err(format!("cudaGetDeviceCount failed: rc={}", rc));
        }
        if count < 1 {
            return Err("no cuda device present".into());
        }
        Ok(count as usize)
    }

    pub fn mem_info(&self) -> CudaResult<(usize, usize)> {
        let f = self.fns.mem_get_info.ok_or_else(|| "cudaMemGetInfo unavailable".to_string())?;
        let mut free: usize = 0;
        let mut total: usize = 0;
        let rc = unsafe { f(&mut free, &mut total) };
        if rc != 0 {
            return Err(format!("cudaMemGetInfo failed: rc={}", rc));
        }
        Ok((free, total))
    }

    pub fn store(&self, key: &[u8], payload: &[u8]) -> CudaResult<()> {
        let malloc = self.fns.malloc.ok_or_else(|| "cudaMalloc unavailable".to_string())?;
        let memcpy = self.fns.memcpy.ok_or_else(|| "cudaMemcpy unavailable".to_string())?;
        let mut ptr: DevPtr = std::ptr::null_mut();
        let n = payload.len().max(1);
        let rc = unsafe { malloc(&mut ptr, n) };
        if rc != 0 {
            return Err(format!("cudaMalloc failed: rc={}", rc));
        }
        if !payload.is_empty() {
            let rc = unsafe { memcpy(ptr, payload.as_ptr() as *const c_void, payload.len(), CUDA_MEMCPY_HOST_TO_DEVICE) };
            if rc != 0 {
                unsafe { self.fns.free.unwrap()(ptr) };
                return Err(format!("cudaMemcpy H2D failed: rc={}", rc));
            }
        }
        let mut map = self.device_allocs.lock().map_err(|_| "alloc map lock poisoned".to_string())?;
        if let Some((old, _)) = map.remove(key) {
            unsafe { self.fns.free.unwrap()(old) };
        }
        map.insert(key.to_vec(), (ptr, payload.len()));
        Ok(())
    }

    pub fn load(&self, key: &[u8]) -> CudaResult<Vec<u8>> {
        let memcpy = self.fns.memcpy.ok_or_else(|| "cudaMemcpy unavailable".to_string())?;
        let map = self.device_allocs.lock().map_err(|_| "alloc map lock poisoned".to_string())?;
        let (ptr, n) = map.get(key).ok_or_else(|| "device object not present".to_string()).map(|(p, n)| (*p, *n))?;
        let mut out = vec![0u8; n];
        if n > 0 {
            let rc = unsafe { memcpy(out.as_mut_ptr() as *mut c_void, ptr, n, CUDA_MEMCPY_DEVICE_TO_HOST) };
            if rc != 0 {
                return Err(format!("cudaMemcpy D2H failed: rc={}", rc));
            }
        }
        Ok(out)
    }

    pub fn remove(&self, key: &[u8]) -> CudaResult<u64> {
        let free = self.fns.free.ok_or_else(|| "cudaFree unavailable".to_string())?;
        let mut map = self.device_allocs.lock().map_err(|_| "alloc map lock poisoned".to_string())?;
        if let Some((ptr, n)) = map.remove(key) {
            let rc = unsafe { free(ptr) };
            if rc != 0 {
                return Err(format!("cudaFree failed: rc={}", rc));
            }
            Ok(n as u64)
        } else {
            Ok(0)
        }
    }

    pub fn exists(&self, key: &[u8]) -> bool {
        self.device_allocs.lock().map(|m| m.contains_key(key)).unwrap_or(false)
    }
}

impl Drop for Cuda {
    fn drop(&mut self) {
        if let Some(free) = self.fns.free {
            if let Ok(map) = self.device_allocs.get_mut() {
                for (_, (ptr, _)) in map.iter_mut() {
                    unsafe { free(*ptr) };
                }
                map.clear();
            }
        }
    }
}

fn load_cudart() -> CudaResult<LibHandle> {
    #[cfg(windows)]
    {
        let names = [
            "cudart64_13.dll",
            "cudart64_12.dll",
            "cudart64_11.dll",
            "cudart64.dll",
        ];
        for n in names {
            let w = wide(n);
            let h = unsafe { LoadLibraryW(w.as_ptr()) };
            if !h.is_null() {
                return Ok(LibHandle::Windows(h as isize));
            }
            if let Ok(path) = std::env::var("CUDA_PATH") {
                let full = format!("{}\\bin\\{}", path, n);
                let wf = wide(&full);
                let h2 = unsafe { LoadLibraryW(wf.as_ptr()) };
                if !h2.is_null() {
                    return Ok(LibHandle::Windows(h2 as isize));
                }
            }
        }
        Err("cannot load cudart64*.dll (is CUDA installed and on PATH?)".into())
    }
    #[cfg(not(windows))]
    {
        Err("CUDA dynamic loading is only implemented on Windows in this build".into())
    }
}

#[cfg(windows)]
fn wide(s: &str) -> Vec<u16> {
    use std::os::windows::ffi::OsStrExt;
    std::ffi::OsStr::new(s).encode_wide().chain(std::iter::once(0u16)).collect()
}

#[cfg(windows)]
extern "system" {
    fn LoadLibraryW(name: *const u16) -> *mut c_void;
    fn GetProcAddress(module: isize, name: *const u8) -> *mut c_void;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cuda_load_or_reports_unavailable() {
        let result = Cuda::open();
        if let Ok(c) = result {
            let n = c.device_count().unwrap_or(0);
            assert!(n >= 1);
            c.store(b"k", b"hello").unwrap();
            assert_eq!(c.load(b"k").unwrap(), b"hello");
            assert!(c.exists(b"k"));
            assert_eq!(c.remove(b"k").unwrap(), 5);
            assert!(!c.exists(b"k"));
        }
    }
}