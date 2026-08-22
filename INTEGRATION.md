# Integration

KV Fabric is engine- and backend-independent. It does not depend on vLLM, SGLang, TensorRT-LLM, llama.cpp, or any specific model family.

## Engine adapter boundary

An inference engine integrates through an adapter that describes its cache layout, reports compatibility identity, exports/imports KV blocks, reports token ranges, and commits reusable prefixes. This repository ships a reference simulation adapter (the runtime facade itself) that is sufficient for end-to-end testing; it is the path used by the seven examples and the integration tests.

## Accelerator backends

`accelerator::DeviceStore` is the device abstraction. The default backend is a CPU memory store (`CpuDeviceStore`), which is a genuine allocation + bounded-capacity + integrity path. The `cuda` feature provides `CudaDeviceStore` on top of the `kvfabric-ffi` CUDA loader, which dynamically loads the CUDA Runtime API (no link-time CUDA dependency) and performs real device allocation, host-device transfer and free. HIP, Level Zero, Metal and Vulkan can implement the same trait; they are not required for 1.0.0.

## Tier backends

`storage::TierStore` is the tier abstraction. `HostStore` is the in-memory host tier; `StorageStore` is the persistent filesystem tier with atomic commits. FlashTier-style residency can be added by implementing the trait.

## Broader stack

+ **FlashTier** for physical residency.
+ **Context Fabric** for reusable computational-state identity/lineage/ownership/migration.
+ **Compute Fabric** for locality-aware execution placement.
+ **Reclaim Fabric** for retention economics and lifecycle decisions.
+ **Checkpoint Fabric** for durable recovery of execution-relevant state.

KV Fabric is a specialized vertical runtime applying those principles to reusable KV and prefix state; it is independently useful but leaves clean integration seams for the rest of the stack.