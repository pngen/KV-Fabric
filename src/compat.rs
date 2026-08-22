//! Canonical compatibility identity.
//!
//! A reusable prefix is only reusable if the state that produced it is
//! provably compatible with the new request. A false compatibility hit is a
//! correctness failure, so the compatibility key is computed over a canonical,
//! versioned, unambiguous encoding of every field that can change the meaning
//! of tokens or the layout of cache state.

use crate::hash::CanonicalHasher;

/// The version of the canonical compatibility-key encoding. Bump whenever the
/// set of fields or their framing changes so that old and new keys never match
/// by accident.
pub const COMPAT_KEY_VERSION: u32 = 1;

/// All fields that determine whether KV state for one request is reusable for
/// another. Missing/unset fields are encoded as empty strings so that a field
/// being absent never matches a field being a different value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilitySpec {
    pub model_identity: String,
    pub model_revision: String,
    pub tokenizer_identity: String,
    pub tokenizer_config: String,
    pub engine_abi: String,
    pub engine_version: String,
    pub rope_config: String,
    pub attention_config: String,
    pub dtype: String,
    pub quant_mode: String,
    pub cache_layout: String,
    pub multimodal_preprocess: String,
}

impl Default for CompatibilitySpec {
    fn default() -> Self {
        CompatibilitySpec {
            model_identity: String::new(),
            model_revision: String::new(),
            tokenizer_identity: String::new(),
            tokenizer_config: String::new(),
            engine_abi: String::new(),
            engine_version: String::new(),
            rope_config: String::new(),
            attention_config: String::new(),
            dtype: String::new(),
            quant_mode: String::new(),
            cache_layout: String::new(),
            multimodal_preprocess: String::new(),
        }
    }
}

impl CompatibilitySpec {
    /// Compute the canonical 256-bit compatibility key.
    pub fn key(&self) -> [u8; 32] {
        let mut h = CanonicalHasher::new();
        h.version(COMPAT_KEY_VERSION);
        h.str(1, &self.model_identity);
        h.str(2, &self.model_revision);
        h.str(3, &self.tokenizer_identity);
        h.str(4, &self.tokenizer_config);
        h.str(5, &self.engine_abi);
        h.str(6, &self.engine_version);
        h.str(7, &self.rope_config);
        h.str(8, &self.attention_config);
        h.str(9, &self.dtype);
        h.str(10, &self.quant_mode);
        h.str(11, &self.cache_layout);
        h.str(12, &self.multimodal_preprocess);
        h.digest()
    }

    pub fn key_hex(&self) -> String {
        crate::hash::hex_encode(&self.key())
    }

    /// Human-readable short descriptor for telemetry/logging.
    pub fn summary(&self) -> String {
        format!(
            "model={} rev={} tok={} eng={} v={}",
            self.model_identity,
            self.model_revision,
            self.tokenizer_identity,
            self.engine_abi,
            self.engine_version
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> CompatibilitySpec {
        CompatibilitySpec {
            model_identity: "llama".into(),
            model_revision: "3.1-8b".into(),
            tokenizer_identity: "llama3".into(),
            engine_abi: "vllm".into(),
            ..Default::default()
        }
    }

    #[test]
    fn key_is_deterministic() {
        assert_eq!(base().key(), base().key());
    }

    #[test]
    fn different_field_changes_key() {
        let a = base();
        let mut b = base();
        b.model_identity = "gemma".into();
        assert_ne!(a.key(), b.key());
    }

    #[test]
    fn missing_vs_empty_do_not_collide() {
        let mut a = base();
        a.dtype = "".into();
        let mut b = base();
        b.dtype = "float16".into();
        assert_ne!(a.key(), b.key());
        let mut c = base();
        c.multimodal_preprocess = "x".into();
        assert_ne!(a.key(), c.key());
    }
}
