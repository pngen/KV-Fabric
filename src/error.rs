//! Error type for KV Fabric. All failures are explicit; there are no silent
//! fallbacks and no "boolean false for a complex failure". Every operation
//! returns a `KvResult` carrying a structured `KvError` with a stable code.

use std::fmt;

/// A structured error with a stable machine-readable code and a human message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvError {
    /// Stable category code, used by the CLI and by recovery logic.
    pub kind: ErrorKind,
    /// Human-readable detail.
    pub message: String,
}

/// Stable error categories. The discriminant is part of the wire/CLI contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum ErrorKind {
    InvalidFormat = 1,
    InvalidArgument = 2,
    NotFound = 3,
    AlreadyExists = 4,
    Corrupt = 5,
    Integrity = 6,
    Capacity = 7,
    QuotaExceeded = 8,
    Unauthorized = 9,
    StaleLease = 10,
    StaleGeneration = 11,
    StaleAuthority = 12,
    Conflict = 13,
    Io = 14,
    Network = 15,
    Unsupported = 16,
    Protocol = 17,
    Timeout = 18,
    Internal = 19,
    Rejected = 20,
    NotReady = 21,
    Busy = 22,
}

impl ErrorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorKind::InvalidFormat => "invalid_format",
            ErrorKind::InvalidArgument => "invalid_argument",
            ErrorKind::NotFound => "not_found",
            ErrorKind::AlreadyExists => "already_exists",
            ErrorKind::Corrupt => "corrupt",
            ErrorKind::Integrity => "integrity",
            ErrorKind::Capacity => "capacity",
            ErrorKind::QuotaExceeded => "quota_exceeded",
            ErrorKind::Unauthorized => "unauthorized",
            ErrorKind::StaleLease => "stale_lease",
            ErrorKind::StaleGeneration => "stale_generation",
            ErrorKind::StaleAuthority => "stale_authority",
            ErrorKind::Conflict => "conflict",
            ErrorKind::Io => "io",
            ErrorKind::Network => "network",
            ErrorKind::Unsupported => "unsupported",
            ErrorKind::Protocol => "protocol",
            ErrorKind::Timeout => "timeout",
            ErrorKind::Internal => "internal",
            ErrorKind::Rejected => "rejected",
            ErrorKind::NotReady => "not_ready",
            ErrorKind::Busy => "busy",
        }
    }

    pub fn from_u32(v: u32) -> ErrorKind {
        match v {
            1 => ErrorKind::InvalidFormat,
            2 => ErrorKind::InvalidArgument,
            3 => ErrorKind::NotFound,
            4 => ErrorKind::AlreadyExists,
            5 => ErrorKind::Corrupt,
            6 => ErrorKind::Integrity,
            7 => ErrorKind::Capacity,
            8 => ErrorKind::QuotaExceeded,
            9 => ErrorKind::Unauthorized,
            10 => ErrorKind::StaleLease,
            11 => ErrorKind::StaleGeneration,
            12 => ErrorKind::StaleAuthority,
            13 => ErrorKind::Conflict,
            14 => ErrorKind::Io,
            15 => ErrorKind::Network,
            16 => ErrorKind::Unsupported,
            17 => ErrorKind::Protocol,
            18 => ErrorKind::Timeout,
            19 => ErrorKind::Internal,
            20 => ErrorKind::Rejected,
            21 => ErrorKind::NotReady,
            22 => ErrorKind::Busy,
            _ => ErrorKind::Internal,
        }
    }
}

impl KvError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> KvError {
        KvError { kind, message: message.into() }
    }

    pub fn invalid_arg(msg: impl Into<String>) -> KvError {
        KvError::new(ErrorKind::InvalidArgument, msg)
    }
    pub fn invalid_format(msg: impl Into<String>) -> KvError {
        KvError::new(ErrorKind::InvalidFormat, msg)
    }
    pub fn not_found(msg: impl Into<String>) -> KvError {
        KvError::new(ErrorKind::NotFound, msg)
    }
    pub fn already_exists(msg: impl Into<String>) -> KvError {
        KvError::new(ErrorKind::AlreadyExists, msg)
    }
    pub fn corrupt(msg: impl Into<String>) -> KvError {
        KvError::new(ErrorKind::Corrupt, msg)
    }
    pub fn integrity(msg: impl Into<String>) -> KvError {
        KvError::new(ErrorKind::Integrity, msg)
    }
    pub fn capacity(msg: impl Into<String>) -> KvError {
        KvError::new(ErrorKind::Capacity, msg)
    }
    pub fn quota(namespace: impl Into<String>, msg: impl Into<String>) -> KvError {
        KvError::new(ErrorKind::QuotaExceeded, format!("namespace '{}': {}", namespace.into(), msg.into()))
    }
    pub fn unauthorized(msg: impl Into<String>) -> KvError {
        KvError::new(ErrorKind::Unauthorized, msg)
    }
    pub fn stale_lease(msg: impl Into<String>) -> KvError {
        KvError::new(ErrorKind::StaleLease, msg)
    }
    pub fn stale_generation(msg: impl Into<String>) -> KvError {
        KvError::new(ErrorKind::StaleGeneration, msg)
    }
    pub fn stale_authority(msg: impl Into<String>) -> KvError {
        KvError::new(ErrorKind::StaleAuthority, msg)
    }
    pub fn conflict(msg: impl Into<String>) -> KvError {
        KvError::new(ErrorKind::Conflict, msg)
    }
    pub fn network(msg: impl Into<String>) -> KvError {
        KvError::new(ErrorKind::Network, msg)
    }
    pub fn unsupported(msg: impl Into<String>) -> KvError {
        KvError::new(ErrorKind::Unsupported, msg)
    }
    pub fn protocol(msg: impl Into<String>) -> KvError {
        KvError::new(ErrorKind::Protocol, msg)
    }
    pub fn timeout(msg: impl Into<String>) -> KvError {
        KvError::new(ErrorKind::Timeout, msg)
    }
    pub fn io(msg: impl Into<String>) -> KvError {
        KvError::new(ErrorKind::Io, msg)
    }
    pub fn internal(msg: impl Into<String>) -> KvError {
        KvError::new(ErrorKind::Internal, msg)
    }
    pub fn rejected(msg: impl Into<String>) -> KvError {
        KvError::new(ErrorKind::Rejected, msg)
    }
    pub fn not_ready(msg: impl Into<String>) -> KvError {
        KvError::new(ErrorKind::NotReady, msg)
    }
    pub fn busy(msg: impl Into<String>) -> KvError {
        KvError::new(ErrorKind::Busy, msg)
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Convert to a stable 4-byte code that can travel over the wire and back.
    pub fn to_wire(&self) -> (u32, String) {
        (self.kind as u32, self.message.clone())
    }

    pub fn from_wire(code: u32, message: String) -> KvError {
        KvError::new(ErrorKind::from_u32(code), message)
    }
}

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.kind.as_str(), self.message)
    }
}

impl std::error::Error for KvError {}

impl From<std::io::Error> for KvError {
    fn from(e: std::io::Error) -> KvError {
        KvError::new(ErrorKind::Io, e.to_string())
    }
}

impl From<std::num::ParseIntError> for KvError {
    fn from(e: std::num::ParseIntError) -> KvError {
        KvError::invalid_arg(format!("parse error: {}", e))
    }
}

impl From<std::net::AddrParseError> for KvError {
    fn from(e: std::net::AddrParseError) -> KvError {
        KvError::invalid_arg(format!("address parse error: {}", e))
    }
}

impl From<std::io::ErrorKind> for KvError {
    fn from(e: std::io::ErrorKind) -> KvError {
        KvError::io(format!("io error kind: {:?}", e))
    }
}

impl From<std::convert::Infallible> for KvError {
    fn from(_: std::convert::Infallible) -> KvError {
        KvError::internal("infallible")
    }
}

/// Result alias used throughout KV Fabric.
pub type KvResult<T> = Result<T, KvError>;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn wire_roundtrip() {
        let e = KvError::stale_authority("epoch mismatch");
        let (c, m) = e.to_wire();
        let back = KvError::from_wire(c, m);
        assert_eq!(e, back);
        assert_eq!(back.kind(), ErrorKind::StaleAuthority);
    }
    #[test]
    fn kind_str_is_stable() {
        assert_eq!(ErrorKind::StaleAuthority.as_str(), "stale_authority");
        assert_eq!(ErrorKind::QuotaExceeded.as_str(), "quota_exceeded");
    }
}
