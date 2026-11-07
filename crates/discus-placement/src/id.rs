//! Random 128-bit identities: [`NodeId`] for a process and [`ClusterId`] for a cluster.

use std::fmt;
use std::str::FromStr;

/// The text of an ID is not 32 hexadecimal digits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseIdError;

impl fmt::Display for ParseIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an ID is 32 hexadecimal digits")
    }
}

impl std::error::Error for ParseIdError {}

/// 128 random bits from the operating system. If the operating system refuses, fall back to the
/// process-local generator, which is still unique enough for an identity.
fn random_u128() -> u128 {
    let mut bytes = [0u8; 16];
    if getrandom::fill(&mut bytes).is_ok() {
        u128::from_be_bytes(bytes)
    } else {
        fastrand::u128(..)
    }
}

macro_rules! id_type {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
        pub struct $name(u128);

        impl $name {
            /// The all-zero ID. No random ID is ever this value in practice.
            pub const NIL: $name = $name(0);

            pub const fn from_u128(raw: u128) -> $name {
                $name(raw)
            }

            pub const fn as_u128(self) -> u128 {
                self.0
            }

            /// The 16 big-endian bytes. Placement hashes these bytes.
            pub const fn to_be_bytes(self) -> [u8; 16] {
                self.0.to_be_bytes()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{:032x}", self.0)
            }
        }

        impl FromStr for $name {
            type Err = ParseIdError;

            fn from_str(s: &str) -> Result<$name, ParseIdError> {
                if s.len() != 32 || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
                    return Err(ParseIdError);
                }
                u128::from_str_radix(s, 16).map($name).map_err(|_| ParseIdError)
            }
        }
    };
}

id_type!(
    /// A node identity. A process generates one at start. A restarted process is a new node.
    NodeId
);

id_type!(
    /// A cluster identity. The bootstrap node mints it. Epochs of different clusters are never
    /// compared.
    ClusterId
);

impl NodeId {
    /// A fresh random identity. It is never [`NodeId::NIL`].
    pub fn random() -> NodeId {
        loop {
            let id = random_u128();
            if id != 0 {
                return NodeId(id);
            }
        }
    }
}

impl ClusterId {
    /// A fresh random cluster identity.
    pub fn mint() -> ClusterId {
        ClusterId(random_u128())
    }
}
