//! ACL users: the rules of one user and the check that applies them.

mod user;

pub use user::{CommandSet, Denied, KeyPattern, User};

use sha2::{Digest, Sha256};

/// The SHA-256 of a password, the only form in which the ACL keeps one.
pub fn sha256(password: &[u8]) -> [u8; 32] {
    Sha256::digest(password).into()
}
