//! ACL users: the rules of one user and the check that applies them.

mod parse;
mod user;

pub use user::{Denied, KeyPattern, User};

/// The user of a login without a name, and of a connection without a user in `ACL WHOAMI`.
pub const DEFAULT_USER: &str = "default";

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::Arc;

use sha2::{Digest, Sha256};

/// The SHA-256 of a password, the only form in which the ACL keeps one.
pub fn sha256(password: &[u8]) -> [u8; 32] {
    Sha256::digest(password).into()
}

/// The users of one ACL file.
#[derive(Debug, Default)]
pub struct Acl {
    users: HashMap<String, Arc<User>>,
}

impl Acl {
    /// Parse the text of an ACL file. The error starts with `line <n>: ` when a line causes it.
    pub fn parse(text: &str) -> Result<Acl, String> {
        parse::parse(text)
    }

    /// Read and parse the file. A parse error has the kind `InvalidData`.
    pub fn load(path: &Path) -> io::Result<Acl> {
        let text = std::fs::read_to_string(path)?;
        Acl::parse(&text).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// The user with this exact name. Names are case-sensitive, as in Valkey.
    pub fn user(&self, name: &[u8]) -> Option<&Arc<User>> {
        self.users.get(std::str::from_utf8(name).ok()?)
    }

    /// Every user, sorted by name.
    pub fn users(&self) -> Vec<&Arc<User>> {
        let mut users: Vec<&Arc<User>> = self.users.values().collect();
        users.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        users
    }
}
