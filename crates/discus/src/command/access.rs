//! The read or write class of every command, for scope checks (spec §14.3).

/// What a command needs from the scopes of a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// Runs before authentication: `QUIT`, `AUTH`, and `HELLO`.
    Public,
    /// No data access. Authentication is enough.
    Connection,
    /// Reads data or node state. Needs `discus.read` or `discus.write`.
    Read,
    /// Changes data. Needs `discus.write`.
    Write,
}

/// The class of a command by its name. An unknown name is a write, so a command that a later
/// change forgets to classify fails closed for read-only clients.
pub fn access(name: &[u8]) -> Access {
    super::lookup(name).map_or(Access::Write, |spec| spec.access)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_reads_and_set_writes() {
        assert_eq!(access(b"GET"), Access::Read);
        assert_eq!(access(b"SET"), Access::Write);
        assert_eq!(access(b"PING"), Access::Connection);
        assert_eq!(access(b"NEWCOMMAND"), Access::Write);
    }
}
