//! The ACL command categories of Valkey, such as `@read` and `@dangerous`.

/// A set of command categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Categories(u32);

impl Categories {
    pub const EMPTY: Categories = Categories(0);
    pub const KEYSPACE: Categories = Categories(1 << 0);
    pub const READ: Categories = Categories(1 << 1);
    pub const WRITE: Categories = Categories(1 << 2);
    pub const SET: Categories = Categories(1 << 3);
    pub const SORTEDSET: Categories = Categories(1 << 4);
    pub const LIST: Categories = Categories(1 << 5);
    pub const HASH: Categories = Categories(1 << 6);
    pub const STRING: Categories = Categories(1 << 7);
    pub const BITMAP: Categories = Categories(1 << 8);
    pub const HYPERLOGLOG: Categories = Categories(1 << 9);
    pub const GEO: Categories = Categories(1 << 10);
    pub const STREAM: Categories = Categories(1 << 11);
    pub const PUBSUB: Categories = Categories(1 << 12);
    pub const ADMIN: Categories = Categories(1 << 13);
    pub const FAST: Categories = Categories(1 << 14);
    pub const SLOW: Categories = Categories(1 << 15);
    pub const BLOCKING: Categories = Categories(1 << 16);
    pub const DANGEROUS: Categories = Categories(1 << 17);
    pub const CONNECTION: Categories = Categories(1 << 18);
    pub const TRANSACTION: Categories = Categories(1 << 19);
    pub const SCRIPTING: Categories = Categories(1 << 20);

    /// Every category with its name in lower case and without the `@`, in the order of Valkey.
    pub const NAMES: &'static [(&'static str, Categories)] = &[
        ("keyspace", Categories::KEYSPACE),
        ("read", Categories::READ),
        ("write", Categories::WRITE),
        ("set", Categories::SET),
        ("sortedset", Categories::SORTEDSET),
        ("list", Categories::LIST),
        ("hash", Categories::HASH),
        ("string", Categories::STRING),
        ("bitmap", Categories::BITMAP),
        ("hyperloglog", Categories::HYPERLOGLOG),
        ("geo", Categories::GEO),
        ("stream", Categories::STREAM),
        ("pubsub", Categories::PUBSUB),
        ("admin", Categories::ADMIN),
        ("fast", Categories::FAST),
        ("slow", Categories::SLOW),
        ("blocking", Categories::BLOCKING),
        ("dangerous", Categories::DANGEROUS),
        ("connection", Categories::CONNECTION),
        ("transaction", Categories::TRANSACTION),
        ("scripting", Categories::SCRIPTING),
    ];

    pub const fn union(self, other: Categories) -> Categories {
        Categories(self.0 | other.0)
    }

    pub const fn intersects(self, other: Categories) -> bool {
        self.0 & other.0 != 0
    }

    /// The category of `name`, ignoring case. `name` has no `@`.
    pub fn parse(name: &str) -> Option<Categories> {
        Self::NAMES
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|&(_, c)| c)
    }

    /// The names of the categories in this set, in the order of `NAMES`.
    pub fn names(self) -> impl Iterator<Item = &'static str> {
        Self::NAMES
            .iter()
            .filter(move |(_, c)| self.intersects(*c))
            .map(|&(n, _)| n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_parse_and_print() {
        assert_eq!(Categories::NAMES.len(), 21);
        assert_eq!(Categories::parse("Dangerous"), Some(Categories::DANGEROUS));
        assert_eq!(Categories::parse("all"), None);
        let c = Categories::STRING
            .union(Categories::WRITE)
            .union(Categories::SLOW);
        assert_eq!(c.names().collect::<Vec<_>>(), ["write", "string", "slow"]);
    }
}
