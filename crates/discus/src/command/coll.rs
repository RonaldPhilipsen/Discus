//! Typed access to collection values under the shard lock.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use discus_store::{Entry, Value as Stored, ZSet};

use super::error::CommandError;
use super::util::{admit, check_key};
use crate::node::Node;

/// A collection type that a key can hold.
pub trait Collection: Default {
    fn from_mut(v: &mut Stored) -> Option<&mut Self>;
    fn from_ref(v: &Stored) -> Option<&Self>;
    fn into_stored(self) -> Stored;
    fn count(&self) -> usize;
    fn contains(&self, member: &[u8]) -> bool;
}

macro_rules! collection {
    ($ty:ty, $variant:ident, |$c:ident, $m:ident| $contains:expr) => {
        impl Collection for $ty {
            fn from_mut(v: &mut Stored) -> Option<&mut Self> {
                match v {
                    Stored::$variant(c) => Some(c),
                    _ => None,
                }
            }
            fn from_ref(v: &Stored) -> Option<&Self> {
                match v {
                    Stored::$variant(c) => Some(c),
                    _ => None,
                }
            }
            fn into_stored(self) -> Stored {
                Stored::$variant(self)
            }
            fn count(&self) -> usize {
                self.len()
            }
            fn contains(&self, $m: &[u8]) -> bool {
                let $c = self;
                $contains
            }
        }
    };
}

collection!(HashMap<Bytes, Bytes>, Hash, |h, m| h.contains_key(m));
collection!(HashSet<Bytes>, Set, |s, m| s.contains(m));
collection!(ZSet, ZSet, |z, m| z.score(m).is_some());

/// Mutate the collection at `key`. A missing key gives `f` an empty collection, and a collection
/// that ends up empty is deleted. A key of another type gives `WrongType`.
///
/// `f` must not change the collection when it returns an error: the key then counts as
/// unchanged, keeps its version, and does not replicate.
///
/// `grow_bytes` is the payload that the call can add. It is admitted against `maxmemory` first.
/// `adds` are the members that `f` may add. The distinct ones that are absent must fit under the
/// member limit before `f` runs.
pub fn mutate<'a, C: Collection, R>(
    node: &Node,
    key: &Bytes,
    grow_bytes: usize,
    adds: impl IntoIterator<Item = &'a Bytes>,
    f: impl FnOnce(&mut C) -> Result<R, CommandError>,
) -> Result<R, CommandError> {
    check_key(node, key)?;
    admit(node, key.len() + grow_bytes)?;
    let stamp = node.stamp()?;
    node.write(key, stamp, |slot| {
        let mut entry = slot
            .untracked()
            .take()
            .unwrap_or_else(|| Entry::new(C::default().into_stored(), None));
        let Some(coll) = C::from_mut(&mut entry.value) else {
            *slot.untracked() = Some(entry);
            return Err(CommandError::WrongType);
        };
        let fresh: HashSet<&Bytes> = adds.into_iter().filter(|m| !coll.contains(m)).collect();
        let result = guard(node, coll.count() + fresh.len()).and_then(|()| f(coll));
        let keep = coll.count() > 0;
        if keep {
            *slot.untracked() = Some(entry);
        }
        if result.is_ok() {
            slot.touch();
        }
        result
    })
}

/// Read the collection at `key`. A missing key gives `None`.
pub fn view<C: Collection, R>(
    node: &Node,
    key: &[u8],
    f: impl FnOnce(Option<&C>) -> R,
) -> Result<R, CommandError> {
    node.store.read(key, |e| match e {
        None => Ok(f(None)),
        Some(e) => C::from_ref(&e.value)
            .map(|c| f(Some(c)))
            .ok_or(CommandError::WrongType),
    })
}

/// Refuse to grow a collection past the configured member limit.
fn guard(node: &Node, members_after: usize) -> Result<(), CommandError> {
    if members_after > node.config.max_collection_members {
        return Err(CommandError::Custom("ERR collection too large".into()));
    }
    Ok(())
}
