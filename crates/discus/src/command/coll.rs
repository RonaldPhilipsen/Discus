//! Typed access to collection values under the shard lock.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use discus_store::{Entry, Value as Stored, ZSet};

use super::error::CommandError;
use super::util::admit;
use crate::node::Node;

/// A collection type that a key can hold.
pub trait Collection: Default {
    fn from_mut(v: &mut Stored) -> Option<&mut Self>;
    fn from_ref(v: &Stored) -> Option<&Self>;
    fn into_stored(self) -> Stored;
    fn count(&self) -> usize;
}

impl Collection for HashMap<Bytes, Bytes> {
    fn from_mut(v: &mut Stored) -> Option<&mut Self> {
        if let Stored::Hash(h) = v {
            Some(h)
        } else {
            None
        }
    }
    fn from_ref(v: &Stored) -> Option<&Self> {
        if let Stored::Hash(h) = v {
            Some(h)
        } else {
            None
        }
    }
    fn into_stored(self) -> Stored {
        Stored::Hash(self)
    }
    fn count(&self) -> usize {
        self.len()
    }
}

impl Collection for HashSet<Bytes> {
    fn from_mut(v: &mut Stored) -> Option<&mut Self> {
        if let Stored::Set(s) = v {
            Some(s)
        } else {
            None
        }
    }
    fn from_ref(v: &Stored) -> Option<&Self> {
        if let Stored::Set(s) = v {
            Some(s)
        } else {
            None
        }
    }
    fn into_stored(self) -> Stored {
        Stored::Set(self)
    }
    fn count(&self) -> usize {
        self.len()
    }
}

impl Collection for ZSet {
    fn from_mut(v: &mut Stored) -> Option<&mut Self> {
        if let Stored::ZSet(z) = v {
            Some(z)
        } else {
            None
        }
    }
    fn from_ref(v: &Stored) -> Option<&Self> {
        if let Stored::ZSet(z) = v {
            Some(z)
        } else {
            None
        }
    }
    fn into_stored(self) -> Stored {
        Stored::ZSet(self)
    }
    fn count(&self) -> usize {
        self.len()
    }
}

/// Mutate the collection at `key`. A missing key gives `f` an empty collection, and a collection
/// that ends up empty is deleted. A key of another type gives `WrongType`.
///
/// `f` must not change the collection when it returns an error: the key then counts as
/// unchanged, keeps its version, and does not replicate.
///
/// `grow_bytes` is the payload that the call can add. It is admitted against `maxmemory` first.
pub fn mutate<C: Collection, R>(
    node: &Node,
    key: &[u8],
    grow_bytes: usize,
    f: impl FnOnce(&mut C) -> Result<R, CommandError>,
) -> Result<R, CommandError> {
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
        let result = f(coll);
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
pub fn guard(node: &Node, members_after: usize) -> Result<(), CommandError> {
    if members_after > node.config.max_collection_members {
        return Err(CommandError::Custom("ERR collection too large".into()));
    }
    Ok(())
}
