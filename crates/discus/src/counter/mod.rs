//! Strong counters (spec section 10): keys under `--counter-prefix` served linearizably by
//! counter groups.

pub mod budget;
pub mod raft;
pub mod state;
