//! The member model of an epoch.

use std::net::SocketAddr;

use crate::NodeId;

/// The life cycle state of a member. Only `Normal` members own keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MemberState {
    Joining,
    Normal,
    Leaving,
    Dead,
}

impl MemberState {
    pub fn name(self) -> &'static str {
        match self {
            MemberState::Joining => "JOINING",
            MemberState::Normal => "NORMAL",
            MemberState::Leaving => "LEAVING",
            MemberState::Dead => "DEAD",
        }
    }
}

/// A placement weight: finite and greater than zero. Every weight starts at 1.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Weight(f64);

impl Weight {
    pub fn new(value: f64) -> Option<Weight> {
        (value.is_finite() && value > 0.0).then_some(Weight(value))
    }

    pub fn get(self) -> f64 {
        self.0
    }
}

impl Default for Weight {
    fn default() -> Weight {
        Weight(1.0)
    }
}

/// One member of an epoch.
#[derive(Debug, Clone, PartialEq)]
pub struct Member {
    pub id: NodeId,
    pub addr: SocketAddr,
    pub state: MemberState,
    pub weight: Weight,
}
