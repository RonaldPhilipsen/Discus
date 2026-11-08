//! Leader-follower mode (spec section 10.1).

use std::sync::Arc;

use discus_resp::{Command, Value};
use discus_wire::Regime;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use super::budget::{Budget, Tracked};
use super::state::{self, CounterState, Limits};
use super::Refusal;

struct Core {
    state: Tracked,
    /// The largest stamp so far. Stamps never run backwards within one regime.
    last_stamp: u64,
}

/// The leader of one group in leader-follower mode.
pub struct LfLeader {
    pub regime: Regime,
    core: Mutex<Core>,
    stop: CancellationToken,
}

impl LfLeader {
    pub fn new(regime: Regime, state: CounterState, budget: &Arc<Budget>) -> Arc<LfLeader> {
        Arc::new(LfLeader {
            regime,
            core: Mutex::new(Core {
                last_stamp: state.clock_ms(),
                state: Tracked::new(state, budget.clone()),
            }),
            stop: CancellationToken::new(),
        })
    }

    /// The leader serves.
    pub fn serving(&self) -> bool {
        !self.stop.is_cancelled()
    }

    pub fn stop(&self) {
        self.stop.cancel();
    }

    pub fn state(&self) -> CounterState {
        self.core.lock().state.get().clone()
    }

    /// Run one command. A read runs at `now_ms` without a stamp. A write takes the next stamp.
    pub async fn execute(
        &self,
        cmd: &Command,
        now_ms: u64,
        limits: &Limits,
    ) -> Result<Value, Refusal> {
        if !self.serving() {
            return Err(Refusal::NotLeader);
        }
        let mut core = self.core.lock();
        if state::is_read(cmd) {
            return Ok(core.state.get().read(now_ms, cmd));
        }
        let stamp = now_ms.max(core.last_stamp);
        core.last_stamp = stamp;
        Ok(core.state.apply(stamp, cmd, limits))
    }
}
