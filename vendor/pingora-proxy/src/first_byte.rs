// Copyright 2026 Cloudflare, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The first-byte deadline cell.
//!
//! (maxllm local change — see vendor/README.md.)
//!
//! Pingora's `read_timeout` is a *per read* budget: it catches an upstream
//! that goes completely quiet, but it says nothing about how long the
//! upstream may take to *start* answering. A caller that wants a deadline
//! for the first byte can only decide it once it has read the request
//! body — that is when it knows whether the answer will arrive as a
//! stream (whose first byte comes quickly) or as one document (whose first
//! byte is the whole answer) — while the proxy has to arm the deadline
//! before it reads the upstream's first response byte. The two halves of
//! the proxy meet at the request body filter, so the decision travels
//! through this cell.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;

/// The first-byte deadline of one upstream attempt.
///
/// The outer `Option` says whether the deadline has been decided yet:
/// `None` means "not decided", `Some(None)` means "decided: no deadline"
/// and `Some(Some(d))` means "decided: the upstream has `d` to produce its
/// first byte".
#[derive(Clone, Default)]
pub struct FirstByteBudget {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    decided: Mutex<Option<Option<Duration>>>,
    notify: Notify,
}

impl FirstByteBudget {
    /// A fresh, undecided budget.
    pub fn new() -> Self {
        Self::default()
    }

    /// Decide the deadline. `None` means "no deadline".
    ///
    /// The first decision wins: the caller decides once per request, and a
    /// failover re-uses that decision for the next attempt.
    pub fn set(&self, budget: Option<Duration>) {
        {
            let mut slot = self.inner.decided.lock().unwrap_or_else(|e| e.into_inner());
            if slot.is_some() {
                return;
            }
            *slot = Some(budget);
        }
        self.inner.notify.notify_waiters();
    }

    /// The decision, if it has been made.
    pub fn get(&self) -> Option<Option<Duration>> {
        *self.inner.decided.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Wait until the deadline is decided.
    ///
    /// Cancel-safe: the waiter registers itself before it looks at the
    /// decision, so a decision made while the wait is being cancelled is
    /// never missed.
    pub async fn wait(&self) -> Option<Duration> {
        loop {
            let notified = self.inner.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(decided) = self.get() {
                return decided;
            }
            notified.await;
        }
    }
}
