use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

use super::agent::{
    AgentCompatibilityProfile, AgentInvocationIdentity, AgentLifecycleMilestone, AgentObservation,
    AgentObservationEnvelope,
};
use super::agent_diagnostics::AgentDiagnosticSession;
use super::runtime::ActionId;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct InvocationUsage {
    pub(crate) input_tokens: u64,
    pub(crate) output_tokens: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NativeSessionFact {
    pub(crate) profile: AgentCompatibilityProfile,
    pub(crate) diagnostic_identity: Arc<str>,
    pub(crate) native_session_identity: Option<Arc<str>>,
}

#[derive(Clone, Default)]
pub(crate) struct InvocationAccountingLog {
    inner: Arc<Mutex<InvocationAccounting>>,
}

#[derive(Default)]
struct InvocationAccounting {
    usage: BTreeMap<ActionId, InvocationUsage>,
    message_usage: BTreeMap<ActionId, MessageUsage>,
    native_sessions: BTreeMap<ActionId, NativeSessionFact>,
}

#[derive(Default)]
struct MessageUsage {
    completed: InvocationUsage,
    active: bool,
    current: Option<InvocationUsage>,
}

impl InvocationUsage {
    fn saturating_add(self, other: Self) -> Self {
        Self {
            input_tokens: self.input_tokens.saturating_add(other.input_tokens),
            output_tokens: self.output_tokens.saturating_add(other.output_tokens),
        }
    }
}

impl InvocationAccountingLog {
    pub(crate) fn record_observation(&self, envelope: &AgentObservationEnvelope) {
        let invocation = envelope.invocation();
        let mut accounting = lock(&self.inner);
        let message_scoped = accounting
            .native_sessions
            .get(&invocation)
            .is_some_and(|session| {
                matches!(
                    session.profile,
                    AgentCompatibilityProfile::PiJsonV1
                        | AgentCompatibilityProfile::ClaudeCodeStreamJsonV1
                )
            });
        match envelope.observation() {
            AgentObservation::Lifecycle {
                milestone: AgentLifecycleMilestone::MessageStarted,
            } if message_scoped => {
                let usage = accounting.message_usage.entry(invocation).or_default();
                usage.active = true;
                usage.current = None;
            }
            AgentObservation::Lifecycle {
                milestone: AgentLifecycleMilestone::MessageCompleted,
            } if message_scoped => {
                let updated = accounting
                    .message_usage
                    .get_mut(&invocation)
                    .and_then(|usage| {
                        usage.active = false;
                        usage.current.take().map(|current| {
                            usage.completed = usage.completed.saturating_add(current);
                            usage.completed
                        })
                    });
                if let Some(updated) = updated {
                    accounting.usage.insert(invocation, updated);
                }
            }
            AgentObservation::Usage {
                input_tokens,
                output_tokens,
            } => {
                let observed = InvocationUsage {
                    input_tokens: *input_tokens,
                    output_tokens: *output_tokens,
                };
                let updated = if message_scoped {
                    accounting
                        .message_usage
                        .get_mut(&invocation)
                        .filter(|usage| usage.active)
                        .map(|usage| {
                            usage.current = Some(observed);
                            usage.completed.saturating_add(observed)
                        })
                        .unwrap_or(observed)
                } else {
                    observed
                };
                accounting.usage.insert(invocation, updated);
            }
            _ => {}
        }
    }

    pub(crate) fn record_native_session(
        &self,
        identity: &AgentInvocationIdentity,
        profile: AgentCompatibilityProfile,
        session: &AgentDiagnosticSession,
    ) {
        let diagnostic_identity = session
            .directory()
            .file_name()
            .and_then(|name| name.to_str())
            .map(Arc::from)
            .unwrap_or_else(|| Arc::from("unavailable"));
        let native_session_identity = match profile {
            AgentCompatibilityProfile::PiJsonV1 => session
                .pi_native_session_directory()
                .map(|_| Arc::clone(&diagnostic_identity)),
            AgentCompatibilityProfile::ClaudeCodeStreamJsonV1 => {
                session.claude_code_native_session_id().map(Arc::from)
            }
            AgentCompatibilityProfile::CodexAppServerV1 => None,
        };
        lock(&self.inner).native_sessions.insert(
            identity.invocation(),
            NativeSessionFact {
                profile,
                diagnostic_identity,
                native_session_identity,
            },
        );
    }

    pub(crate) fn usage(&self, invocation: ActionId) -> Option<InvocationUsage> {
        lock(&self.inner).usage.get(&invocation).copied()
    }

    pub(crate) fn native_session(&self, invocation: ActionId) -> Option<NativeSessionFact> {
        lock(&self.inner).native_sessions.get(&invocation).cloned()
    }

    pub(crate) fn recorded_invocations(&self) -> Vec<ActionId> {
        let accounting = lock(&self.inner);
        accounting
            .usage
            .keys()
            .chain(accounting.native_sessions.keys())
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect()
    }
}

fn lock(accounting: &Mutex<InvocationAccounting>) -> MutexGuard<'_, InvocationAccounting> {
    accounting
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::workflow::agent::{
        AgentLifecycleMilestone, AgentObservation, WorkflowRunId,
    };
    use crate::execution::workflow::runtime::{ActionId, TransitionSequence};

    fn action(value: u64) -> ActionId {
        ActionId {
            transition_sequence: TransitionSequence(value),
        }
    }

    fn observation(
        invocation: ActionId,
        sequence: u64,
        observation: AgentObservation,
    ) -> AgentObservationEnvelope {
        AgentObservationEnvelope::fixture(
            AgentInvocationIdentity::new(
                WorkflowRunId::from(Arc::from("run")),
                Arc::from("step"),
                invocation,
            ),
            sequence,
            observation,
        )
    }

    #[test]
    fn cumulative_usage_updates_are_not_double_counted() {
        let log = InvocationAccountingLog::default();
        let invocation = action(1);

        // Admitted adapters emit cumulative snapshots for an invocation. A later snapshot
        // replaces the earlier observation instead of representing another billable call.
        for (input_tokens, output_tokens) in [(10, 0), (10, 4)] {
            log.record_observation(&observation(
                invocation,
                1,
                AgentObservation::Usage {
                    input_tokens,
                    output_tokens,
                },
            ));
        }

        assert_eq!(
            log.usage(invocation),
            Some(InvocationUsage {
                input_tokens: 10,
                output_tokens: 4,
            })
        );
    }

    #[test]
    fn message_snapshots_accumulate_once_across_model_calls() {
        let log = InvocationAccountingLog::default();
        let invocation = action(1);
        lock(&log.inner).native_sessions.insert(
            invocation,
            NativeSessionFact {
                profile: AgentCompatibilityProfile::ClaudeCodeStreamJsonV1,
                diagnostic_identity: Arc::from("diagnostic"),
                native_session_identity: Some(Arc::from("session")),
            },
        );
        for (sequence, observed) in [
            (
                1,
                AgentObservation::Lifecycle {
                    milestone: AgentLifecycleMilestone::MessageStarted,
                },
            ),
            (
                2,
                AgentObservation::Usage {
                    input_tokens: 10,
                    output_tokens: 0,
                },
            ),
            (
                3,
                AgentObservation::Usage {
                    input_tokens: 10,
                    output_tokens: 4,
                },
            ),
            (
                4,
                AgentObservation::Lifecycle {
                    milestone: AgentLifecycleMilestone::MessageCompleted,
                },
            ),
            (
                5,
                AgentObservation::Lifecycle {
                    milestone: AgentLifecycleMilestone::MessageStarted,
                },
            ),
            (
                6,
                AgentObservation::Usage {
                    input_tokens: 3,
                    output_tokens: 2,
                },
            ),
            (
                7,
                AgentObservation::Lifecycle {
                    milestone: AgentLifecycleMilestone::MessageCompleted,
                },
            ),
        ] {
            log.record_observation(&observation(invocation, sequence, observed));
        }

        assert_eq!(
            log.usage(invocation),
            Some(InvocationUsage {
                input_tokens: 13,
                output_tokens: 6,
            })
        );
    }

    #[test]
    fn final_usage_isolated_by_invocation_identity() {
        let log = InvocationAccountingLog::default();
        for (invocation, input, output) in
            [(action(1), 3, 2), (action(1), 5, 7), (action(2), 11, 13)]
        {
            log.record_observation(&observation(
                invocation,
                1,
                AgentObservation::Usage {
                    input_tokens: input,
                    output_tokens: output,
                },
            ));
        }
        assert_eq!(
            log.usage(action(1)),
            Some(InvocationUsage {
                input_tokens: 5,
                output_tokens: 7,
            })
        );
        assert_eq!(
            log.usage(action(2)),
            Some(InvocationUsage {
                input_tokens: 11,
                output_tokens: 13,
            })
        );
        assert_eq!(log.recorded_invocations(), vec![action(1), action(2)]);
    }
}
