use std::future::Future;

use super::{
    AgentAdapter, AgentFailureCause, AgentObservationSink, AgentOutcome, AgentStartCallback,
    AgentTerminalCallback, invoke_agent_adapter,
};
use crate::execution::workflow::agent_input::ClosedAgentInvocation;
use crate::execution::workflow::claude_code::ClaudeCodeConfig;
use crate::execution::workflow::claude_code_stream_json_v1::ClaudeCodeStreamJsonV1ProtocolLimits;

pub(crate) trait AgentInvocationDispatcher<Sink>: Clone + Send + Sync + 'static
where
    Sink: AgentObservationSink,
{
    fn invoke(
        &self,
        invocation: ClosedAgentInvocation<Sink>,
        started: AgentStartCallback,
        terminal: AgentTerminalCallback,
    ) -> impl Future<Output = ()> + Send;
}

pub(crate) async fn invoke_agent_dispatcher<Dispatcher, Sink>(
    dispatcher: &Dispatcher,
    invocation: ClosedAgentInvocation<Sink>,
    started: AgentStartCallback,
    terminal: AgentTerminalCallback,
) where
    Dispatcher: AgentInvocationDispatcher<Sink>,
    Sink: AgentObservationSink,
{
    let unreported_return = terminal.clone();
    dispatcher.invoke(invocation, started, terminal).await;
    let _ = unreported_return.report(AgentOutcome::Failed {
        cause: AgentFailureCause::HarnessProtocolFailed,
    });
}

#[derive(Clone)]
pub(crate) struct ClosedAgentDispatcher<PiAdapter, ClaudeCodeAdapter> {
    pi: PiAdapter,
    claude_code: ClaudeCodeAdapter,
}

impl<PiAdapter, ClaudeCodeAdapter> ClosedAgentDispatcher<PiAdapter, ClaudeCodeAdapter> {
    pub(crate) fn new(pi: PiAdapter, claude_code: ClaudeCodeAdapter) -> Self {
        Self { pi, claude_code }
    }
}

impl<Sink, PiAdapter, ClaudeCodeAdapter> AgentInvocationDispatcher<Sink>
    for ClosedAgentDispatcher<PiAdapter, ClaudeCodeAdapter>
where
    Sink: AgentObservationSink,
    PiAdapter: AgentAdapter<
            Sink,
            NativeConfiguration = crate::execution::workflow::pi::PiConfig,
            ProtocolLimits = crate::execution::workflow::pi_json_v1::PiJsonV1ProtocolLimits,
        >,
    ClaudeCodeAdapter: AgentAdapter<
            Sink,
            NativeConfiguration = ClaudeCodeConfig,
            ProtocolLimits = ClaudeCodeStreamJsonV1ProtocolLimits,
        >,
{
    async fn invoke(
        &self,
        invocation: ClosedAgentInvocation<Sink>,
        started: AgentStartCallback,
        terminal: AgentTerminalCallback,
    ) {
        match invocation {
            ClosedAgentInvocation::Pi(invocation) => {
                invoke_agent_adapter(&self.pi, invocation, started, terminal).await;
            }
            ClosedAgentInvocation::ClaudeCode(invocation) => {
                invoke_agent_adapter(&self.claude_code, invocation, started, terminal).await;
            }
        }
    }
}

#[cfg(test)]
mod tests;
