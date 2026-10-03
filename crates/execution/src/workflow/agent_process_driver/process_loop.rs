//! One guarded event loop for native streaming protocols. Protocol implementations own
//! frame decisions and typed failures; this loop owns the process and its drain order.
use std::io;
use std::process::ExitStatus;

use rustix::process::Pid;
use tokio::process::ChildStdout;
use tokio::sync::{mpsc, oneshot};

use super::{
    GuardedChild, Settlement, group_is_quiescent, read_standard_output, settle_child,
    supervise_process_group, wait_for_process_group_probe,
};
use crate::workflow::admission::{CancellationReason, CancellationSource};
use crate::workflow::agent::{AgentOutcome, AgentProcessDirective};
use crate::workflow::coordinator::CoordinatorClock;

pub(crate) struct ProcessOutput {
    pub child: GuardedChild,
    pub process_group: Pid,
    pub standard_output: ChildStdout,
}

pub(crate) struct State<Clock> {
    pub child: GuardedChild,
    pub process_group: Pid,
    pub standard_output: ChildStdout,
    pub cancellation: CancellationSource,
    pub clock: Clock,
    pub output_closed: bool,
    pub completion: Option<ExitStatus>,
    pub wait_failed: bool,
    pub group_quiescent: bool,
    pub termination_requested: bool,
    pub parser_enabled: bool,
    pub cancelled: Option<CancellationReason>,
}

impl<Clock> State<Clock> {
    pub(crate) fn new(
        output: ProcessOutput,
        cancellation: CancellationSource,
        clock: Clock,
    ) -> Self {
        Self {
            child: output.child,
            process_group: output.process_group,
            standard_output: output.standard_output,
            cancellation,
            clock,
            output_closed: false,
            completion: None,
            wait_failed: false,
            group_quiescent: false,
            termination_requested: false,
            parser_enabled: true,
            cancelled: None,
        }
    }

    /// Observe cancellation at a stream error before attributing it to the parser.
    pub(crate) fn cancellation_at_read_error(&mut self) -> bool {
        if let Some(reason) = self.cancellation.cancellation_reason() {
            self.cancelled = Some(reason);
            true
        } else {
            false
        }
    }

    pub(crate) fn fail_stream_and_force(&mut self) {
        self.parser_enabled = false;
        self.force_group();
    }

    pub(crate) fn force_group(&mut self) {
        self.child.force_process_group(self.process_group);
        self.termination_requested = true;
    }
}

pub(crate) trait Protocol<Clock: CoordinatorClock> {
    type Extra;
    async fn on_start(&mut self, _state: &mut State<Clock>) {}
    fn extra_enabled(&self, state: &State<Clock>) -> bool;
    async fn extra(&mut self) -> Self::Extra;
    async fn on_extra(&mut self, event: Self::Extra, state: &mut State<Clock>);
    async fn on_cancel(&mut self, _reason: CancellationReason, state: &mut State<Clock>) {
        state.parser_enabled = false;
    }
    async fn on_stdout(&mut self, bytes: &[u8], state: &mut State<Clock>);
    fn classify_read_failure(&mut self);
    fn cancellation_precedes_read_failure(&self) -> bool {
        false
    }
    fn read_failure_enabled(&self, state: &State<Clock>) -> bool {
        state.parser_enabled
    }
    async fn on_read_error(&mut self, _error: io::Error, state: &mut State<Clock>) {
        if self.read_failure_enabled(state) {
            if self.cancellation_precedes_read_failure() && state.cancellation_at_read_error() {
                state.parser_enabled = false;
            } else {
                self.classify_read_failure();
                state.fail_stream_and_force();
            }
        }
    }
    async fn on_wait_error(&mut self, state: &mut State<Clock>);
    async fn after_event(&mut self, _state: &mut State<Clock>) {}
    fn needs_group(&self, state: &State<Clock>) -> bool;
    fn probe_group(&self, state: &State<Clock>) -> bool;
    fn on_group_quiescent(&mut self, _state: &mut State<Clock>) {}
    fn on_group_live(&mut self, _state: &mut State<Clock>) {}
    fn force_before_settle(&self, state: &State<Clock>) -> bool {
        !state.group_quiescent
    }
    async fn finish(self, state: State<Clock>, supervisor_quiesced: bool) -> AgentOutcome;
}

pub(crate) struct Supervisor<Clock> {
    pub directives: mpsc::UnboundedReceiver<AgentProcessDirective>,
    pub interrupt: Box<dyn Fn() + Send + 'static>,
    pub settlement: Option<Settlement<Clock>>,
}

pub(crate) async fn drive_signalled<Clock, P>(
    output: ProcessOutput,
    cancellation: CancellationSource,
    clock: Clock,
    protocol: P,
    directives: mpsc::UnboundedReceiver<AgentProcessDirective>,
    settlement: Option<Settlement<Clock>>,
) -> AgentOutcome
where
    Clock: CoordinatorClock,
    P: Protocol<Clock>,
{
    let process_group = output.process_group;
    drive(
        State::new(output, cancellation, clock),
        protocol,
        Supervisor {
            directives,
            interrupt: Box::new(move || super::signal_interrupt(process_group)),
            settlement,
        },
    )
    .await
}

pub(crate) async fn drive<Clock, P>(
    mut state: State<Clock>,
    mut protocol: P,
    supervisor: Supervisor<Clock>,
) -> AgentOutcome
where
    Clock: CoordinatorClock,
    P: Protocol<Clock>,
{
    let (stop_supervisor, supervisor_shutdown) = oneshot::channel();
    let process_supervisor = tokio::spawn(supervise_process_group(
        state.process_group,
        state.cancellation.clone(),
        supervisor.directives,
        supervisor.interrupt,
        supervisor_shutdown,
        supervisor.settlement,
    ));
    let cancellation_source = state.cancellation.clone();
    let cancellation = cancellation_source.wait_for_cancellation();
    tokio::pin!(cancellation);
    let mut buffer = [0_u8; 8 * 1024];
    let mut probe_clock = state.clock.clone();
    protocol.on_start(&mut state).await;
    while !state.output_closed
        || (state.completion.is_none() && !state.wait_failed)
        || protocol.needs_group(&state)
    {
        let extra_enabled = protocol.extra_enabled(&state);
        let probe_enabled = protocol.probe_group(&state);
        tokio::select! {
            biased;
            reason = &mut cancellation, if state.cancelled.is_none() => {
                state.cancelled = Some(reason);
                protocol.on_cancel(reason, &mut state).await;
            }
            event = protocol.extra(), if extra_enabled => {
                protocol.on_extra(event, &mut state).await;
            }
            read = read_standard_output(&mut state.standard_output, &mut buffer), if !state.output_closed => {
                match read {
                    Ok(None) => state.output_closed = true,
                    Ok(Some(count)) if state.parser_enabled => {
                        protocol.on_stdout(&buffer[..count], &mut state).await;
                    }
                    Ok(Some(_)) => {}
                    Err(error) => {
                        state.output_closed = true;
                        protocol.on_read_error(error, &mut state).await;
                    }
                }
            }
            waited = state.child.wait(), if state.completion.is_none() && !state.wait_failed => {
                match waited {
                    Ok(status) => state.completion = Some(status),
                    Err(()) => {
                        state.wait_failed = true;
                        state.parser_enabled = false;
                        protocol.on_wait_error(&mut state).await;
                    }
                }
            }
            () = wait_for_process_group_probe(&mut probe_clock), if probe_enabled => {}
        }
        protocol.after_event(&mut state).await;
        if state.output_closed
            && (state.completion.is_some() || state.wait_failed)
            && !state.group_quiescent
            && protocol.needs_group(&state)
        {
            if group_is_quiescent(state.process_group) {
                state.group_quiescent = true;
                protocol.on_group_quiescent(&mut state);
            } else {
                protocol.on_group_live(&mut state);
            }
        }
    }
    if state.cancelled.is_none() {
        state.cancelled = cancellation_source.cancellation_reason();
    }
    if protocol.force_before_settle(&state) {
        state.force_group();
    }
    settle_child(&mut state.child, state.process_group, &mut state.completion).await;
    let _ = stop_supervisor.send(());
    let supervisor_quiesced = process_supervisor.await.is_ok();
    protocol.finish(state, supervisor_quiesced).await
}
