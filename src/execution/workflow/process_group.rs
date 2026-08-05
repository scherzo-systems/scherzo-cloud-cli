use std::sync::Arc;

use nix::errno::Errno;
use nix::sys::signal::killpg;
use nix::unistd::Pid as NixPid;
use rustix::process::{Pid, Signal, WaitOptions, kill_process_group, waitpgid};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AuthenticatedProcessGroup {
    process_group: Pid,
    leader_start_identity: String,
}

impl AuthenticatedProcessGroup {
    pub(crate) fn new(process_group: Pid, leader_start_identity: String) -> Option<Self> {
        if leader_start_identity.is_empty() || leader_start_identity.len() > 256 {
            return None;
        }
        Some(Self {
            process_group,
            leader_start_identity,
        })
    }

    pub(crate) const fn process_group(&self) -> Pid {
        self.process_group
    }

    pub(crate) fn leader_start_identity(&self) -> &str {
        &self.leader_start_identity
    }
}

pub(crate) trait DurableProcessGuardStore: Send + Sync {
    fn register(
        &self,
        step: &str,
        action_id: u64,
        identity: &AuthenticatedProcessGroup,
    ) -> Result<String, ()>;

    fn mark_released(&self, guard_id: &str) -> Result<(), ()>;

    fn mark_quiesced(&self, guard_id: &str) -> Result<(), ()>;
}

#[derive(Clone, Default)]
pub(crate) struct ProcessGuardRegistry {
    durable: Option<Arc<dyn DurableProcessGuardStore>>,
}

impl ProcessGuardRegistry {
    pub(crate) fn durable(store: Arc<dyn DurableProcessGuardStore>) -> Self {
        Self {
            durable: Some(store),
        }
    }

    pub(crate) fn is_durable(&self) -> bool {
        self.durable.is_some()
    }

    pub(crate) fn register(
        &self,
        step: &str,
        action_id: u64,
        identity: &AuthenticatedProcessGroup,
    ) -> Result<ProcessGuardRegistration, ()> {
        let guard_id = self
            .durable
            .as_ref()
            .map(|durable| durable.register(step, action_id, identity))
            .transpose()?;
        Ok(ProcessGuardRegistration {
            durable: self.durable.clone(),
            guard_id,
            quiesced: false,
        })
    }
}

pub(crate) struct ProcessGuardRegistration {
    durable: Option<Arc<dyn DurableProcessGuardStore>>,
    guard_id: Option<String>,
    quiesced: bool,
}

impl ProcessGuardRegistration {
    pub(crate) fn mark_released(&self) -> Result<(), ()> {
        match (&self.durable, &self.guard_id) {
            (Some(durable), Some(guard_id)) => durable.mark_released(guard_id),
            (None, None) => Ok(()),
            _ => Err(()),
        }
    }

    pub(crate) fn mark_quiesced(&mut self) -> Result<(), ()> {
        if self.quiesced {
            return Ok(());
        }
        match (&self.durable, &self.guard_id) {
            (Some(durable), Some(guard_id)) => durable.mark_quiesced(guard_id)?,
            (None, None) => {}
            _ => return Err(()),
        }
        self.quiesced = true;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LeaderState {
    Running,
    Stopped,
    Zombie,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProcessIdentityObservation {
    Exact { leader: LeaderState },
    Absent,
    Unavailable,
}

pub(crate) trait ProcessIdentityInspector {
    fn observe(&self, identity: &AuthenticatedProcessGroup) -> ProcessIdentityObservation;
}

pub(crate) struct SystemProcessIdentityInspector;

#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) fn capture_process_group_identity(
    process_group: Pid,
) -> Option<AuthenticatedProcessGroup> {
    let raw = i64::from(process_group.as_raw_pid());
    let process = read_linux_process(raw).ok().flatten()?;
    if process.pid != raw || process.process_group != raw {
        return None;
    }
    AuthenticatedProcessGroup::new(process_group, process.start_identity)
}

#[cfg(target_vendor = "apple")]
pub(crate) fn capture_process_group_identity(
    process_group: Pid,
) -> Option<AuthenticatedProcessGroup> {
    let raw = process_group.as_raw_pid();
    let process = read_apple_process(raw).ok().flatten()?;
    if process.pid != i64::from(raw) || process.process_group != i64::from(raw) {
        return None;
    }
    AuthenticatedProcessGroup::new(process_group, process.start_identity)
}

#[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
pub(crate) fn capture_process_group_identity(
    _process_group: Pid,
) -> Option<AuthenticatedProcessGroup> {
    None
}

impl ProcessIdentityInspector for SystemProcessIdentityInspector {
    fn observe(&self, identity: &AuthenticatedProcessGroup) -> ProcessIdentityObservation {
        system_process_identity_observation(identity)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AuthenticatedSignalResult {
    Signalled,
    Absent,
    Unavailable,
}

pub(crate) fn interrupt_authenticated_process_group(
    process_group: &AuthenticatedProcessGroup,
) -> AuthenticatedSignalResult {
    signal_authenticated_process_group(process_group, Signal::INT, &SystemProcessIdentityInspector)
}

pub(crate) fn continue_authenticated_process_group(
    process_group: &AuthenticatedProcessGroup,
) -> AuthenticatedSignalResult {
    signal_authenticated_process_group(process_group, Signal::CONT, &SystemProcessIdentityInspector)
}

pub(crate) fn terminate_authenticated_process_group(
    process_group: &AuthenticatedProcessGroup,
) -> AuthenticatedSignalResult {
    terminate_authenticated_process_group_with(process_group, &SystemProcessIdentityInspector)
}

pub(crate) fn terminate_authenticated_process_group_with(
    process_group: &AuthenticatedProcessGroup,
    inspector: &impl ProcessIdentityInspector,
) -> AuthenticatedSignalResult {
    signal_authenticated_process_group(process_group, Signal::KILL, inspector)
}

pub(super) fn interrupt_process_group(process_group: Pid) {
    let _ = kill_process_group(process_group, Signal::INT);
}

pub(super) fn terminate_process_group(process_group: Pid) {
    let _ = kill_process_group(process_group, Signal::KILL);
}

pub(super) fn reap_process_group_children(process_group: Pid) {
    while matches!(waitpgid(process_group, WaitOptions::NOHANG), Ok(Some(_))) {}
}

pub(super) fn process_group_is_quiescent(process_group: Pid) -> bool {
    matches!(
        killpg(
            NixPid::from_raw(process_group.as_raw_pid()),
            None::<nix::sys::signal::Signal>,
        ),
        Err(Errno::ESRCH)
    )
}

fn signal_authenticated_process_group(
    process_group: &AuthenticatedProcessGroup,
    signal: Signal,
    inspector: &impl ProcessIdentityInspector,
) -> AuthenticatedSignalResult {
    signal_authenticated_process_group_with(process_group, signal, inspector, |group, signal| {
        kill_process_group(group, signal).map_err(|_| ())
    })
}

fn signal_authenticated_process_group_with(
    process_group: &AuthenticatedProcessGroup,
    signal: Signal,
    inspector: &impl ProcessIdentityInspector,
    mut signal_group: impl FnMut(Pid, Signal) -> Result<(), ()>,
) -> AuthenticatedSignalResult {
    match inspector.observe(process_group) {
        ProcessIdentityObservation::Exact { .. } => {
            if signal_group(process_group.process_group(), signal).is_ok() {
                AuthenticatedSignalResult::Signalled
            } else if matches!(
                inspector.observe(process_group),
                ProcessIdentityObservation::Absent
            ) {
                AuthenticatedSignalResult::Absent
            } else {
                AuthenticatedSignalResult::Unavailable
            }
        }
        ProcessIdentityObservation::Absent => AuthenticatedSignalResult::Absent,
        ProcessIdentityObservation::Unavailable => AuthenticatedSignalResult::Unavailable,
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) fn system_process_identity_observation(
    identity: &AuthenticatedProcessGroup,
) -> ProcessIdentityObservation {
    let process_group = i64::from(identity.process_group().as_raw_pid());
    match read_linux_process(process_group) {
        Ok(Some(leader)) => {
            return observe_process_snapshot(identity, &[leader]);
        }
        Ok(None) => {}
        Err(()) => return ProcessIdentityObservation::Unavailable,
    }

    let Ok(entries) = std::fs::read_dir("/proc") else {
        return ProcessIdentityObservation::Unavailable;
    };
    let mut group_found = false;
    for entry in entries {
        let Ok(entry) = entry else {
            return ProcessIdentityObservation::Unavailable;
        };
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<i64>().ok())
        else {
            continue;
        };
        match read_linux_process(pid) {
            Ok(Some(process)) if process.process_group == process_group => group_found = true,
            Ok(Some(_) | None) => {}
            Err(()) => return ProcessIdentityObservation::Unavailable,
        }
    }
    if group_found {
        ProcessIdentityObservation::Unavailable
    } else {
        ProcessIdentityObservation::Absent
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn read_linux_process(pid: i64) -> Result<Option<ObservedProcess>, ()> {
    let stat = match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => stat,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(()),
    };
    linux_process_identity(pid, &stat).map(Some).ok_or(())
}

#[cfg(target_vendor = "apple")]
pub(crate) fn system_process_identity_observation(
    identity: &AuthenticatedProcessGroup,
) -> ProcessIdentityObservation {
    let process_group = identity.process_group().as_raw_pid();
    match read_apple_process(process_group) {
        Ok(Some(leader)) => observe_process_snapshot(identity, &[leader]),
        Ok(None) => match apple_process_group_exists(process_group) {
            Ok(false) => ProcessIdentityObservation::Absent,
            Ok(true) | Err(()) => ProcessIdentityObservation::Unavailable,
        },
        Err(()) => ProcessIdentityObservation::Unavailable,
    }
}

#[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
pub(crate) fn system_process_identity_observation(
    _identity: &AuthenticatedProcessGroup,
) -> ProcessIdentityObservation {
    ProcessIdentityObservation::Unavailable
}

#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
#[derive(Clone, Debug, Eq, PartialEq)]
struct ObservedProcess {
    pid: i64,
    process_group: i64,
    start_identity: String,
    state: LeaderState,
}

#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
fn observe_process_snapshot(
    identity: &AuthenticatedProcessGroup,
    processes: &[ObservedProcess],
) -> ProcessIdentityObservation {
    let process_group = i64::from(identity.process_group().as_raw_pid());
    let leader = processes
        .iter()
        .find(|process| process.pid == process_group);
    if let Some(leader) = leader {
        if leader.process_group == process_group
            && leader.start_identity == identity.leader_start_identity()
        {
            return ProcessIdentityObservation::Exact {
                leader: leader.state,
            };
        }
        // A reused leader PID cannot authenticate the old process group, even if the
        // replacement happens to have selected the same numeric PGID.
        return ProcessIdentityObservation::Absent;
    }
    if processes
        .iter()
        .any(|process| process.process_group == process_group)
    {
        // The guarded protocol retains its leader until group termination. A group
        // without that leader is therefore not safe to classify by number alone.
        ProcessIdentityObservation::Unavailable
    } else {
        ProcessIdentityObservation::Absent
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn linux_process_identity(pid: i64, stat: &str) -> Option<ObservedProcess> {
    let fields = stat
        .rsplit_once(") ")?
        .1
        .split_ascii_whitespace()
        .collect::<Vec<_>>();
    let state = match *fields.first()? {
        "T" | "t" => LeaderState::Stopped,
        "Z" => LeaderState::Zombie,
        _ => LeaderState::Running,
    };
    Some(ObservedProcess {
        pid,
        process_group: fields.get(2)?.parse().ok()?,
        start_identity: (*fields.get(19)?).to_owned(),
        state,
    })
}

#[cfg(target_vendor = "apple")]
#[allow(
    unsafe_code,
    reason = "the Darwin process-identity boundary reads fixed libproc structures"
)]
fn read_apple_process(pid: i32) -> Result<Option<ObservedProcess>, ()> {
    let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::zeroed();
    let size = std::mem::size_of::<libc::proc_bsdinfo>();
    let size_i32 = i32::try_from(size).map_err(|_| ())?;
    // SAFETY: libproc receives the exact size and writable address of `proc_bsdinfo`.
    let read = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            0,
            info.as_mut_ptr().cast(),
            size_i32,
        )
    };
    if read == 0 {
        return match std::io::Error::last_os_error().raw_os_error() {
            Some(libc::ESRCH) => Ok(None),
            _ => Err(()),
        };
    }
    if read != size_i32 {
        return Err(());
    }
    // SAFETY: a full-sized successful libproc read initialized the structure.
    let info = unsafe { info.assume_init() };
    let state = match info.pbi_status {
        libc::SSTOP => LeaderState::Stopped,
        libc::SZOMB => LeaderState::Zombie,
        _ => LeaderState::Running,
    };
    Ok(Some(ObservedProcess {
        pid: i64::from(info.pbi_pid),
        process_group: i64::from(info.pbi_pgid),
        start_identity: format!("{}:{}", info.pbi_start_tvsec, info.pbi_start_tvusec),
        state,
    }))
}

#[cfg(target_vendor = "apple")]
#[allow(
    unsafe_code,
    reason = "the Darwin process-group boundary asks libproc for one numeric member"
)]
fn apple_process_group_exists(process_group: i32) -> Result<bool, ()> {
    let mut member = 0_i32;
    let size = i32::try_from(std::mem::size_of_val(&member)).map_err(|_| ())?;
    // SAFETY: libproc receives the exact size and writable address of one `pid_t`.
    let count = unsafe {
        libc::proc_listpgrppids(process_group, std::ptr::from_mut(&mut member).cast(), size)
    };
    if count < 0 { Err(()) } else { Ok(count > 0) }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;

    struct FixtureInspector(ProcessIdentityObservation);

    impl ProcessIdentityInspector for FixtureInspector {
        fn observe(&self, _identity: &AuthenticatedProcessGroup) -> ProcessIdentityObservation {
            self.0
        }
    }

    fn identity() -> AuthenticatedProcessGroup {
        AuthenticatedProcessGroup::new(Pid::from_raw(41).unwrap(), "9001".to_owned()).unwrap()
    }

    #[test]
    fn exact_identity_is_the_only_fixture_authorized_to_receive_a_signal() {
        let signals = RefCell::new(Vec::new());
        let result = signal_authenticated_process_group_with(
            &identity(),
            Signal::KILL,
            &FixtureInspector(ProcessIdentityObservation::Exact {
                leader: LeaderState::Running,
            }),
            |process_group, signal| {
                signals.borrow_mut().push((process_group, signal));
                Ok(())
            },
        );

        assert_eq!(result, AuthenticatedSignalResult::Signalled);
        assert_eq!(
            signals.borrow().as_slice(),
            [(Pid::from_raw(41).unwrap(), Signal::KILL)]
        );
    }

    #[test]
    fn absent_and_unavailable_identities_are_never_signalled() {
        for (observation, expected) in [
            (
                ProcessIdentityObservation::Absent,
                AuthenticatedSignalResult::Absent,
            ),
            (
                ProcessIdentityObservation::Unavailable,
                AuthenticatedSignalResult::Unavailable,
            ),
        ] {
            let mut signal_count = 0;
            let result = signal_authenticated_process_group_with(
                &identity(),
                Signal::KILL,
                &FixtureInspector(observation),
                |_, _| {
                    signal_count += 1;
                    Ok(())
                },
            );
            assert_eq!(result, expected);
            assert_eq!(signal_count, 0);
        }
    }

    #[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
    #[test]
    fn recycled_leader_identifier_is_evidence_that_the_recorded_group_is_absent() {
        let processes = [ObservedProcess {
            pid: 41,
            process_group: 41,
            start_identity: "later-start".to_owned(),
            state: LeaderState::Running,
        }];

        assert_eq!(
            observe_process_snapshot(&identity(), &processes),
            ProcessIdentityObservation::Absent
        );
    }

    #[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
    #[test]
    fn exact_zombie_leader_keeps_the_identity_authenticated_until_reap() {
        let processes = [ObservedProcess {
            pid: 41,
            process_group: 41,
            start_identity: "9001".to_owned(),
            state: LeaderState::Zombie,
        }];

        assert_eq!(
            observe_process_snapshot(&identity(), &processes),
            ProcessIdentityObservation::Exact {
                leader: LeaderState::Zombie
            }
        );
        assert_eq!(
            observe_process_snapshot(&identity(), &[]),
            ProcessIdentityObservation::Absent
        );
    }
}
