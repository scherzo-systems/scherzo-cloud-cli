use std::cmp::Ordering;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
#[cfg(test)]
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::time::Duration;

use tokio::sync::Notify;

const SYSTEM_CLOCK_DOMAIN: u64 = 0;
#[cfg(test)]
static NEXT_TEST_CLOCK_DOMAIN: AtomicU64 = AtomicU64::new(1);
#[cfg(test)]
static ACTIVE_NATIVE_WAITS: AtomicUsize = AtomicUsize::new(0);

type TimerFuture = Pin<Box<dyn Future<Output = Result<(), LeaseClockError>> + Send + 'static>>;

/// A local, process-only instant from one suspend-aware clock domain.
///
/// The representation stays private so callers cannot construct an instant from civil time or
/// from a suspend-excluding clock.
#[derive(Clone, Copy, Debug)]
pub(super) struct LeaseInstant {
    clock_domain: u64,
    nanoseconds: u64,
}

impl LeaseInstant {
    pub(super) fn checked_add(self, duration: Duration) -> Result<Self, LeaseClockError> {
        let nanoseconds = duration_nanoseconds(duration)?;
        Ok(Self {
            clock_domain: self.clock_domain,
            nanoseconds: self
                .nanoseconds
                .checked_add(nanoseconds)
                .ok_or(LeaseClockError::ArithmeticOverflow)?,
        })
    }

    pub(super) fn checked_sub(self, duration: Duration) -> Result<Self, LeaseClockError> {
        let nanoseconds = duration_nanoseconds(duration)?;
        Ok(Self {
            clock_domain: self.clock_domain,
            nanoseconds: self
                .nanoseconds
                .checked_sub(nanoseconds)
                .ok_or(LeaseClockError::ArithmeticOverflow)?,
        })
    }

    pub(super) fn checked_duration_since(self, earlier: Self) -> Result<Duration, LeaseClockError> {
        self.require_same_domain(earlier)?;
        let nanoseconds = self
            .nanoseconds
            .checked_sub(earlier.nanoseconds)
            .ok_or(LeaseClockError::ArithmeticOverflow)?;
        Ok(Duration::from_nanos(nanoseconds))
    }

    pub(super) fn checked_cmp(self, other: Self) -> Result<Ordering, LeaseClockError> {
        self.require_same_domain(other)?;
        Ok(self.nanoseconds.cmp(&other.nanoseconds))
    }

    fn require_same_domain(self, other: Self) -> Result<(), LeaseClockError> {
        if self.clock_domain == other.clock_domain {
            Ok(())
        } else {
            Err(LeaseClockError::IncompatibleInstant)
        }
    }
}

fn duration_nanoseconds(duration: Duration) -> Result<u64, LeaseClockError> {
    u64::try_from(duration.as_nanos()).map_err(|_| LeaseClockError::ArithmeticOverflow)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LeaseClockError {
    UnsupportedPlatform,
    ClockUnavailable,
    TimerUnavailable,
    TimerWaitFailed,
    ArithmeticOverflow,
    IncompatibleInstant,
}

impl std::fmt::Display for LeaseClockError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::UnsupportedPlatform => {
                "suspend-aware lease clock is unsupported on this platform"
            }
            Self::ClockUnavailable => "suspend-aware lease clock is unavailable",
            Self::TimerUnavailable => "suspend-aware lease timer is unavailable",
            Self::TimerWaitFailed => "suspend-aware lease timer wait failed",
            Self::ArithmeticOverflow => "suspend-aware lease time arithmetic overflowed",
            Self::IncompatibleInstant => "suspend-aware lease instants use different clock domains",
        })
    }
}

impl std::error::Error for LeaseClockError {}

trait LeaseClockSource: Send + Sync {
    fn now_nanoseconds(&self) -> Result<u64, LeaseClockError>;
    fn start_timer(&self, deadline_nanoseconds: u64) -> Result<TimerFuture, LeaseClockError>;
}

/// The reusable clock boundary owned by Runner Serve.
#[derive(Clone)]
pub(super) struct LeaseClock {
    clock_domain: u64,
    source: Arc<dyn LeaseClockSource>,
}

impl LeaseClock {
    pub(super) fn system() -> Result<Self, LeaseClockError> {
        let source: Arc<dyn LeaseClockSource> = Arc::new(SystemLeaseClockSource);
        source.now_nanoseconds()?;
        Ok(Self {
            clock_domain: SYSTEM_CLOCK_DOMAIN,
            source,
        })
    }

    pub(super) fn now(&self) -> Result<LeaseInstant, LeaseClockError> {
        Ok(LeaseInstant {
            clock_domain: self.clock_domain,
            nanoseconds: self.source.now_nanoseconds()?,
        })
    }

    pub(super) fn start_wait(&self, deadline: LeaseInstant) -> Result<LeaseWait, LeaseClockError> {
        if deadline.clock_domain != self.clock_domain {
            return Err(LeaseClockError::IncompatibleInstant);
        }
        Ok(LeaseWait {
            timer: self.source.start_timer(deadline.nanoseconds)?,
        })
    }

    #[cfg(test)]
    fn with_source(source: Arc<dyn LeaseClockSource>) -> Result<Self, LeaseClockError> {
        let clock_domain = NEXT_TEST_CLOCK_DOMAIN
            .fetch_update(AtomicOrdering::Relaxed, AtomicOrdering::Relaxed, |domain| {
                domain.checked_add(1)
            })
            .map_err(|_| LeaseClockError::ArithmeticOverflow)?;
        Ok(Self {
            clock_domain,
            source,
        })
    }
}

/// A separately owned cancellation signal for a lease wait.
#[derive(Clone, Default)]
pub(super) struct LeaseWaitCancellation {
    cancelled: Arc<AtomicBool>,
    changed: Arc<Notify>,
}

impl LeaseWaitCancellation {
    pub(super) fn cancel(&self) {
        if !self.cancelled.swap(true, AtomicOrdering::AcqRel) {
            self.changed.notify_waiters();
        }
    }

    async fn cancelled(&self) {
        loop {
            let notified = self.changed.notified();
            if self.cancelled.load(AtomicOrdering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LeaseWaitOutcome {
    Due,
    Cancelled,
}

/// An armed native timer. Dropping it or completing `wait` releases its native descriptor and
/// Tokio reactor registration.
pub(super) struct LeaseWait {
    timer: TimerFuture,
}

impl LeaseWait {
    pub(super) async fn wait(
        self,
        cancellation: &LeaseWaitCancellation,
    ) -> Result<LeaseWaitOutcome, LeaseClockError> {
        let mut timer = self.timer;
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Ok(LeaseWaitOutcome::Cancelled),
            result = &mut timer => {
                result?;
                Ok(LeaseWaitOutcome::Due)
            }
        }
    }
}

struct SystemLeaseClockSource;

impl LeaseClockSource for SystemLeaseClockSource {
    fn now_nanoseconds(&self) -> Result<u64, LeaseClockError> {
        platform::now_nanoseconds()
    }

    fn start_timer(&self, deadline_nanoseconds: u64) -> Result<TimerFuture, LeaseClockError> {
        platform::start_timer(deadline_nanoseconds)
    }
}

struct NativeWaitRegistration;

impl NativeWaitRegistration {
    fn new() -> Self {
        #[cfg(test)]
        ACTIVE_NATIVE_WAITS.fetch_add(1, AtomicOrdering::Relaxed);
        Self
    }
}

impl Drop for NativeWaitRegistration {
    fn drop(&mut self) {
        #[cfg(test)]
        ACTIVE_NATIVE_WAITS.fetch_sub(1, AtomicOrdering::Relaxed);
    }
}

#[cfg(any(target_os = "linux", all(target_os = "macos", target_arch = "aarch64")))]
fn require_tokio_reactor() -> Result<(), LeaseClockError> {
    tokio::runtime::Handle::try_current()
        .map(|_| ())
        .map_err(|_| LeaseClockError::TimerUnavailable)
}

#[cfg(any(target_os = "linux", all(target_os = "macos", target_arch = "aarch64")))]
fn timer_descriptor(result: libc::c_int) -> Result<libc::c_int, LeaseClockError> {
    if result < 0 {
        Err(LeaseClockError::TimerUnavailable)
    } else {
        Ok(result)
    }
}

#[cfg(any(target_os = "linux", all(target_os = "macos", target_arch = "aarch64")))]
fn timer_setup(result: libc::c_int) -> Result<(), LeaseClockError> {
    if result == 0 {
        Ok(())
    } else {
        Err(LeaseClockError::TimerUnavailable)
    }
}

#[cfg(any(target_os = "linux", all(target_os = "macos", target_arch = "aarch64")))]
#[allow(
    unsafe_code,
    reason = "a successful native timer constructor transfers its new descriptor to OwnedFd"
)]
fn owned_native_timer(raw_fd: libc::c_int) -> std::os::fd::OwnedFd {
    // SAFETY: each caller passes the successful result of a native descriptor constructor.
    unsafe { <std::os::fd::OwnedFd as std::os::fd::FromRawFd>::from_raw_fd(raw_fd) }
}

#[cfg(any(target_os = "linux", all(target_os = "macos", target_arch = "aarch64")))]
fn reactor_timer(
    timer: std::os::fd::OwnedFd,
    consume: fn(libc::c_int) -> std::io::Result<()>,
) -> Result<TimerFuture, LeaseClockError> {
    use std::os::fd::AsRawFd as _;

    let timer = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::io::unix::AsyncFd::with_interest(timer, tokio::io::Interest::READABLE)
    }))
    .map_err(|_| LeaseClockError::TimerUnavailable)?
    .map_err(|_| LeaseClockError::TimerUnavailable)?;
    let registration = NativeWaitRegistration::new();
    Ok(Box::pin(async move {
        let _registration = registration;
        loop {
            let mut ready = timer
                .readable()
                .await
                .map_err(|_| LeaseClockError::TimerWaitFailed)?;
            match ready.try_io(|descriptor| consume(descriptor.get_ref().as_raw_fd())) {
                Ok(Ok(())) => return Ok(()),
                Ok(Err(_)) => return Err(LeaseClockError::TimerWaitFailed),
                Err(_) => continue,
            }
        }
    }))
}

// Linux and macOS compile as mutually exclusive implementations of one platform module;
// keeping each small native import surface local makes unsupported targets fail closed.
// jscpd:ignore-start
#[cfg(target_os = "linux")]
mod platform {
    use std::io;
    use std::os::fd::AsRawFd;
    use std::ptr;

    use super::{
        LeaseClockError, TimerFuture, owned_native_timer, reactor_timer, require_tokio_reactor,
        timer_descriptor, timer_setup,
    };
    // jscpd:ignore-end

    pub(super) fn now_nanoseconds() -> Result<u64, LeaseClockError> {
        clock_gettime_nanoseconds(libc::CLOCK_BOOTTIME)
    }

    #[cfg(test)]
    pub(super) fn suspend_clock_sample() -> Result<(u64, u64), LeaseClockError> {
        // CLOCK_MONOTONIC is sampled only to prove that the operator host actually suspended. It
        // is never exposed through the production lease-clock boundary or used as a fallback.
        Ok((
            clock_gettime_nanoseconds(libc::CLOCK_BOOTTIME)?,
            clock_gettime_nanoseconds(libc::CLOCK_MONOTONIC)?,
        ))
    }

    pub(super) fn start_timer(deadline_nanoseconds: u64) -> Result<TimerFuture, LeaseClockError> {
        if deadline_nanoseconds <= now_nanoseconds()? {
            return Ok(Box::pin(async { Ok(()) }));
        }
        require_tokio_reactor()?;
        let raw_fd = create_timerfd()?;
        let timer = owned_native_timer(raw_fd);
        arm_timer(timer.as_raw_fd(), deadline_nanoseconds)?;
        reactor_timer(timer, read_expiration)
    }

    #[allow(
        unsafe_code,
        reason = "Linux suspend-clock evidence and production CLOCK_BOOTTIME reads require clock_gettime"
    )]
    fn clock_gettime_nanoseconds(clock_id: libc::clockid_t) -> Result<u64, LeaseClockError> {
        let mut value = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: `value` is initialized and exclusively borrowed for this call.
        if unsafe { libc::clock_gettime(clock_id, &mut value) } != 0 {
            return Err(LeaseClockError::ClockUnavailable);
        }
        if value.tv_sec < 0 || !(0..1_000_000_000).contains(&value.tv_nsec) {
            return Err(LeaseClockError::ClockUnavailable);
        }
        let seconds = u64::try_from(value.tv_sec).map_err(|_| LeaseClockError::ClockUnavailable)?;
        let nanoseconds =
            u64::try_from(value.tv_nsec).map_err(|_| LeaseClockError::ClockUnavailable)?;
        seconds
            .checked_mul(1_000_000_000)
            .and_then(|whole| whole.checked_add(nanoseconds))
            .ok_or(LeaseClockError::ClockUnavailable)
    }

    #[allow(
        unsafe_code,
        reason = "timerfd_create is the Linux CLOCK_BOOTTIME timer boundary"
    )]
    fn create_timerfd() -> Result<libc::c_int, LeaseClockError> {
        // SAFETY: the call has no pointer arguments and returns a new descriptor on success.
        timer_descriptor(unsafe {
            libc::timerfd_create(libc::CLOCK_BOOTTIME, libc::TFD_CLOEXEC | libc::TFD_NONBLOCK)
        })
    }

    #[allow(
        unsafe_code,
        reason = "timerfd_settime arms the owned Linux CLOCK_BOOTTIME timer descriptor"
    )]
    fn arm_timer(
        descriptor: libc::c_int,
        deadline_nanoseconds: u64,
    ) -> Result<(), LeaseClockError> {
        let seconds = deadline_nanoseconds / 1_000_000_000;
        let nanoseconds = deadline_nanoseconds % 1_000_000_000;
        let value = libc::itimerspec {
            it_interval: libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            },
            it_value: libc::timespec {
                tv_sec: libc::time_t::try_from(seconds)
                    .map_err(|_| LeaseClockError::ArithmeticOverflow)?,
                tv_nsec: libc::c_long::try_from(nanoseconds)
                    .map_err(|_| LeaseClockError::ArithmeticOverflow)?,
            },
        };
        // SAFETY: `descriptor` is an owned timerfd and `value` is a valid immutable timer spec.
        timer_setup(unsafe {
            libc::timerfd_settime(descriptor, libc::TFD_TIMER_ABSTIME, &value, ptr::null_mut())
        })
    }

    #[allow(
        unsafe_code,
        reason = "reading one u64 expiration count consumes readiness from timerfd"
    )]
    fn read_expiration(descriptor: libc::c_int) -> io::Result<()> {
        let mut expirations = 0_u64;
        // SAFETY: the destination points to a writable u64 for exactly its byte length.
        let read = unsafe {
            libc::read(
                descriptor,
                ptr::from_mut(&mut expirations).cast(),
                size_of::<u64>(),
            )
        };
        if read < 0 {
            return Err(io::Error::last_os_error());
        }
        if usize::try_from(read).ok() != Some(size_of::<u64>()) || expirations == 0 {
            return Err(io::Error::other("invalid timerfd expiration"));
        }
        Ok(())
    }
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
mod platform {
    use std::io;
    use std::os::fd::AsRawFd;
    use std::ptr;

    use super::{
        LeaseClockError, TimerFuture, owned_native_timer, reactor_timer, require_tokio_reactor,
        timer_descriptor, timer_setup,
    };

    const TIMER_IDENTIFIER: u64 = 1;

    #[repr(C)]
    struct MachTimebaseInfo {
        numer: u32,
        denom: u32,
    }

    #[allow(
        unsafe_code,
        reason = "mach_continuous_time and its timebase are the macOS suspend-aware clock boundary"
    )]
    unsafe extern "C" {
        fn mach_absolute_time() -> u64;
        fn mach_continuous_time() -> u64;
        fn mach_timebase_info(info: *mut MachTimebaseInfo) -> libc::c_int;
    }

    pub(super) fn now_nanoseconds() -> Result<u64, LeaseClockError> {
        let timebase = timebase()?;
        // SAFETY: mach_continuous_time has no pointer arguments and returns a clock tick count.
        ticks_to_nanoseconds(unsafe { mach_continuous_time() }, &timebase)
    }

    #[cfg(test)]
    pub(super) fn suspend_clock_sample() -> Result<(u64, u64), LeaseClockError> {
        let timebase = timebase()?;
        // mach_absolute_time is sampled only to prove that the operator host actually suspended.
        // It is never exposed through the production lease-clock boundary or used as a fallback.
        // SAFETY: both Mach clock functions have no pointer arguments and return clock tick counts.
        let (continuous, absolute) = unsafe { (mach_continuous_time(), mach_absolute_time()) };
        Ok((
            ticks_to_nanoseconds(continuous, &timebase)?,
            ticks_to_nanoseconds(absolute, &timebase)?,
        ))
    }

    fn ticks_to_nanoseconds(
        ticks: u64,
        timebase: &MachTimebaseInfo,
    ) -> Result<u64, LeaseClockError> {
        let nanoseconds = u128::from(ticks)
            .checked_mul(u128::from(timebase.numer))
            .ok_or(LeaseClockError::ClockUnavailable)?
            / u128::from(timebase.denom);
        u64::try_from(nanoseconds).map_err(|_| LeaseClockError::ClockUnavailable)
    }

    pub(super) fn start_timer(deadline_nanoseconds: u64) -> Result<TimerFuture, LeaseClockError> {
        if deadline_nanoseconds <= now_nanoseconds()? {
            return Ok(Box::pin(async { Ok(()) }));
        }
        let timebase = timebase()?;
        let scaled = u128::from(deadline_nanoseconds)
            .checked_mul(u128::from(timebase.denom))
            .ok_or(LeaseClockError::ArithmeticOverflow)?;
        let rounding = u128::from(timebase.numer - 1);
        let deadline_ticks = scaled
            .checked_add(rounding)
            .ok_or(LeaseClockError::ArithmeticOverflow)?
            / u128::from(timebase.numer);
        let deadline_ticks =
            i64::try_from(deadline_ticks).map_err(|_| LeaseClockError::ArithmeticOverflow)?;
        require_tokio_reactor()?;
        let raw_fd = create_kqueue()?;
        let timer = owned_native_timer(raw_fd);
        arm_timer(timer.as_raw_fd(), deadline_ticks)?;
        reactor_timer(timer, receive_event)
    }

    #[allow(
        unsafe_code,
        reason = "mach_timebase_info converts clock values without consulting civil time"
    )]
    fn timebase() -> Result<MachTimebaseInfo, LeaseClockError> {
        let mut timebase = MachTimebaseInfo { numer: 0, denom: 0 };
        // SAFETY: `timebase` is initialized and exclusively borrowed for this call.
        if unsafe { mach_timebase_info(&mut timebase) } != 0
            || timebase.numer == 0
            || timebase.denom == 0
        {
            Err(LeaseClockError::ClockUnavailable)
        } else {
            Ok(timebase)
        }
    }

    #[allow(
        unsafe_code,
        reason = "kqueue owns the macOS continuous EVFILT_TIMER descriptor"
    )]
    fn create_kqueue() -> Result<libc::c_int, LeaseClockError> {
        // SAFETY: kqueue has no pointer arguments and returns a new descriptor on success.
        timer_descriptor(unsafe { libc::kqueue() })
    }

    #[allow(
        unsafe_code,
        reason = "kevent64 arms the owned macOS continuous-time timer descriptor"
    )]
    fn arm_timer(descriptor: libc::c_int, deadline_ticks: i64) -> Result<(), LeaseClockError> {
        let event = libc::kevent64_s {
            ident: TIMER_IDENTIFIER,
            filter: libc::EVFILT_TIMER,
            flags: libc::EV_ADD | libc::EV_ONESHOT,
            fflags: libc::NOTE_ABSOLUTE | libc::NOTE_MACHTIME | libc::NOTE_MACH_CONTINUOUS_TIME,
            data: deadline_ticks,
            udata: 0,
            ext: [0; 2],
        };
        // SAFETY: `event` is a valid one-shot timer change and the event-list arguments are empty.
        timer_setup(unsafe {
            libc::kevent64(
                descriptor,
                &event,
                1,
                ptr::null_mut(),
                0,
                libc::KEVENT_FLAG_NONE,
                ptr::null(),
            )
        })
    }

    #[allow(
        unsafe_code,
        reason = "kevent64 consumes one pending event from the owned timer kqueue"
    )]
    fn receive_event(descriptor: libc::c_int) -> io::Result<()> {
        let mut event = libc::kevent64_s {
            ident: 0,
            filter: 0,
            flags: 0,
            fflags: 0,
            data: 0,
            udata: 0,
            ext: [0; 2],
        };
        let timeout = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: `event` is writable, the change list is empty, and the zero timeout prevents a
        // spurious readiness notification from making this reactor callback block.
        let received = unsafe {
            libc::kevent64(
                descriptor,
                ptr::null(),
                0,
                &mut event,
                1,
                libc::KEVENT_FLAG_NONE,
                &timeout,
            )
        };
        if received < 0 {
            return Err(io::Error::last_os_error());
        }
        if received == 0 {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }
        if event.flags & libc::EV_ERROR != 0
            || event.ident != TIMER_IDENTIFIER
            || event.filter != libc::EVFILT_TIMER
        {
            return Err(io::Error::other("invalid continuous timer event"));
        }
        Ok(())
    }
}

#[cfg(not(any(target_os = "linux", all(target_os = "macos", target_arch = "aarch64"))))]
mod platform {
    use super::{LeaseClockError, TimerFuture};

    pub(super) fn now_nanoseconds() -> Result<u64, LeaseClockError> {
        Err(LeaseClockError::UnsupportedPlatform)
    }

    pub(super) fn start_timer(_deadline_nanoseconds: u64) -> Result<TimerFuture, LeaseClockError> {
        Err(LeaseClockError::UnsupportedPlatform)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Mutex;
    use std::task::Poll;

    use super::*;
    use crate::runner::service::test_support::with_watchdog;

    struct ControlledSource {
        state: Arc<Mutex<ControlledState>>,
        changed: Arc<Notify>,
    }

    struct ControlledState {
        now_nanoseconds: u64,
        active_timers: usize,
        clock_available: bool,
        timer_available: bool,
    }

    impl ControlledSource {
        fn new(now_nanoseconds: u64) -> Arc<Self> {
            Arc::new(Self {
                state: Arc::new(Mutex::new(ControlledState {
                    now_nanoseconds,
                    active_timers: 0,
                    clock_available: true,
                    timer_available: true,
                })),
                changed: Arc::new(Notify::new()),
            })
        }

        fn clock(self: &Arc<Self>) -> LeaseClock {
            let source: Arc<dyn LeaseClockSource> = self.clone();
            LeaseClock::with_source(source).expect("allocate controlled clock domain")
        }

        fn advance(&self, duration: Duration) {
            let nanoseconds = duration_nanoseconds(duration).expect("controlled clock duration");
            let mut state = self.state.lock().expect("controlled clock mutex poisoned");
            state.now_nanoseconds = state
                .now_nanoseconds
                .checked_add(nanoseconds)
                .expect("controlled clock advance overflowed");
            drop(state);
            self.changed.notify_waiters();
        }

        fn active_timers(&self) -> usize {
            self.state
                .lock()
                .expect("controlled clock mutex poisoned")
                .active_timers
        }

        fn make_clock_unavailable(&self) {
            self.state
                .lock()
                .expect("controlled clock mutex poisoned")
                .clock_available = false;
        }

        fn make_timer_unavailable(&self) {
            self.state
                .lock()
                .expect("controlled clock mutex poisoned")
                .timer_available = false;
        }
    }

    struct ControlledTimerRegistration {
        state: Arc<Mutex<ControlledState>>,
    }

    impl Drop for ControlledTimerRegistration {
        fn drop(&mut self) {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.active_timers -= 1;
        }
    }

    async fn future_is_pending<F: Future>(mut future: Pin<&mut F>) -> bool {
        std::future::poll_fn(|context| match future.as_mut().poll(context) {
            Poll::Pending => Poll::Ready(true),
            Poll::Ready(_) => Poll::Ready(false),
        })
        .await
    }

    #[derive(Debug, Eq, PartialEq)]
    struct SuspendElapsed {
        suspend_aware: Duration,
        awake: Duration,
        suspended: Duration,
    }

    fn suspend_elapsed(
        before: (u64, u64),
        after: (u64, u64),
    ) -> Result<SuspendElapsed, LeaseClockError> {
        let suspend_aware_nanoseconds = after
            .0
            .checked_sub(before.0)
            .ok_or(LeaseClockError::ArithmeticOverflow)?;
        let awake_nanoseconds = after
            .1
            .checked_sub(before.1)
            .ok_or(LeaseClockError::ArithmeticOverflow)?;
        Ok(SuspendElapsed {
            suspend_aware: Duration::from_nanos(suspend_aware_nanoseconds),
            awake: Duration::from_nanos(awake_nanoseconds),
            suspended: Duration::from_nanos(
                suspend_aware_nanoseconds.saturating_sub(awake_nanoseconds),
            ),
        })
    }

    impl LeaseClockSource for ControlledSource {
        fn now_nanoseconds(&self) -> Result<u64, LeaseClockError> {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.clock_available {
                Ok(state.now_nanoseconds)
            } else {
                Err(LeaseClockError::ClockUnavailable)
            }
        }

        fn start_timer(&self, deadline_nanoseconds: u64) -> Result<TimerFuture, LeaseClockError> {
            {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if !state.timer_available {
                    return Err(LeaseClockError::TimerUnavailable);
                }
                state.active_timers += 1;
            }
            let state = Arc::clone(&self.state);
            let changed = Arc::clone(&self.changed);
            let registration = ControlledTimerRegistration {
                state: Arc::clone(&state),
            };
            Ok(Box::pin(async move {
                let _registration = registration;
                loop {
                    let notified = changed.notified();
                    if state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .now_nanoseconds
                        >= deadline_nanoseconds
                    {
                        return Ok(());
                    }
                    notified.await;
                }
            }))
        }
    }

    #[test]
    fn lease_clock_checked_arithmetic_and_comparison_fail_closed() {
        let first_source = ControlledSource::new(10);
        let first_clock = first_source.clock();
        let origin = first_clock.now().unwrap();
        let later = origin.checked_add(Duration::from_nanos(5)).unwrap();
        assert_eq!(later.checked_cmp(origin), Ok(Ordering::Greater));
        assert_eq!(
            later.checked_duration_since(origin),
            Ok(Duration::from_nanos(5))
        );
        assert_eq!(
            later
                .checked_sub(Duration::from_nanos(5))
                .unwrap()
                .checked_cmp(origin),
            Ok(Ordering::Equal)
        );
        assert!(matches!(
            origin.checked_sub(Duration::from_nanos(11)),
            Err(LeaseClockError::ArithmeticOverflow)
        ));
        assert_eq!(
            origin.checked_duration_since(later),
            Err(LeaseClockError::ArithmeticOverflow)
        );

        let near_limit_source = ControlledSource::new(u64::MAX);
        let near_limit = near_limit_source.clock().now().unwrap();
        assert!(matches!(
            near_limit.checked_add(Duration::from_nanos(1)),
            Err(LeaseClockError::ArithmeticOverflow)
        ));
        assert!(matches!(
            origin.checked_add(Duration::new(u64::MAX, 0)),
            Err(LeaseClockError::ArithmeticOverflow)
        ));

        let other = ControlledSource::new(10).clock().now().unwrap();
        assert_eq!(
            origin.checked_cmp(other),
            Err(LeaseClockError::IncompatibleInstant)
        );
        assert_eq!(
            origin.checked_duration_since(other),
            Err(LeaseClockError::IncompatibleInstant)
        );
        assert!(matches!(
            first_clock.start_wait(other),
            Err(LeaseClockError::IncompatibleInstant)
        ));
    }

    #[tokio::test]
    async fn lease_clock_controlled_advance_wakes_due_wait() {
        let source = ControlledSource::new(1_000);
        let clock = source.clock();
        let deadline = clock
            .now()
            .unwrap()
            .checked_add(Duration::from_secs(5))
            .unwrap();
        let wait = clock.start_wait(deadline).unwrap();
        assert_eq!(source.active_timers(), 1);

        source.advance(Duration::from_secs(4));
        assert_eq!(
            clock.now().unwrap().checked_cmp(deadline),
            Ok(Ordering::Less)
        );
        source.advance(Duration::from_secs(1));
        let outcome = with_watchdog(wait.wait(&LeaseWaitCancellation::default()))
            .await
            .expect("controlled lease timer timed out")
            .unwrap();
        assert_eq!(outcome, LeaseWaitOutcome::Due);
        assert_eq!(source.active_timers(), 0);
    }

    #[tokio::test]
    async fn lease_clock_cancellation_releases_timer_resources() {
        let source = ControlledSource::new(0);
        let clock = source.clock();
        let deadline = clock
            .now()
            .unwrap()
            .checked_add(Duration::from_secs(60))
            .unwrap();
        let wait = clock.start_wait(deadline).unwrap();
        assert_eq!(source.active_timers(), 1);

        let cancellation = LeaseWaitCancellation::default();
        let wait_future = wait.wait(&cancellation);
        tokio::pin!(wait_future);
        assert!(
            future_is_pending(wait_future.as_mut()).await,
            "controlled lease wait completed before cancellation"
        );
        cancellation.cancel();
        let outcome = with_watchdog(wait_future)
            .await
            .expect("controlled lease cancellation timed out")
            .unwrap();
        assert_eq!(outcome, LeaseWaitOutcome::Cancelled);
        assert_eq!(source.active_timers(), 0);

        let dropped = clock.start_wait(deadline).unwrap();
        assert_eq!(source.active_timers(), 1);
        drop(dropped);
        assert_eq!(source.active_timers(), 0);
    }

    #[test]
    fn lease_clock_reports_unavailable_clock_and_timer() {
        let clock_source = ControlledSource::new(0);
        let clock = clock_source.clock();
        clock_source.make_clock_unavailable();
        assert!(matches!(
            clock.now(),
            Err(LeaseClockError::ClockUnavailable)
        ));

        let timer_source = ControlledSource::new(0);
        let timer_clock = timer_source.clock();
        let deadline = timer_clock
            .now()
            .unwrap()
            .checked_add(Duration::from_secs(1))
            .unwrap();
        timer_source.make_timer_unavailable();
        assert!(matches!(
            timer_clock.start_wait(deadline),
            Err(LeaseClockError::TimerUnavailable)
        ));
        assert_eq!(timer_source.active_timers(), 0);
    }

    #[test]
    fn lease_clock_suspend_observation_rejects_awake_only_elapsed() {
        let awake_only = suspend_elapsed((100, 100), (20_000_000_100, 20_000_000_100)).unwrap();
        assert_eq!(awake_only.suspended, Duration::ZERO);

        let with_suspend = suspend_elapsed((100, 100), (20_000_000_100, 1_000_000_100)).unwrap();
        assert_eq!(with_suspend.suspended, Duration::from_secs(19));
    }

    #[cfg(any(target_os = "linux", all(target_os = "macos", target_arch = "aarch64")))]
    #[test]
    fn lease_clock_native_timer_without_tokio_fails_observably() {
        let clock = LeaseClock::system().unwrap();
        let deadline = clock
            .now()
            .unwrap()
            .checked_add(Duration::from_secs(1))
            .unwrap();
        assert!(matches!(
            clock.start_wait(deadline),
            Err(LeaseClockError::TimerUnavailable)
        ));
    }

    #[cfg(any(target_os = "linux", all(target_os = "macos", target_arch = "aarch64")))]
    #[test]
    fn lease_clock_native_timer_without_io_driver_fails_observably() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("build runtime without an I/O driver");
        runtime.block_on(async {
            let clock = LeaseClock::system().unwrap();
            let deadline = clock
                .now()
                .unwrap()
                .checked_add(Duration::from_secs(1))
                .unwrap();
            assert!(matches!(
                clock.start_wait(deadline),
                Err(LeaseClockError::TimerUnavailable)
            ));
        });
    }

    #[cfg(any(target_os = "linux", all(target_os = "macos", target_arch = "aarch64")))]
    #[tokio::test]
    async fn lease_clock_native_timer_wakes_and_cancels_without_leaking() {
        let clock = LeaseClock::system().unwrap();
        let deadline = clock
            .now()
            .unwrap()
            .checked_add(Duration::from_millis(10))
            .unwrap();
        let wait = clock.start_wait(deadline).unwrap();
        assert_eq!(ACTIVE_NATIVE_WAITS.load(AtomicOrdering::Relaxed), 1);
        let outcome = with_watchdog(wait.wait(&LeaseWaitCancellation::default()))
            .await
            .expect("native suspend-aware lease timer timed out")
            .unwrap();
        assert_eq!(outcome, LeaseWaitOutcome::Due);
        assert_eq!(ACTIVE_NATIVE_WAITS.load(AtomicOrdering::Relaxed), 0);

        let deadline = clock
            .now()
            .unwrap()
            .checked_add(Duration::from_secs(60))
            .unwrap();
        let wait = clock.start_wait(deadline).unwrap();
        let cancellation = LeaseWaitCancellation::default();
        let wait_future = wait.wait(&cancellation);
        tokio::pin!(wait_future);
        assert!(
            future_is_pending(wait_future.as_mut()).await,
            "native lease wait completed before cancellation"
        );
        cancellation.cancel();
        assert_eq!(
            with_watchdog(wait_future)
                .await
                .expect("native lease cancellation timed out")
                .unwrap(),
            LeaseWaitOutcome::Cancelled
        );
        assert_eq!(ACTIVE_NATIVE_WAITS.load(AtomicOrdering::Relaxed), 0);
    }

    #[cfg(any(target_os = "linux", all(target_os = "macos", target_arch = "aarch64")))]
    #[tokio::test]
    #[ignore = "operator must arrange a dedicated-host suspend and wake through the owning script"]
    async fn lease_clock_real_suspend_probe() {
        const DUE_AFTER: Duration = Duration::from_secs(10);
        const MIN_OBSERVED_SUSPEND: Duration = Duration::from_secs(1);
        let ready_path = PathBuf::from(
            std::env::var_os("SCHERZO_LEASE_SUSPEND_READY_FILE")
                .expect("owning suspend script did not provide its readiness path"),
        );
        let clock = LeaseClock::system().expect("open native suspend-aware lease clock");
        let started = clock.now().expect("read native suspend-aware lease clock");
        let deadline = started
            .checked_add(DUE_AFTER)
            .expect("build probe deadline");
        let wait = clock
            .start_wait(deadline)
            .expect("arm native suspend-aware lease timer");
        assert_eq!(ACTIVE_NATIVE_WAITS.load(AtomicOrdering::Relaxed), 1);
        let suspend_sample_before =
            platform::suspend_clock_sample().expect("sample clocks before suspend");
        std::fs::write(&ready_path, b"ready\n").expect("publish probe readiness");

        let outcome = wait
            .wait(&LeaseWaitCancellation::default())
            .await
            .expect("wait for native suspend-aware lease timer");
        assert_eq!(outcome, LeaseWaitOutcome::Due);
        let elapsed = clock
            .now()
            .expect("read native clock after resume")
            .checked_duration_since(started)
            .expect("measure suspend-aware elapsed time");
        assert!(elapsed >= DUE_AFTER);
        let suspend_sample_after =
            platform::suspend_clock_sample().expect("sample clocks after resume");
        let observed_suspend = suspend_elapsed(suspend_sample_before, suspend_sample_after)
            .expect("measure suspend-only elapsed time");
        assert!(
            observed_suspend.suspended >= MIN_OBSERVED_SUSPEND,
            "host did not demonstrate suspend: suspend-aware elapsed={:?}, awake elapsed={:?}",
            observed_suspend.suspend_aware,
            observed_suspend.awake,
        );
        assert_eq!(ACTIVE_NATIVE_WAITS.load(AtomicOrdering::Relaxed), 0);

        let cancellation = LeaseWaitCancellation::default();
        let cancel_deadline = clock
            .now()
            .expect("read native clock before cancellation")
            .checked_add(Duration::from_secs(3_600))
            .expect("build cancellation deadline");
        let cancelled_wait = clock
            .start_wait(cancel_deadline)
            .expect("arm cancellation probe timer");
        let cancelled_future = cancelled_wait.wait(&cancellation);
        tokio::pin!(cancelled_future);
        assert!(
            future_is_pending(cancelled_future.as_mut()).await,
            "real-suspend lease wait completed before cancellation"
        );
        cancellation.cancel();
        assert_eq!(
            with_watchdog(cancelled_future)
                .await
                .expect("real-suspend cancellation timed out")
                .unwrap(),
            LeaseWaitOutcome::Cancelled
        );
        assert_eq!(ACTIVE_NATIVE_WAITS.load(AtomicOrdering::Relaxed), 0);

        println!(
            "lease suspend probe: platform={} architecture={} elapsed_milliseconds={} awake_elapsed_milliseconds={} suspend_observed_milliseconds={} due=true cancellation=cancelled native_waits_after_cancel=0",
            std::env::consts::OS,
            std::env::consts::ARCH,
            elapsed.as_millis(),
            observed_suspend.awake.as_millis(),
            observed_suspend.suspended.as_millis(),
        );
    }
}
