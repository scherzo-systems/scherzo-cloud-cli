use std::time::Duration;

pub(crate) const MINIMUM_CANCELLATION_GRACE: Duration = Duration::from_secs(1);
pub(crate) const MAXIMUM_CANCELLATION_GRACE: Duration = Duration::from_secs(5 * 60);
