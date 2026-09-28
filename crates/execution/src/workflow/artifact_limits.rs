//! Bounds shared by artifact producers, portable inspection, publication and archive loading.
pub(super) const CARRIER_LIMIT_DIAGNOSTIC_CODE: &str = "carrier_limit_exceeded";
pub(super) const MAXIMUM_EXPORTS: usize = 4_096;
pub(super) const MAXIMUM_CARRIERS: usize = 4_096;
// A valid root contains only result.json and exports. Enumeration is bounded
// separately from the exact-name check, so unexpected names remain diagnostic.
// An exports directory has one entry per distinct carrier (aliases share files).
pub(super) const MAXIMUM_ROOT_ENTRIES: usize = 4_096;
pub(super) const MAXIMUM_EXPORT_ENTRIES: usize = MAXIMUM_CARRIERS;
pub(super) const MAXIMUM_CARRIER_BYTES: u64 = 1024 * 1024 * 1024;
pub(super) const MAXIMUM_TOTAL_CARRIER_BYTES: u64 = 4 * 1024 * 1024 * 1024;
