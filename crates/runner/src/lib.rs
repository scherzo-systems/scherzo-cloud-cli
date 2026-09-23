#![cfg_attr(
    test,
    allow(
        clippy::disallowed_macros,
        clippy::expect_used,
        clippy::panic,
        clippy::unwrap_used,
        reason = "unit tests use Cargo-provided fixture paths and panic shortcuts"
    )
)]

mod control_client;
mod control_protocol;
mod credential;
mod doctor;
mod enrollment;
mod service;
mod telemetry;
mod validation;

use std::fmt;

pub use control_client::{RequestFailure, request};
pub use control_protocol::{
    AssignmentCounts, ConnectionFailure, ConnectionState, ControlError, Operation, ProcessState,
    ProtocolFailure, Response, StatusSnapshot,
};
pub use doctor::{
    CheckDescriptor, CheckResult, Outcome, Registry, RegistryError, Report, SelectionError, Status,
    Summary, built_in_registry,
};
pub use enrollment::{
    ActivationArtifact, ActivationArtifactParts, EnrollmentError, EnrollmentOutcome,
    EnrollmentResponse, ReplacementDisposition, enroll, load_control_socket_path,
    replacement_disposition, write_activation_file,
};
pub use service::{Config, ConfigError};

#[derive(Debug)]
pub struct ServiceError(service::ServiceError);

impl ServiceError {
    pub const fn requires_operator_recovery(&self) -> bool {
        self.0.requires_operator_recovery()
    }
}

impl fmt::Display for ServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::error::Error for ServiceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

pub fn run(config: Config, service_version: &str) -> Result<(), ServiceError> {
    service::run(config, service_version).map_err(ServiceError)
}

#[cfg(feature = "test-fixtures")]
pub fn run_nested_workflow_delivery_failure_fixture(
    fixture_arguments: &[String],
) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(anyhow::Error::from)?;
    runtime.block_on(service::run_nested_workflow_delivery_failure_scenario(
        fixture_arguments,
    ))
}

pub fn workflow_git_helper_requested() -> bool {
    service::workflow_git_helper_requested()
}

pub fn run_workflow_git_helper() -> bool {
    service::run_workflow_git_helper()
}

fn is_loopback(endpoint: &url::Url) -> bool {
    match endpoint.host() {
        Some(url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(address)) => address.is_loopback(),
        Some(url::Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    }
}
