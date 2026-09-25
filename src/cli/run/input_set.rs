use std::ffi::OsString;
use std::io::{self, Write};

use anyhow::Context as _;
use clap::{Args, Subcommand};
use serde::Serialize;

use crate::exit_code::{ExitCode, OutcomeClass};
use scherzo_cloud_api::{
    RunFailure, RunInputSet, RunInputUpload, RunInputUploadOutcome, input_set_is_open,
    input_set_state_name,
};
use scherzo_cloud_human_auth::Deployment;

use super::super::{OrganizationArg, ProjectArg};
use super::acquisition::{self, AcquiredInputs};

pub(super) const ABOUT: &str = "Manage Run Input Sets";

macro_rules! require_run_success {
    ($result:expr) => {
        match $result {
            Ok(value) => value,
            Err(failure) => return Ok(Err(failure)),
        }
    };
}

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<InputSetCommand>,
}

#[derive(Debug, Subcommand)]
enum InputSetCommand {
    #[command(about = "Create an open immutable Run Input Set")]
    Create(CreateCommand),
    #[command(about = "Logically delete a Run Input Set and schedule content cleanup")]
    Delete(DeleteCommand),
    #[command(about = "Seal an uploaded Run Input Set")]
    Seal(SealCommand),
    #[command(about = "Show a Run Input Set and its manifest")]
    Show(ShowCommand),
    #[command(about = "Upload selected members to an open Run Input Set")]
    Upload(UploadCommand),
}

// Input-set creation and runner-pool creation share only generic CLI shape; their
// project/input and pool-name operations remain clearer as separate command types.
// jscpd:ignore-start
#[derive(Debug, Args)]
struct CreateCommand {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,

    #[arg(long, value_name = ProjectArg::VALUE_NAME, help = ProjectArg::HELP)]
    project_id: ProjectArg,

    #[command(flatten)]
    inputs: super::super::NamedInputArgs,

    #[command(flatten)]
    options: super::CloudInputOptions,
}
// jscpd:ignore-end

#[derive(Clone, Debug, Args)]
struct InputSetReference {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,

    #[arg(
        value_name = "INPUT_SET",
        value_parser = super::parse_input_set_id,
        help = "Exact Run Input Set ID"
    )]
    input_set_id: String,
}

impl InputSetReference {
    fn get(
        &self,
        deployment: &Deployment,
        policy: scherzo_cloud_api::HttpTransportPolicy,
        authentication: &super::super::PrincipalAuthenticationArgs,
    ) -> anyhow::Result<Result<RunInputSet, RunFailure>> {
        super::with_api(deployment, policy, authentication, |api| {
            api.get_input_set(&self.organization, &self.input_set_id)
        })
    }

    fn get_open(
        &self,
        deployment: &Deployment,
        policy: scherzo_cloud_api::HttpTransportPolicy,
        authentication: &super::super::PrincipalAuthenticationArgs,
    ) -> anyhow::Result<Result<RunInputSet, RunFailure>> {
        Ok(match self.get(deployment, policy, authentication)? {
            Ok(input_set) if input_set_is_open(&input_set) => Ok(input_set),
            Ok(_) => Err(RunFailure::Conflict),
            Err(failure) => Err(failure),
        })
    }
}

#[derive(Debug, Args)]
struct ShowCommand {
    #[command(flatten)]
    input_set: InputSetReference,

    #[command(flatten)]
    options: super::CloudInputOptions,
}

#[derive(Debug, Args)]
struct UploadCommand {
    #[command(flatten)]
    input_set: InputSetReference,

    #[arg(
        long = "member-file",
        value_names = ["MEMBER", "PATH"],
        num_args = 2,
        action = clap::ArgAction::Append,
        required = true,
        help = "Upload a regular file as one exact logical member"
    )]
    member_files: Vec<OsString>,

    #[command(flatten)]
    options: super::CloudInputOptions,
}

#[derive(Debug, Args)]
struct SealCommand {
    #[command(flatten)]
    input_set: InputSetReference,

    #[command(flatten)]
    options: super::CloudInputOptions,
}

// Keep this leaf's reference and confirmation wording explicit: Run Input Set deletion
// and retained-content deletion are distinct destructive contracts in CLI help.
// jscpd:ignore-start
#[derive(Debug, Args)]
struct DeleteCommand {
    #[command(flatten)]
    input_set: InputSetReference,

    #[command(flatten)]
    confirmation: super::super::ConfirmationArgs,

    #[command(flatten)]
    options: super::CloudInputOptions,
}
// jscpd:ignore-end

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        match self.command {
            None => super::super::print_help(&[super::NAME, "input-set"]),
            Some(InputSetCommand::Create(command)) => execute_deployed(
                command,
                "configure Run Input Set creation",
                CreateCommand::execute,
            ),
            Some(InputSetCommand::Show(command)) => execute_deployed(
                command,
                "configure Run Input Set access",
                ShowCommand::execute,
            ),
            Some(InputSetCommand::Upload(command)) => execute_deployed(
                command,
                "configure Run Input Set upload",
                UploadCommand::execute,
            ),
            Some(InputSetCommand::Seal(command)) => execute_deployed(
                command,
                "configure Run Input Set sealing",
                SealCommand::execute,
            ),
            Some(InputSetCommand::Delete(command)) => execute_deployed(
                command,
                "configure Run Input Set deletion",
                DeleteCommand::execute,
            ),
        }
    }
}

fn execute_deployed<T>(
    command: T,
    context: &'static str,
    operation: impl FnOnce(T, Deployment) -> super::super::CommandResult,
) -> super::super::CommandResult {
    super::super::execute_deployment_command(
        Some(command),
        &[super::NAME, "input-set"],
        context,
        move |command, deployment| operation(command, deployment.clone()),
    )
}

impl CreateCommand {
    fn execute(self, deployment: Deployment) -> super::super::CommandResult {
        let signal_deployment = deployment.clone();
        let signal_organization = self.organization.clone();
        let signal_json = self.options.json;
        super::super::execute_mutation_with_signals(
            "Run Input Set creation",
            None::<String>,
            move |control| {
                let Some(acquired) = acquire_required_inputs(
                    &self.inputs,
                    deployment.fingerprint().api_url(),
                    &self.organization,
                    "at least one named input is required",
                    self.options.json,
                    self.options.authentication.uses_stdin(),
                    control,
                )?
                else {
                    return Ok(super::super::ExitCode::GeneralFailure);
                };
                if control.is_cancelled() {
                    return Ok(super::super::ExitCode::GeneralFailure);
                }
                let result = create_input_set(
                    &deployment,
                    self.options.http.transport_policy(),
                    &self.options.authentication,
                    &self.organization,
                    &self.project_id,
                    &acquired,
                    || control.begin_dispatch(),
                )?;
                if let Ok(input_set) = &result {
                    control.update_recovery(Some(input_set.id.clone()));
                }
                let input_set_id = control.recovery();
                // Creation reports an allocated input-set coordinate rather than the retained-run
                // coordinate used by input deletion; each recovery envelope stays explicit.
                // jscpd:ignore-start
                super::finish_operation(control, || {
                    write_result(
                        deployment.fingerprint().api_url(),
                        &self.organization,
                        input_set_id.as_deref(),
                        "created",
                        result,
                        self.options.authentication.kind(),
                        self.options.json,
                    )
                })
                // jscpd:ignore-end
            },
            move |signal, snapshot| {
                super::super::report_dispatched_signal(
                    signal,
                    snapshot,
                    |recovery| match recovery {
                        Some(input_set_id) => super::write_input_set_recovery(
                            signal_deployment.fingerprint().api_url(),
                            &signal_organization,
                            &input_set_id,
                            signal_json,
                            signal,
                        )
                        .map_err(Into::into),
                        None => super::write_resource_mutation_unknown(
                            "Run Input Set creation",
                            signal_deployment.fingerprint().api_url(),
                            &signal_organization,
                            "input set",
                            None,
                            signal_json,
                            signal,
                        )
                        .map_err(Into::into),
                    },
                )
            },
        )
    }
}

fn create_input_set(
    deployment: &Deployment,
    transport_policy: scherzo_cloud_api::HttpTransportPolicy,
    authentication: &super::super::PrincipalAuthenticationArgs,
    organization: &str,
    project_id: &str,
    acquired: &AcquiredInputs,
    begin_dispatch: impl Fn() -> bool,
) -> anyhow::Result<Result<RunInputSet, RunFailure>> {
    let create_key = scherzo_cloud_support::generate_idempotency_key()
        .context("generate Run Input Set request identity")?;
    super::with_api(deployment, transport_policy, authentication, |api| {
        api.create_input_set(
            organization,
            &create_key,
            project_id,
            &acquired.manifest,
            &begin_dispatch,
        )
    })
}

fn report_input_set_recovery(
    signal: super::super::ExitCode,
    snapshot: super::super::SignalSnapshot<()>,
    deployment: &Deployment,
    organization: &str,
    input_set_id: &str,
    json: bool,
) -> super::super::CommandResult {
    super::super::report_dispatched_signal(signal, snapshot, |()| {
        super::write_input_set_recovery(
            deployment.fingerprint().api_url(),
            organization,
            input_set_id,
            json,
            signal,
        )
        .map_err(Into::into)
    })
}

fn report_input_set_mutation_unknown(
    signal: super::super::ExitCode,
    snapshot: super::super::SignalSnapshot<()>,
    operation: &'static str,
    deployment: &Deployment,
    input_set: &InputSetReference,
    json: bool,
) -> super::super::CommandResult {
    super::super::report_dispatched_signal(signal, snapshot, |()| {
        super::write_resource_mutation_unknown(
            operation,
            deployment.fingerprint().api_url(),
            &input_set.organization,
            "input set",
            Some(&input_set.input_set_id),
            json,
            signal,
        )
        .map_err(Into::into)
    })
}

pub(super) fn stage_and_seal(
    deployment: &Deployment,
    transport_policy: scherzo_cloud_api::HttpTransportPolicy,
    authentication: &super::super::PrincipalAuthenticationArgs,
    organization: &str,
    project_id: &str,
    acquired: &AcquiredInputs,
    control: &super::super::OperationControl<super::CreateRecoveryState>,
) -> anyhow::Result<Result<RunInputSet, RunFailure>> {
    let seal_key = scherzo_cloud_support::generate_idempotency_key()
        .context("generate Run Input Set seal identity")?;
    let input_set = match create_input_set(
        deployment,
        transport_policy,
        authentication,
        organization,
        project_id,
        acquired,
        || {
            control.begin_dispatch_with_recovery(
                super::CreateRecoveryState::input_set_allocation_dispatched(),
            )
        },
    )? {
        Ok(input_set) => input_set,
        Err(failure) => return Ok(Err(failure)),
    };
    control.update_recovery(super::CreateRecoveryState::allocated_input_set(
        &input_set.id,
    ));
    if control.is_cancelled() {
        return Ok(Err(RunFailure::Interrupted));
    }
    let uploads = acquired
        .objects
        .iter()
        .map(acquisition::AcquiredInputObject::upload)
        .collect::<Vec<RunInputUpload<'_>>>();
    if let Err(failure) = super::with_api(deployment, transport_policy, authentication, |api| {
        api.upload_input_members(organization, &input_set, &uploads, || {
            control.begin_dispatch()
        })
    })? {
        return Ok(Err(failure));
    }
    if control.is_cancelled() {
        return Ok(Err(RunFailure::Interrupted));
    }
    super::with_api(deployment, transport_policy, authentication, |api| {
        api.seal_input_set(organization, &seal_key, &input_set, || {
            control.begin_dispatch()
        })
    })
}

impl ShowCommand {
    fn execute(self, deployment: Deployment) -> super::super::CommandResult {
        super::super::execute_read_only_with_signals("Run Input Set show", move |control| {
            let result = self.input_set.get(
                &deployment,
                self.options.http.transport_policy(),
                &self.options.authentication,
            )?;
            super::super::complete_read_only_output(control, || {
                write_result(
                    deployment.fingerprint().api_url(),
                    &self.input_set.organization,
                    Some(&self.input_set.input_set_id),
                    "found",
                    result,
                    self.options.authentication.kind(),
                    self.options.json,
                )
                .map_err(Into::into)
            })
        })
    }
}

impl UploadCommand {
    fn execute(self, deployment: Deployment) -> super::super::CommandResult {
        let signal_deployment = deployment.clone();
        let signal_organization = self.input_set.organization.clone();
        let signal_input_set_id = self.input_set.input_set_id.clone();
        let signal_json = self.options.json;
        super::super::execute_mutation_with_signals(
            "Run Input Set upload",
            (),
            move |control| {
                let acquired = match acquisition::acquire_member_files(&self.member_files) {
                    Ok(acquired) => acquired,
                    Err(failure) => {
                        return super::finish_operation(control, || {
                            super::write_input_acquisition_failure(
                                deployment.fingerprint().api_url(),
                                &self.input_set.organization,
                                &failure,
                                self.options.json,
                            )
                        });
                    }
                };
                let result = upload_selected(&deployment, &self, &acquired, control)?;
                super::finish_operation(control, || {
                    write_upload_result(
                        deployment.fingerprint().api_url(),
                        &self.input_set.organization,
                        &self.input_set.input_set_id,
                        result,
                        self.options.authentication.kind(),
                        self.options.json,
                    )
                })
            },
            move |signal, snapshot| {
                report_input_set_recovery(
                    signal,
                    snapshot,
                    &signal_deployment,
                    &signal_organization,
                    &signal_input_set_id,
                    signal_json,
                )
            },
        )
    }
}

fn upload_selected(
    deployment: &Deployment,
    command: &UploadCommand,
    acquired: &[acquisition::AcquiredInputObject],
    control: &super::super::OperationControl<()>,
) -> anyhow::Result<Result<RunInputUploadOutcome, RunFailure>> {
    let input_set = require_run_success!(command.input_set.get_open(
        deployment,
        command.options.http.transport_policy(),
        &command.options.authentication,
    )?);
    let uploads = acquired
        .iter()
        .map(acquisition::AcquiredInputObject::upload)
        .collect::<Vec<RunInputUpload<'_>>>();
    super::with_api(
        deployment,
        command.options.http.transport_policy(),
        &command.options.authentication,
        |api| {
            api.upload_input_members(
                &command.input_set.organization,
                &input_set,
                &uploads,
                || control.begin_dispatch(),
            )
        },
    )
}

impl SealCommand {
    fn execute(self, deployment: Deployment) -> super::super::CommandResult {
        let signal_deployment = deployment.clone();
        let signal_input_set = self.input_set.clone();
        let signal_json = self.options.json;
        super::super::execute_mutation_with_signals(
            "Run Input Set sealing",
            (),
            move |control| {
                let result = seal(&deployment, &self, control)?;
                // Sealing and deletion have distinct success bodies and recovery semantics even
                // though both complete through the shared mutation control.
                // jscpd:ignore-start
                super::finish_operation(control, || {
                    write_result(
                        deployment.fingerprint().api_url(),
                        &self.input_set.organization,
                        Some(&self.input_set.input_set_id),
                        "sealed",
                        result,
                        self.options.authentication.kind(),
                        self.options.json,
                    )
                })
                // jscpd:ignore-end
            },
            move |signal, snapshot| {
                report_input_set_mutation_unknown(
                    signal,
                    snapshot,
                    "Run Input Set sealing",
                    &signal_deployment,
                    &signal_input_set,
                    signal_json,
                )
            },
        )
    }
}

fn seal(
    deployment: &Deployment,
    command: &SealCommand,
    control: &super::super::OperationControl<()>,
) -> anyhow::Result<Result<RunInputSet, RunFailure>> {
    let input_set = require_run_success!(command.input_set.get_open(
        deployment,
        command.options.http.transport_policy(),
        &command.options.authentication,
    )?);
    let key = scherzo_cloud_support::generate_idempotency_key()
        .context("generate Run Input Set seal identity")?;
    super::with_api(
        deployment,
        command.options.http.transport_policy(),
        &command.options.authentication,
        |api| {
            api.seal_input_set(&command.input_set.organization, &key, &input_set, || {
                control.begin_dispatch()
            })
        },
    )
}

impl DeleteCommand {
    fn execute(self, deployment: Deployment) -> super::super::CommandResult {
        let key = scherzo_cloud_support::generate_idempotency_key()
            .context("generate Run Input Set deletion identity")?;
        let signal_deployment = deployment.clone();
        let signal_input_set = self.input_set.clone();
        let signal_json = self.options.json;
        super::super::execute_mutation_with_signals(
            "Run Input Set deletion",
            (),
            move |control| {
                // Input Set deletion and retained-input deletion deliberately keep separate
                // recovery subjects and output envelopes.
                // jscpd:ignore-start
                let result = super::with_api(
                    &deployment,
                    self.options.http.transport_policy(),
                    &self.options.authentication,
                    |api| {
                        api.delete_input_set(
                            &self.input_set.organization,
                            &self.input_set.input_set_id,
                            &key,
                            || control.begin_dispatch(),
                        )
                    },
                )?;
                // jscpd:ignore-end
                super::finish_operation(control, || {
                    write_mutation_result(
                        deployment.fingerprint().api_url(),
                        &self.input_set.organization,
                        &self.input_set.input_set_id,
                        "deleted",
                        result,
                        self.options.authentication.kind(),
                        self.options.json,
                    )
                })
            },
            move |signal, snapshot| {
                report_input_set_mutation_unknown(
                    signal,
                    snapshot,
                    "Run Input Set deletion",
                    &signal_deployment,
                    &signal_input_set,
                    signal_json,
                )
            },
        )
    }
}

fn write_result(
    deployment: &str,
    organization: &str,
    input_set_id: Option<&str>,
    outcome: &'static str,
    result: Result<RunInputSet, RunFailure>,
    authentication: super::super::PrincipalAuthenticationKind,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match result {
        Ok(input_set) => {
            if json {
                super::write_json(&InputSetResult {
                    schema_version: 1,
                    deployment,
                    outcome,
                    input_set: &input_set,
                })?;
            } else {
                let mut stdout = io::stdout().lock();
                writeln!(
                    stdout,
                    "✓ Run Input Set {}.",
                    input_set_state_name(&input_set)
                )?;
                writeln!(stdout, "\ninput set: {}", input_set.id)?;
                writeln!(stdout, "project: {}", input_set.project_id)?;
                writeln!(stdout, "state: {}", input_set_state_name(&input_set))?;
                writeln!(stdout, "inputs: {}", input_set.manifest.inputs.len())?;
                writeln!(
                    stdout,
                    "manifest digest: {}",
                    input_set.manifest_digest.value
                )?;
                writeln!(stdout, "organization: {organization}")?;
                writeln!(stdout, "deployment: {deployment}")?;
            }
            Ok(ExitCode::Success)
        }
        Err(failure) => write_input_set_failure(
            deployment,
            organization,
            input_set_id,
            &failure,
            authentication,
            json,
        ),
    }
}

fn write_upload_result(
    deployment: &str,
    organization: &str,
    input_set_id: &str,
    result: Result<RunInputUploadOutcome, RunFailure>,
    authentication: super::super::PrincipalAuthenticationKind,
    json: bool,
) -> anyhow::Result<ExitCode> {
    write_input_set_outcome(
        deployment,
        organization,
        Some(input_set_id),
        result,
        authentication,
        json,
        |upload| {
            let verification_required = upload.verification_required_members > 0;
            let outcome = if verification_required {
                "verification_required"
            } else {
                "uploaded"
            };
            if json {
                super::write_json(&UploadResult {
                    schema_version: 1,
                    deployment,
                    outcome,
                    organization_ref: organization,
                    input_set_id,
                    accepted_members: upload.accepted_members,
                    verification_required_members: upload.verification_required_members,
                })?;
            } else {
                writeln!(
                    io::stdout().lock(),
                    "✓ Run Input Set upload finished.\n\ninput set: {input_set_id}\naccepted members: {}\nverification required members: {}\norganization: {organization}\ndeployment: {deployment}{}",
                    upload.accepted_members,
                    upload.verification_required_members,
                    if verification_required {
                        "\n\nSeal this input set to verify members reported as already present."
                    } else {
                        ""
                    }
                )?;
            }
            Ok(())
        },
    )
}

// Deletion has an empty success value and a mutation receipt; upload keeps member counts and a
// verification-required outcome, so explicit closures make the two result contracts reviewable.
// jscpd:ignore-start
fn write_mutation_result(
    deployment: &str,
    organization: &str,
    input_set_id: &str,
    outcome: &'static str,
    result: Result<(), RunFailure>,
    authentication: super::super::PrincipalAuthenticationKind,
    json: bool,
) -> anyhow::Result<ExitCode> {
    write_input_set_outcome(
        deployment,
        organization,
        Some(input_set_id),
        result,
        authentication,
        json,
        |()| {
            if json {
                super::write_json(&MutationResult {
                    schema_version: 1,
                    deployment,
                    outcome,
                    organization_ref: organization,
                    input_set_id,
                })?;
            } else {
                writeln!(
                    io::stdout().lock(),
                    "✓ Run Input Set made logically unavailable; content cleanup scheduled.\n\ninput set: {input_set_id}\norganization: {organization}\ndeployment: {deployment}"
                )?;
            }
            Ok(())
        },
    )
}
// jscpd:ignore-end

fn write_input_set_outcome<T>(
    deployment: &str,
    organization: &str,
    input_set_id: Option<&str>,
    result: Result<T, RunFailure>,
    authentication: super::super::PrincipalAuthenticationKind,
    json: bool,
    write_success: impl FnOnce(T) -> anyhow::Result<()>,
) -> anyhow::Result<ExitCode> {
    super::write_api_outcome(result, write_success, |failure| {
        write_input_set_failure(
            deployment,
            organization,
            input_set_id,
            failure,
            authentication,
            json,
        )
    })
}

fn write_input_set_failure(
    deployment: &str,
    organization: &str,
    input_set_id: Option<&str>,
    failure: &RunFailure,
    authentication: super::super::PrincipalAuthenticationKind,
    json: bool,
) -> anyhow::Result<ExitCode> {
    super::write_failure_with_input_set(
        deployment,
        organization,
        None,
        input_set_id,
        failure,
        authentication,
        json,
    )
}

fn acquire_required_inputs<R>(
    arguments: &super::super::NamedInputArgs,
    deployment: &str,
    organization: &str,
    empty_diagnostic: &str,
    json: bool,
    service_api_key_from_stdin: bool,
    control: &super::super::OperationControl<R>,
) -> Result<Option<AcquiredInputs>, super::super::CommandFailure> {
    if let Err(failure) =
        acquisition::validate_standard_input_claims(arguments, None, service_api_key_from_stdin)
    {
        super::finish_operation(control, || {
            super::write_input_acquisition_failure(deployment, organization, &failure, json)
        })?;
        return Ok(None);
    }
    match acquisition::acquire(arguments) {
        Ok(acquired) if !acquired.manifest.inputs.is_empty() => Ok(Some(acquired)),
        Ok(_) => {
            super::finish_operation(control, || {
                write_local_failure(deployment, organization, empty_diagnostic, json)
            })?;
            Ok(None)
        }
        Err(failure) => {
            super::finish_operation(control, || {
                super::write_input_acquisition_failure(deployment, organization, &failure, json)
            })?;
            Ok(None)
        }
    }
}

fn write_local_failure(
    deployment: &str,
    organization: &str,
    diagnostic: &str,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if json {
        super::write_json(&MutationResult {
            schema_version: 1,
            deployment,
            outcome: "invalid_input",
            organization_ref: organization,
            input_set_id: "",
        })?;
    } else {
        writeln!(io::stderr().lock(), "error: {diagnostic}")?;
    }
    Ok(OutcomeClass::GeneralFailure.exit_code())
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InputSetResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    input_set: &'a RunInputSet,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UploadResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    organization_ref: &'a str,
    input_set_id: &'a str,
    accepted_members: usize,
    verification_required_members: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MutationResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    organization_ref: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    input_set_id: &'a str,
}
