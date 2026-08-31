use std::ffi::OsStr;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;

use super::harness_installation::{
    ExecutableValidationFailure, HarnessInstallationProfile, ProbeIsolation, StableVersion,
    ValidatedInstallationParts, discover_and_validate_installation, parse_probe_line,
    validate_installation_with as validate_shared_installation, validate_selected_installation,
};
use crate::process::{CommandOutput, CommandRunner, SystemCommandRunner};

pub(crate) const CODEX_APP_SERVER_V1_SUPPORTED_RANGE: &str = ">=0.147.0 <0.150.0";
pub(crate) const CODEX_APP_SERVER_V1_QUALIFICATION_VERSION: &str = "0.149.0";
const CODEX_APP_SERVER_V1_MINIMUM_VERSION: (u64, u64, u64) = (0, 147, 0);
const CODEX_APP_SERVER_V1_MAXIMUM_VERSION: (u64, u64, u64) = (0, 150, 0);
const CAPABILITY_PROBE_ARGUMENTS: [&str; 4] =
    ["app-server", "generate-json-schema", "--out", "../schemas"];
const MAXIMUM_SCHEMA_FILE_BYTES: u64 = 2 * 1024 * 1024;
const REQUIRED_SCHEMA_FILES: [&str; 9] = [
    "ClientNotification.json",
    "ClientRequest.json",
    "ServerNotification.json",
    "ServerRequest.json",
    "v1/InitializeParams.json",
    "v2/ThreadStartParams.json",
    "v2/ThreadStartResponse.json",
    "v2/TurnInterruptParams.json",
    "v2/TurnStartParams.json",
];
const CLIENT_REQUEST_METHODS: [&str; 5] = [
    "initialize",
    "config/read",
    "thread/start",
    "turn/start",
    "turn/interrupt",
];
const SERVER_REQUEST_METHODS: [&str; 5] = [
    "item/commandExecution/requestApproval",
    "item/fileChange/requestApproval",
    "item/permissions/requestApproval",
    "item/tool/requestUserInput",
    "mcpServer/elicitation/request",
];
const SERVER_NOTIFICATION_METHODS: [&str; 11] = [
    "error",
    "hook/completed",
    "hook/started",
    "item/agentMessage/delta",
    "item/completed",
    "item/started",
    "mcpServer/startupStatus/updated",
    "thread/started",
    "turn/completed",
    "turn/started",
    "warning",
];
const THREAD_START_PROPERTIES: [&str; 8] = [
    "approvalPolicy",
    "config",
    "cwd",
    "developerInstructions",
    "ephemeral",
    "model",
    "modelProvider",
    "sandbox",
];
const TURN_START_PROPERTIES: [&str; 8] = [
    "approvalPolicy",
    "cwd",
    "effort",
    "input",
    "model",
    "outputSchema",
    "sandboxPolicy",
    "threadId",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CodexCompatibilityProfile {
    CodexAppServerV1,
}

impl CodexCompatibilityProfile {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::CodexAppServerV1 => "CodexAppServerV1",
        }
    }
}

pub(crate) type CodexVersion = StableVersion;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CodexCapability {
    AppServerSchemaV1,
}

impl CodexCapability {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::AppServerSchemaV1 => "app_server_schema_v1",
        }
    }
}

const REQUIRED_CAPABILITIES: [CodexCapability; 1] = [CodexCapability::AppServerSchemaV1];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CodexAppServerV1Capabilities {
    required: [CodexCapability; REQUIRED_CAPABILITIES.len()],
}

impl CodexAppServerV1Capabilities {
    pub(crate) fn required(&self) -> &[CodexCapability] {
        &self.required
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedCodexInstallation {
    identity: CodexInstallationIdentity,
    capabilities: CodexAppServerV1Capabilities,
}

impl ValidatedCodexInstallation {
    #[cfg(test)]
    pub(crate) fn fixture(executable: PathBuf) -> Self {
        Self {
            identity: CodexInstallationIdentity::from_parts(
                executable,
                CodexVersion::parse(CODEX_APP_SERVER_V1_QUALIFICATION_VERSION).unwrap(),
                CodexCompatibilityProfile::CodexAppServerV1,
            ),
            capabilities: CodexAppServerV1Capabilities {
                required: REQUIRED_CAPABILITIES,
            },
        }
    }

    pub(crate) fn executable(&self) -> &Path {
        self.identity.executable()
    }

    pub(crate) const fn version(&self) -> &CodexVersion {
        self.identity.version()
    }

    pub(crate) const fn profile(&self) -> CodexCompatibilityProfile {
        self.identity.profile()
    }

    pub(crate) const fn capabilities(&self) -> &CodexAppServerV1Capabilities {
        &self.capabilities
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CodexProbe {
    Version,
    AppServerSchema,
}

impl CodexProbe {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Version => "version",
            Self::AppServerSchema => "app_server_schema",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CodexIncompatibility {
    Version(String),
    Capability(CodexCapability),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CodexInstallationIdentity {
    executable: PathBuf,
    version: CodexVersion,
    profile: CodexCompatibilityProfile,
}

impl CodexInstallationIdentity {
    pub(crate) fn new(
        executable: &Path,
        version: &CodexVersion,
        profile: CodexCompatibilityProfile,
    ) -> Self {
        Self::from_parts(executable.to_owned(), version.clone(), profile)
    }

    fn from_parts(
        executable: PathBuf,
        version: CodexVersion,
        profile: CodexCompatibilityProfile,
    ) -> Self {
        Self {
            executable,
            version,
            profile,
        }
    }

    // Codex failures retain this identity without coupling their contract to Pi or Claude.
    // jscpd:ignore-start
    pub(crate) fn executable(&self) -> &Path {
        &self.executable
    }

    pub(crate) const fn version(&self) -> &CodexVersion {
        &self.version
    }

    pub(crate) const fn profile(&self) -> CodexCompatibilityProfile {
        self.profile
    }
    // jscpd:ignore-end
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CodexInstallationFailure {
    Missing,
    Unexecutable {
        identity: Option<CodexInstallationIdentity>,
    },
    Malformed {
        probe: CodexProbe,
        identity: Option<CodexInstallationIdentity>,
    },
    Unsupported {
        incompatibility: CodexIncompatibility,
        identity: Option<CodexInstallationIdentity>,
    },
}

impl CodexInstallationFailure {
    pub(crate) fn identity(&self) -> Option<&CodexInstallationIdentity> {
        match self {
            Self::Missing => None,
            Self::Unexecutable { identity }
            | Self::Malformed { identity, .. }
            | Self::Unsupported { identity, .. } => identity.as_ref(),
        }
    }

    fn with_identity(mut self, candidate: CodexInstallationIdentity) -> Self {
        match &mut self {
            Self::Missing => {}
            Self::Unexecutable { identity }
            | Self::Malformed { identity, .. }
            | Self::Unsupported { identity, .. } => *identity = Some(candidate),
        }
        self
    }
}

impl fmt::Display for CodexInstallationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing => write!(
                formatter,
                "Codex was not found in inherited PATH; install a stable Codex CLI release in range {CODEX_APP_SERVER_V1_SUPPORTED_RANGE}"
            ),
            Self::Unexecutable { .. } => formatter.write_str(
                "Codex selected from inherited PATH could not complete its validation probes",
            ),
            Self::Malformed {
                probe: CodexProbe::Version,
                ..
            } => formatter.write_str(
                "Codex selected from inherited PATH returned a malformed stable version",
            ),
            Self::Malformed {
                probe: CodexProbe::AppServerSchema,
                ..
            } => formatter.write_str(
                "Codex selected from inherited PATH returned malformed App Server schemas",
            ),
            Self::Unsupported {
                incompatibility: CodexIncompatibility::Version(version),
                ..
            } => write!(
                formatter,
                "Codex version {version} selected from inherited PATH is unsupported; install a stable Codex CLI release in range {CODEX_APP_SERVER_V1_SUPPORTED_RANGE}"
            ),
            Self::Unsupported {
                incompatibility: CodexIncompatibility::Capability(capability),
                ..
            } => write!(
                formatter,
                "Codex selected from inherited PATH lacks the required {} capability",
                capability.as_str()
            ),
        }
    }
}

impl std::error::Error for CodexInstallationFailure {}

struct CodexInstallationProfile;

impl HarnessInstallationProfile for CodexInstallationProfile {
    type Version = CodexVersion;
    type CompatibilityProfile = CodexCompatibilityProfile;
    type Capabilities = CodexAppServerV1Capabilities;
    type Installation = ValidatedCodexInstallation;
    type Failure = CodexInstallationFailure;

    const EXECUTABLE_NAME: &'static str = "codex";
    const CAPABILITY_PROBE_ARGUMENTS: &'static [&'static str] = &CAPABILITY_PROBE_ARGUMENTS;

    // Codex owns its isolated native-state directories independently of Pi's trust policy.
    // jscpd:ignore-start
    fn probe_isolation(search_path: &OsStr) -> Result<ProbeIsolation, Self::Failure> {
        let mut isolation = ProbeIsolation::create(
            "scherzo-codex-validation-",
            &["codex-home", "schemas", "schemas/v1", "schemas/v2"],
            search_path,
        )
        .map_err(|()| Self::unexecutable_failure())?;
        isolation.add_directory_environment("CODEX_HOME", "codex-home");
        Ok(isolation)
    }
    // jscpd:ignore-end

    // Keep release-line failure mappings profile-local so Codex review cannot alter Pi admission.
    // jscpd:ignore-start
    fn parse_version_output(output: &CommandOutput) -> Result<Self::Version, Self::Failure> {
        parse_version_output(output)
    }

    fn compatibility_profile(version: &Self::Version) -> Option<Self::CompatibilityProfile> {
        compatibility_profile(version)
    }

    fn unsupported_version(version: &Self::Version) -> Self::Failure {
        CodexInstallationFailure::Unsupported {
            incompatibility: CodexIncompatibility::Version(version.as_str().to_owned()),
            identity: None,
        }
    }
    // jscpd:ignore-end

    fn validate_capability_output(
        output: &CommandOutput,
        isolation: &ProbeIsolation,
        executable: &Path,
        version: &Self::Version,
        profile: &Self::CompatibilityProfile,
    ) -> Result<Self::Capabilities, Self::Failure> {
        let identity = || CodexInstallationIdentity::new(executable, version, *profile);
        if output.truncated {
            return Err(CodexInstallationFailure::Malformed {
                probe: CodexProbe::AppServerSchema,
                identity: Some(identity()),
            });
        }
        validate_schema_directory(&isolation.directory("schemas"))
            .map_err(|failure| failure.with_identity(identity()))?;
        Ok(CodexAppServerV1Capabilities {
            required: REQUIRED_CAPABILITIES,
        })
    }

    fn capability_probe_failure(
        executable: &Path,
        version: &Self::Version,
        profile: &Self::CompatibilityProfile,
    ) -> Self::Failure {
        CodexInstallationFailure::Unexecutable {
            identity: Some(CodexInstallationIdentity::new(
                executable, version, *profile,
            )),
        }
    }

    // Construction and executable failures remain explicit parts of the closed Codex profile.
    // jscpd:ignore-start
    fn installation(
        parts: ValidatedInstallationParts<
            Self::Version,
            Self::CompatibilityProfile,
            Self::Capabilities,
        >,
    ) -> Self::Installation {
        let (executable, version, profile, capabilities) = parts.into_parts();
        ValidatedCodexInstallation {
            identity: CodexInstallationIdentity::from_parts(executable, version, profile),
            capabilities,
        }
    }

    fn executable_failure(failure: ExecutableValidationFailure) -> Self::Failure {
        match failure {
            ExecutableValidationFailure::Missing => CodexInstallationFailure::Missing,
            ExecutableValidationFailure::Unexecutable => {
                CodexInstallationFailure::Unexecutable { identity: None }
            }
        }
    }

    fn unexecutable_failure() -> Self::Failure {
        CodexInstallationFailure::Unexecutable { identity: None }
    }
    // jscpd:ignore-end
}

pub(crate) fn discover_and_validate_codex_installation()
-> Result<ValidatedCodexInstallation, CodexInstallationFailure> {
    discover_and_validate_installation::<CodexInstallationProfile>(&SystemCommandRunner)
}

pub(crate) fn validate_codex_installation(
    selected_executable: &Path,
) -> Result<ValidatedCodexInstallation, CodexInstallationFailure> {
    validate_selected_installation::<CodexInstallationProfile>(
        selected_executable,
        &SystemCommandRunner,
    )
}

fn validate_codex_installation_with(
    selected_executable: &Path,
    search_path: &OsStr,
    runner: &dyn CommandRunner,
) -> Result<ValidatedCodexInstallation, CodexInstallationFailure> {
    validate_shared_installation::<CodexInstallationProfile>(
        selected_executable,
        search_path,
        runner,
    )
}

fn parse_version_output(output: &CommandOutput) -> Result<CodexVersion, CodexInstallationFailure> {
    let observed = parse_probe_line(output)
        .and_then(|line| line.strip_prefix("codex-cli "))
        .ok_or(CodexInstallationFailure::Malformed {
            probe: CodexProbe::Version,
            identity: None,
        })?;
    CodexVersion::parse(observed).ok_or(CodexInstallationFailure::Malformed {
        probe: CodexProbe::Version,
        identity: None,
    })
}

fn compatibility_profile(version: &CodexVersion) -> Option<CodexCompatibilityProfile> {
    (version.numeric() >= CODEX_APP_SERVER_V1_MINIMUM_VERSION
        && version.numeric() < CODEX_APP_SERVER_V1_MAXIMUM_VERSION)
        .then_some(CodexCompatibilityProfile::CodexAppServerV1)
}

pub(crate) fn compatibility_profile_for_version(
    observed: &str,
) -> Option<CodexCompatibilityProfile> {
    compatibility_profile(&CodexVersion::parse(observed)?)
}

fn validate_schema_directory(directory: &Path) -> Result<(), CodexInstallationFailure> {
    let schemas = REQUIRED_SCHEMA_FILES
        .map(|relative| read_schema(directory, relative))
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    let [
        client_notification,
        client_request,
        server_notification,
        server_request,
        _,
        thread_start,
        thread_start_response,
        _,
        turn_start,
    ] = schemas.as_slice()
    else {
        return Err(malformed_schema());
    };

    let compatible = CLIENT_REQUEST_METHODS
        .iter()
        .all(|method| has_method(client_request, method))
        && has_method(client_notification, "initialized")
        && SERVER_REQUEST_METHODS
            .iter()
            .all(|method| has_method(server_request, method))
        && SERVER_NOTIFICATION_METHODS
            .iter()
            .all(|method| has_method(server_notification, method))
        && has_properties(thread_start, &THREAD_START_PROPERTIES)
        && has_ephemeral_thread_contract(thread_start_response)
        && schema_allows_literal(
            thread_start.pointer("/definitions/SandboxMode"),
            "danger-full-access",
        )
        && has_properties(turn_start, &TURN_START_PROPERTIES)
        && has_external_sandbox_policy(turn_start)
        && ["text", "localImage", "mention"]
            .iter()
            .all(|input_type| has_input_type(turn_start, input_type))
        && is_nonempty_reasoning_effort(turn_start);

    compatible
        .then_some(())
        .ok_or(CodexInstallationFailure::Unsupported {
            incompatibility: CodexIncompatibility::Capability(CodexCapability::AppServerSchemaV1),
            identity: None,
        })
}

fn has_ephemeral_thread_contract(thread_start_response: &Value) -> bool {
    has_properties(thread_start_response, &["thread"])
        && thread_start_response
            .pointer("/definitions/Thread")
            .is_some_and(|thread| {
                has_properties(
                    thread,
                    &["cliVersion", "ephemeral", "id", "sessionId", "turns"],
                )
            })
}

fn read_schema(directory: &Path, relative: &str) -> Result<Value, CodexInstallationFailure> {
    let path = directory.join(relative);
    let metadata = fs::symlink_metadata(&path).map_err(|_| malformed_schema())?;
    if !metadata.file_type().is_file() || metadata.len() > MAXIMUM_SCHEMA_FILE_BYTES {
        return Err(malformed_schema());
    }
    let bytes = fs::read(path).map_err(|_| malformed_schema())?;
    serde_json::from_slice(&bytes).map_err(|_| malformed_schema())
}

fn malformed_schema() -> CodexInstallationFailure {
    CodexInstallationFailure::Malformed {
        probe: CodexProbe::AppServerSchema,
        identity: None,
    }
}

fn has_method(schema: &Value, expected: &str) -> bool {
    any_nested_value(schema, |value| {
        value
            .as_object()
            .and_then(|object| object.get("method"))
            .is_some_and(|method| schema_allows_literal(Some(method), expected))
    })
}

fn has_input_type(schema: &Value, expected: &str) -> bool {
    any_nested_value(schema, |value| {
        value
            .pointer("/properties/type")
            .is_some_and(|kind| schema_allows_literal(Some(kind), expected))
    })
}

fn any_nested_value(schema: &Value, predicate: impl Fn(&Value) -> bool) -> bool {
    let mut pending = vec![schema];
    while let Some(value) = pending.pop() {
        if predicate(value) {
            return true;
        }
        match value {
            Value::Array(values) => pending.extend(values),
            Value::Object(values) => pending.extend(values.values()),
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
    }
    false
}

fn schema_allows_literal(schema: Option<&Value>, expected: &str) -> bool {
    let Some(schema) = schema else {
        return false;
    };
    schema.get("const").and_then(Value::as_str) == Some(expected)
        || schema
            .get("enum")
            .and_then(Value::as_array)
            .is_some_and(|values| values.iter().any(|value| value.as_str() == Some(expected)))
}

fn has_properties(schema: &Value, required: &[&str]) -> bool {
    schema
        .get("properties")
        .and_then(Value::as_object)
        .is_some_and(|properties| required.iter().all(|name| properties.contains_key(*name)))
}

fn has_external_sandbox_policy(turn_start: &Value) -> bool {
    turn_start
        .pointer("/definitions/SandboxPolicy/oneOf")
        .and_then(Value::as_array)
        .is_some_and(|variants| {
            variants.iter().any(|variant| {
                variant
                    .pointer("/properties/type")
                    .is_some_and(|kind| schema_allows_literal(Some(kind), "externalSandbox"))
                    && variant
                        .get("properties")
                        .and_then(Value::as_object)
                        .is_some_and(|properties| properties.contains_key("networkAccess"))
            })
        })
}

fn is_nonempty_reasoning_effort(turn_start: &Value) -> bool {
    turn_start
        .pointer("/definitions/ReasoningEffort")
        .is_some_and(|effort| {
            effort.get("type").and_then(Value::as_str) == Some("string")
                && effort.get("minLength").and_then(Value::as_u64) == Some(1)
        })
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use super::*;
    use crate::process::{CommandProbeError, CommandRequest};

    struct FakeRunner {
        invocations: Mutex<Vec<Vec<String>>>,
        version: CommandOutput,
        schema_compatible: bool,
        capability_success: bool,
    }

    impl CommandRunner for FakeRunner {
        fn run(&self, command: CommandRequest<'_>) -> Result<CommandOutput, CommandProbeError> {
            assert!(command.program.is_absolute());
            assert_eq!(command.timeout, Duration::from_secs(30));
            assert!(command.clear_environment);
            assert_isolated(command.environment, command.current_directory);
            self.invocations.lock().unwrap().push(
                command
                    .args
                    .iter()
                    .map(|argument| (*argument).to_owned())
                    .collect(),
            );
            match command.args {
                ["--version"] => {
                    assert_eq!(command.maximum_stdout_bytes, 4 * 1024);
                    Ok(self.version.clone())
                }
                args if args == CAPABILITY_PROBE_ARGUMENTS => {
                    assert_eq!(command.maximum_stdout_bytes, 64 * 1024);
                    if self.capability_success {
                        let current_directory = command.current_directory.unwrap();
                        let schemas = current_directory.parent().unwrap().join("schemas");
                        copy_schema_fixture(&schemas);
                        if !self.schema_compatible {
                            fs::write(schemas.join("ServerNotification.json"), b"{}").unwrap();
                        }
                    }
                    Ok(CommandOutput {
                        success: self.capability_success,
                        stdout: Vec::new(),
                        truncated: false,
                    })
                }
                _ => Err(CommandProbeError::Spawn),
            }
        }
    }

    fn output(bytes: &[u8]) -> CommandOutput {
        CommandOutput {
            success: true,
            stdout: bytes.to_vec(),
            truncated: false,
        }
    }

    fn compatible_runner(version: &str) -> FakeRunner {
        FakeRunner {
            invocations: Mutex::new(Vec::new()),
            version: output(format!("codex-cli {version}\n").as_bytes()),
            schema_compatible: true,
            capability_success: true,
        }
    }

    fn copy_schema_fixture(destination: &Path) {
        let fixture =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/codex-app-server-v1-schema");
        for relative in REQUIRED_SCHEMA_FILES {
            fs::copy(fixture.join(relative), destination.join(relative)).unwrap();
        }
    }

    fn assert_isolated(environment: &[(&OsStr, &OsStr)], current_directory: Option<&Path>) {
        let current_directory = current_directory.unwrap();
        let root = current_directory.parent().unwrap();
        assert_eq!(current_directory, root.join("project"));
        assert_eq!(environment.len(), 9);
        for (name, expected) in [
            ("HOME", root.join("home")),
            ("CODEX_HOME", root.join("codex-home")),
            ("XDG_CONFIG_HOME", root.join("config")),
            ("XDG_CACHE_HOME", root.join("cache")),
            ("XDG_DATA_HOME", root.join("data")),
            ("XDG_STATE_HOME", root.join("state")),
        ] {
            assert_eq!(
                environment
                    .iter()
                    .find(|(candidate, _)| *candidate == OsStr::new(name))
                    .map(|(_, value)| *value),
                Some(expected.as_os_str())
            );
        }
        assert_eq!(
            environment
                .iter()
                .find(|(candidate, _)| *candidate == OsStr::new("PATH"))
                .map(|(_, value)| *value),
            Some(OsStr::new("/controlled/bin"))
        );
    }

    #[test]
    fn stable_release_line_and_schema_construct_the_exact_installation_identity() {
        let executable = std::env::current_exe().unwrap();
        for version in [
            "0.147.0",
            "0.147.1",
            "0.147.999",
            "0.148.0",
            "0.148.1",
            "0.148.999",
            "0.149.0",
            "0.149.1",
            "0.149.999",
        ] {
            let runner = compatible_runner(version);
            let installation = validate_codex_installation_with(
                &executable,
                OsStr::new("/controlled/bin"),
                &runner,
            )
            .unwrap();

            assert_eq!(
                installation.executable(),
                fs::canonicalize(&executable).unwrap()
            );
            assert_eq!(installation.version().as_str(), version);
            assert_eq!(
                installation.profile(),
                CodexCompatibilityProfile::CodexAppServerV1
            );
            assert_eq!(
                installation.capabilities().required(),
                REQUIRED_CAPABILITIES
            );
            assert_eq!(
                *runner.invocations.lock().unwrap(),
                [
                    vec!["--version".to_owned()],
                    CAPABILITY_PROBE_ARGUMENTS.map(str::to_owned).to_vec(),
                ]
            );
        }
    }

    #[test]
    fn admission_rejects_versions_outside_the_undecorated_stable_release_line() {
        let executable = std::env::current_exe().unwrap();
        for unsupported in ["0.146.999", "0.150.0", "1.147.0"] {
            let runner = compatible_runner(unsupported);
            assert_eq!(
                validate_codex_installation_with(
                    &executable,
                    OsStr::new("/controlled/bin"),
                    &runner,
                ),
                Err(CodexInstallationFailure::Unsupported {
                    incompatibility: CodexIncompatibility::Version(unsupported.to_owned()),
                    identity: None,
                })
            );
            assert_eq!(
                *runner.invocations.lock().unwrap(),
                [vec!["--version".to_owned()]]
            );
        }

        for malformed in [
            "0.147.0-rc.1",
            "0.147.0+build.1",
            "v0.147.0",
            "00.147.0",
            "0.147",
            "0.147.0 (Codex CLI)",
        ] {
            let runner = compatible_runner(malformed);
            assert_eq!(
                validate_codex_installation_with(
                    &executable,
                    OsStr::new("/controlled/bin"),
                    &runner,
                ),
                Err(CodexInstallationFailure::Malformed {
                    probe: CodexProbe::Version,
                    identity: None,
                }),
                "{malformed}"
            );
        }
    }

    #[test]
    fn maintained_schema_probe_is_required_after_version_admission() {
        let executable = std::env::current_exe().unwrap();
        let incompatible = FakeRunner {
            schema_compatible: false,
            ..compatible_runner("0.147.0")
        };
        let canonical = fs::canonicalize(&executable).unwrap();
        let identity = || {
            Some(CodexInstallationIdentity::new(
                &canonical,
                &CodexVersion::parse("0.147.0").unwrap(),
                CodexCompatibilityProfile::CodexAppServerV1,
            ))
        };
        assert_eq!(
            validate_codex_installation_with(
                &executable,
                OsStr::new("/controlled/bin"),
                &incompatible,
            ),
            Err(CodexInstallationFailure::Unsupported {
                incompatibility: CodexIncompatibility::Capability(
                    CodexCapability::AppServerSchemaV1,
                ),
                identity: identity(),
            })
        );

        let failed = FakeRunner {
            capability_success: false,
            ..compatible_runner("0.147.0")
        };
        assert_eq!(
            validate_codex_installation_with(&executable, OsStr::new("/controlled/bin"), &failed,),
            Err(CodexInstallationFailure::Unexecutable {
                identity: identity(),
            })
        );
    }
}
