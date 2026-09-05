use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::process::{Command, Stdio};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use rustix::process::Pid;

use super::*;
use crate::execution::workflow::admission::{
    CancellationPolicy, CancellationSource, ExecutionContext, ResolvedImports, admit_workflow,
    default_execution_policy_limits,
};
use crate::execution::workflow::artifact::{
    CaptureBoundary, CaptureBoundaryObserver, CarrierBudgetClass,
};
use crate::execution::workflow::git_artifact::{
    GitArtifactDescriptor, GitArtifactValidationBudget, validate_git_bundle,
};
use crate::execution::workflow::resolution;

const WORKFLOW: &str =
    "schemaVersion: 1\nsteps:\n  inspect:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";

struct GitFixture {
    _temporary: tempfile::TempDir,
    repository: PathBuf,
    artifacts: ArtifactStaging,
    capture: GitCaptureContext,
}

impl GitFixture {
    fn new() -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        init_repository(&repository);
        fs::write(repository.join("tracked.txt"), b"baseline\n").unwrap();
        git(&repository, &["add", "tracked.txt"]);
        git(&repository, &["commit", "--quiet", "-m", "baseline"]);
        Self::from_prepared(temporary, repository)
    }

    fn from_prepared(temporary: tempfile::TempDir, repository: PathBuf) -> Self {
        Self::from_prepared_with_environment(temporary, repository, [])
    }

    fn from_prepared_with_environment<const N: usize>(
        temporary: tempfile::TempDir,
        repository: PathBuf,
        additional_environment: [(&str, &OsStr); N],
    ) -> Self {
        let (admitted, artifacts) =
            admitted_capture(temporary.path(), &repository, additional_environment);
        let capture =
            GitCaptureContext::admit(admitted.execution(), &CaptureCancellation::default())
                .unwrap();
        Self {
            _temporary: temporary,
            repository,
            artifacts,
            capture,
        }
    }

    fn commit_file(&self, path: &str, bytes: &[u8], message: &str) {
        fs::write(self.repository.join(path), bytes).unwrap();
        git(&self.repository, &["add", path]);
        git(&self.repository, &["commit", "--quiet", "-m", message]);
    }

    fn capture(&self) -> Result<CaptureCandidateSet, GitCaptureFailure> {
        self.capture
            .capture("changes", &self.artifacts, &CaptureCancellation::default())
    }
}

fn admitted_capture<const N: usize>(
    root: &Path,
    repository: &Path,
    additional_environment: [(&str, &OsStr); N],
) -> (
    crate::execution::workflow::admission::AdmittedWorkflow,
    ArtifactStaging,
) {
    let source = root.join(format!("workflow-{N}"));
    let staging = root.join(format!("staging-{N}"));
    fs::create_dir(&source).unwrap();
    fs::create_dir(&staging).unwrap();
    fs::write(source.join("workflow.yaml"), WORKFLOW).unwrap();
    let mut environment = std::env::vars_os().collect::<BTreeMap<_, _>>();
    for (name, value) in additional_environment {
        environment.insert(OsString::from(name), value.to_owned());
    }
    let admitted = admit_workflow(
        resolution::resolve(&source, Path::new("workflow.yaml")).unwrap(),
        ResolvedImports::default(),
        ExecutionContext::new(
            repository.to_owned(),
            default_execution_policy_limits(1),
            EnvironmentSnapshot::new(environment),
            CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(1)),
        ),
    )
    .unwrap();
    let artifacts = ArtifactStaging::create(admitted.execution(), &staging).unwrap();
    (admitted, artifacts)
}

fn git_executable() -> &'static Path {
    static GIT_EXECUTABLE: OnceLock<PathBuf> = OnceLock::new();
    GIT_EXECUTABLE.get_or_init(|| {
        let path = std::env::var_os("PATH").expect("test PATH must contain Git");
        std::env::split_paths(&path)
            .map(|directory| directory.join("git"))
            .find(|candidate| {
                candidate.metadata().is_ok_and(|metadata| {
                    metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
                })
            })
            .expect("Git must be available to Git capture tests")
    })
}

fn init_repository(path: &Path) {
    git_parent(&["init", "--quiet", path.to_str().unwrap()]);
    git(path, &["config", "user.name", "Scherzo Test"]);
    git(path, &["config", "user.email", "test@example.invalid"]);
}

fn fixture_git_command() -> Command {
    crate::test_support::fixture_git_command(git_executable())
}

fn git(repository: &Path, arguments: &[&str]) -> String {
    let output = fixture_git_command()
        .arg("-C")
        .arg(repository)
        .args(arguments)
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

fn git_parent(arguments: &[&str]) -> String {
    let output = fixture_git_command().args(arguments).output().unwrap();
    assert!(
        output.status.success(),
        "git {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

fn create_fifo(path: &Path) {
    assert!(Command::new("mkfifo").arg(path).status().unwrap().success());
}

fn add_clean_submodule(repository: &Path, source: &Path) {
    init_repository(source);
    fs::write(source.join("nested.txt"), b"clean\n").unwrap();
    git(source, &["add", "nested.txt"]);
    git(source, &["commit", "--quiet", "-m", "nested"]);
    git(
        repository,
        &[
            "-c",
            "protocol.file.allow=always",
            "submodule",
            "add",
            "--quiet",
            source.to_str().unwrap(),
            "nested",
        ],
    );
    git(repository, &["commit", "--quiet", "-am", "add submodule"]);
}

fn git_oid(repository: &Path, expression: &str) -> String {
    git(repository, &["rev-parse", expression])
        .trim()
        .to_owned()
}

fn read_carrier(artifacts: &ArtifactStaging, candidates: &CaptureCandidateSet) -> Vec<u8> {
    let carrier = candidates.outputs()["changes"]
        .as_git_branch()
        .unwrap()
        .carrier()
        .unwrap();
    let mut bytes = Vec::new();
    artifacts.copy_to(carrier.handle(), &mut bytes).unwrap();
    bytes
}

fn capture_failure(result: Result<CaptureCandidateSet, GitCaptureFailure>) -> GitCaptureFailure {
    match result {
        Err(failure) => failure,
        Ok(candidates) => {
            candidates.abort();
            panic!("Git capture unexpectedly succeeded")
        }
    }
}

#[test]
fn changed_capture_has_verified_bundle_profile_metadata_size_and_digest() {
    let fixture = GitFixture::new();
    let baseline = fixture.capture.baseline_oid().to_owned();
    fixture.commit_file("tracked.txt", b"changed\n", "change");
    let head = git_oid(&fixture.repository, "HEAD");
    let tree = git_oid(&fixture.repository, "HEAD^{tree}");

    let candidates = fixture.capture().unwrap();
    let branch = candidates.outputs()["changes"].as_git_branch().unwrap();
    assert_eq!(branch.metadata().artifact_version(), 1);
    assert_eq!(branch.metadata().object_format(), GitObjectFormat::Sha1);
    assert_eq!(branch.metadata().base_oid(), baseline);
    assert_eq!(branch.metadata().head_oid(), head);
    assert_eq!(branch.metadata().tree_oid(), tree);
    let carrier = branch.carrier().unwrap();
    assert_eq!(carrier.media_type(), BUNDLE_MEDIA_TYPE);
    assert_eq!(carrier.budget_class(), CarrierBudgetClass::Git);
    let bytes = read_carrier(&fixture.artifacts, &candidates);
    assert_eq!(carrier.size(), u64::try_from(bytes.len()).unwrap());
    assert_eq!(
        carrier.sha256(),
        lowercase_hex(ring::digest::digest(&SHA256, &bytes).as_ref())
    );
    assert_eq!(
        &bytes[..bundle_header(&baseline, &head).len()],
        bundle_header(&baseline, &head)
    );
    let mut bundle = tempfile::tempfile().unwrap();
    bundle.write_all(&bytes).unwrap();
    assert_eq!(
        validate_git_bundle(
            &mut bundle,
            GitArtifactDescriptor {
                base_oid: &baseline,
                head_oid: &head,
                tree_oid: &tree,
            },
            &mut GitArtifactValidationBudget::default(),
            &AtomicBool::new(false),
        ),
        Ok(())
    );
    let body = &bytes[bundle_header(&baseline, &head).len()..];
    assert_eq!(&body[..4], b"PACK");
    assert_eq!(u32::from_be_bytes(body[4..8].try_into().unwrap()), 2);
    assert_eq!(
        git(
            &fixture.repository,
            &["for-each-ref", "--format=%(refname)", "refs/scherzo"]
        ),
        ""
    );
    assert_eq!(git(&fixture.repository, &["status", "--porcelain=v1"]), "");

    assert_eq!(fixture.artifacts.git_reservation_usage().0, 1);
    let carrier_size = carrier.size();
    let outputs = candidates.commit();
    assert_eq!(fixture.artifacts.git_budget_usage(), (1, carrier_size));
    drop(outputs);
    assert_eq!(fixture.artifacts.git_budget_usage(), (0, 0));
    assert_eq!(fixture.artifacts.staged_artifact_count(), 0);
}

#[test]
fn capture_accepts_tree_copied_from_baseline() {
    let temporary = tempfile::tempdir().unwrap();
    let repository = temporary.path().join("repository");
    init_repository(&repository);
    fs::create_dir_all(repository.join("source/nested")).unwrap();
    fs::create_dir_all(repository.join("target/nested")).unwrap();
    fs::write(repository.join("source/nested/data.txt"), b"reused\n").unwrap();
    fs::write(repository.join("target/nested/data.txt"), b"target\n").unwrap();
    git(&repository, &["add", "."]);
    git(&repository, &["commit", "--quiet", "-m", "baseline"]);
    let fixture = GitFixture::from_prepared(temporary, repository);
    fs::create_dir(fixture.repository.join("target/copied")).unwrap();
    fs::copy(
        fixture.repository.join("source/nested/data.txt"),
        fixture.repository.join("target/copied/data.txt"),
    )
    .unwrap();
    git(&fixture.repository, &["add", "target/copied"]);
    git(
        &fixture.repository,
        &["commit", "--quiet", "-m", "copy baseline tree"],
    );

    let candidates = fixture.capture().unwrap();

    assert!(
        candidates.outputs()["changes"]
            .as_git_branch()
            .unwrap()
            .carrier()
            .is_some()
    );
}

#[test]
fn zero_delta_carries_metadata_without_a_carrier_or_budget_charge() {
    let fixture = GitFixture::new();
    let baseline = fixture.capture.baseline_oid().to_owned();
    let tree = git_oid(&fixture.repository, "HEAD^{tree}");
    fs::write(
        fixture.repository.join(".git/info/exclude"),
        b"ignored.txt\n",
    )
    .unwrap();
    fs::write(fixture.repository.join("ignored.txt"), b"ignored\n").unwrap();

    let candidates = fixture.capture().unwrap();
    let branch = candidates.outputs()["changes"].as_git_branch().unwrap();
    assert_eq!(branch.metadata().base_oid(), baseline);
    assert_eq!(branch.metadata().head_oid(), baseline);
    assert_eq!(branch.metadata().tree_oid(), tree);
    assert!(branch.carrier().is_none());
    assert_eq!(fixture.artifacts.git_reservation_usage(), (0, 0));
    assert_eq!(fixture.artifacts.staged_artifact_count(), 0);

    let outputs = candidates.commit();
    assert_eq!(fixture.artifacts.git_budget_usage(), (0, 0));
    drop(outputs);
}

#[test]
fn admission_requires_the_execution_root_to_be_the_sha1_worktree_root() {
    let temporary = tempfile::tempdir().unwrap();
    let repository = temporary.path().join("repository");
    init_repository(&repository);
    fs::write(repository.join("base.txt"), b"base\n").unwrap();
    git(&repository, &["add", "base.txt"]);
    git(&repository, &["commit", "--quiet", "-m", "baseline"]);
    let nested = repository.join("nested");
    fs::create_dir(&nested).unwrap();
    let (admitted, _artifacts) = admitted_capture(temporary.path(), &nested, []);

    let failure =
        match GitCaptureContext::admit(admitted.execution(), &CaptureCancellation::default()) {
            Err(failure) => failure,
            Ok(_) => panic!("nested execution root unexpectedly admitted"),
        };
    assert_eq!(
        failure,
        GitWorkspaceAdmissionFailure::ExecutionRootNotWorkTreeRoot
    );

    let sha256 = temporary.path().join("sha256");
    git_parent(&[
        "init",
        "--quiet",
        "--object-format=sha256",
        sha256.to_str().unwrap(),
    ]);
    git(&sha256, &["config", "user.name", "Scherzo Test"]);
    git(&sha256, &["config", "user.email", "test@example.invalid"]);
    fs::write(sha256.join("base.txt"), b"base\n").unwrap();
    git(&sha256, &["add", "base.txt"]);
    git(&sha256, &["commit", "--quiet", "-m", "baseline"]);
    let other_root = temporary.path().join("sha256-admission");
    fs::create_dir(&other_root).unwrap();
    let (admitted, _artifacts) = admitted_capture(&other_root, &sha256, []);
    let failure =
        match GitCaptureContext::admit(admitted.execution(), &CaptureCancellation::default()) {
            Err(failure) => failure,
            Ok(_) => panic!("SHA-256 repository unexpectedly admitted"),
        };
    assert_eq!(
        failure,
        GitWorkspaceAdmissionFailure::UnsupportedObjectFormat
    );
}

#[test]
fn admission_rejects_a_clean_tracked_submodule() {
    let temporary = tempfile::tempdir().unwrap();
    let repository = temporary.path().join("repository");
    init_repository(&repository);
    fs::write(repository.join("base.txt"), b"base\n").unwrap();
    git(&repository, &["add", "base.txt"]);
    git(&repository, &["commit", "--quiet", "-m", "baseline"]);
    add_clean_submodule(&repository, &temporary.path().join("submodule-source"));
    let (admitted, _artifacts) = admitted_capture(temporary.path(), &repository, []);

    let failure =
        match GitCaptureContext::admit(admitted.execution(), &CaptureCancellation::default()) {
            Err(failure) => failure,
            Ok(_) => panic!("multi-repository workspace unexpectedly admitted"),
        };
    assert_eq!(
        failure,
        GitWorkspaceAdmissionFailure::ExecutionRootNotWorkTreeRoot
    );
}

#[test]
fn git_trace_environment_cannot_author_the_workspace() {
    let temporary = tempfile::tempdir().unwrap();
    let repository = temporary.path().join("repository");
    init_repository(&repository);
    fs::write(repository.join("base.txt"), b"base\n").unwrap();
    git(&repository, &["add", "base.txt"]);
    git(&repository, &["commit", "--quiet", "-m", "baseline"]);
    let trace = repository.join(".git/scherzo-git-trace");
    let before = repository_bytes(&repository);
    let (admitted, _artifacts) = admitted_capture(
        temporary.path(),
        &repository,
        [("GIT_TRACE", trace.as_os_str())],
    );

    GitCaptureContext::admit(admitted.execution(), &CaptureCancellation::default()).unwrap();

    assert!(
        !trace.exists(),
        "Git admission created a trace file inside .git"
    );
    assert_eq!(repository_bytes(&repository), before);
}

#[test]
fn non_descendant_head_is_rejected_without_a_carrier_reservation() {
    let fixture = GitFixture::new();
    git(
        &fixture.repository,
        &["switch", "--quiet", "--orphan", "unrelated"],
    );
    fs::write(fixture.repository.join("unrelated.txt"), b"unrelated\n").unwrap();
    git(&fixture.repository, &["add", "unrelated.txt"]);
    git(
        &fixture.repository,
        &["commit", "--quiet", "-m", "unrelated"],
    );

    assert_eq!(
        capture_failure(fixture.capture()),
        GitCaptureFailure::BaselineNotAncestor
    );
    assert_eq!(fixture.artifacts.git_reservation_usage(), (0, 0));
    assert_eq!(fixture.artifacts.staged_artifact_count(), 0);
}

#[test]
fn merge_topology_is_carried_from_the_exact_run_baseline() {
    let fixture = GitFixture::new();
    let initial_branch = git(&fixture.repository, &["branch", "--show-current"])
        .trim()
        .to_owned();
    git(&fixture.repository, &["switch", "--quiet", "-c", "side"]);
    fixture.commit_file("side.txt", b"side\n", "side");
    git(&fixture.repository, &["switch", "--quiet", &initial_branch]);
    fixture.commit_file("main.txt", b"main\n", "main");
    git(
        &fixture.repository,
        &["merge", "--quiet", "--no-ff", "side", "-m", "merge"],
    );
    let merge = git_oid(&fixture.repository, "HEAD");

    let candidates = fixture.capture().unwrap();
    assert_eq!(
        candidates.outputs()["changes"]
            .as_git_branch()
            .unwrap()
            .metadata()
            .head_oid(),
        merge
    );
    assert!(!read_carrier(&fixture.artifacts, &candidates).is_empty());
}

#[test]
fn dirty_index_worktree_untracked_and_submodule_states_fail_without_adapter_authorship() {
    for dirty in ["staged", "tracked", "untracked"] {
        let fixture = GitFixture::new();
        match dirty {
            "staged" => {
                fs::write(fixture.repository.join("tracked.txt"), b"staged\n").unwrap();
                git(&fixture.repository, &["add", "tracked.txt"]);
            }
            "tracked" => {
                fs::write(fixture.repository.join("tracked.txt"), b"tracked dirty\n").unwrap();
            }
            "untracked" => {
                fs::write(fixture.repository.join("untracked.txt"), b"untracked\n").unwrap();
            }
            _ => panic!("unknown workspace mutation fixture"),
        }
        let before = repository_bytes(&fixture.repository);
        assert_eq!(
            capture_failure(fixture.capture()),
            GitCaptureFailure::WorkspaceDirty
        );
        assert_eq!(repository_bytes(&fixture.repository), before);
        assert_eq!(fixture.artifacts.git_reservation_usage(), (0, 0));
        assert_eq!(fixture.artifacts.staged_artifact_count(), 0);
    }

    let fixture = GitFixture::new();
    let submodule_source = fixture._temporary.path().join("submodule-source");
    add_clean_submodule(&fixture.repository, &submodule_source);
    fs::write(fixture.repository.join("nested/nested.txt"), b"dirty\n").unwrap();
    let before = repository_bytes(&fixture.repository);

    assert_eq!(
        capture_failure(fixture.capture()),
        GitCaptureFailure::WorkspaceDirty
    );
    assert_eq!(repository_bytes(&fixture.repository), before);
    assert_eq!(fixture.artifacts.staged_artifact_count(), 0);
}

#[test]
fn capture_rejects_a_clean_submodule_added_after_admission() {
    let fixture = GitFixture::new();
    let submodule_source = fixture._temporary.path().join("submodule-source");
    add_clean_submodule(&fixture.repository, &submodule_source);

    assert_eq!(
        capture_failure(fixture.capture()),
        GitCaptureFailure::WorkspaceChanged
    );
    assert_eq!(fixture.artifacts.git_reservation_usage(), (0, 0));
    assert_eq!(fixture.artifacts.staged_artifact_count(), 0);
}

#[test]
fn shallow_baseline_admits_and_captures_without_prebaseline_connectivity() {
    let temporary = tempfile::tempdir().unwrap();
    let source = temporary.path().join("source");
    init_repository(&source);
    fs::write(source.join("old.txt"), b"old\n").unwrap();
    git(&source, &["add", "old.txt"]);
    git(&source, &["commit", "--quiet", "-m", "history"]);
    fs::write(source.join("base.txt"), b"base\n").unwrap();
    git(&source, &["add", "base.txt"]);
    git(&source, &["commit", "--quiet", "-m", "baseline"]);
    let repository = temporary.path().join("shallow");
    git_parent(&[
        "clone",
        "--quiet",
        "--depth=1",
        &format!("file://{}", source.display()),
        repository.to_str().unwrap(),
    ]);
    git(&repository, &["config", "user.name", "Scherzo Test"]);
    git(
        &repository,
        &["config", "user.email", "test@example.invalid"],
    );
    assert!(repository.join(".git/shallow").is_file());
    let fixture = GitFixture::from_prepared(temporary, repository);
    fixture.commit_file("new.txt", b"new\n", "new");

    let candidates = fixture.capture().unwrap();
    assert!(
        candidates.outputs()["changes"]
            .as_git_branch()
            .unwrap()
            .carrier()
            .is_some()
    );
}

struct PromisorFixture {
    workspace: GitFixture,
    source: PathBuf,
    promised_blob: String,
}

struct PreparedPromisor {
    temporary: tempfile::TempDir,
    source: PathBuf,
    repository: PathBuf,
    promised_blob: String,
}

fn prepare_promisor() -> PreparedPromisor {
    let temporary = tempfile::tempdir().unwrap();
    let source = temporary.path().join("promisor-source");
    init_repository(&source);
    git(&source, &["config", "uploadpack.allowFilter", "true"]);
    fs::write(source.join("base.txt"), b"base\n").unwrap();
    git(&source, &["add", "base.txt"]);
    git(&source, &["commit", "--quiet", "-m", "baseline"]);
    let baseline = git_oid(&source, "HEAD");
    fs::write(source.join("promised.txt"), vec![b'x'; 512 * 1024]).unwrap();
    git(&source, &["add", "promised.txt"]);
    git(&source, &["commit", "--quiet", "-m", "add promised blob"]);
    let promised_blob = git_oid(&source, "HEAD:promised.txt");
    fs::remove_file(source.join("promised.txt")).unwrap();
    git(
        &source,
        &["commit", "--quiet", "-am", "delete promised blob"],
    );
    let repository = temporary.path().join("partial");
    git_parent(&[
        "clone",
        "--quiet",
        "--filter=blob:none",
        "--no-checkout",
        &format!("file://{}", source.display()),
        repository.to_str().unwrap(),
    ]);
    git(&repository, &["checkout", "--quiet", &baseline]);
    git(&repository, &["config", "user.name", "Scherzo Test"]);
    git(
        &repository,
        &["config", "user.email", "test@example.invalid"],
    );
    PreparedPromisor {
        temporary,
        source,
        repository,
        promised_blob,
    }
}

fn finish_promisor<const N: usize>(
    prepared: PreparedPromisor,
    additional_environment: [(&str, &OsStr); N],
) -> PromisorFixture {
    git(
        &prepared.repository,
        &[
            "remote",
            "add",
            "unrelated",
            "file:///definitely-not-a-scherzo-source",
        ],
    );
    let workspace = GitFixture::from_prepared_with_environment(
        prepared.temporary,
        prepared.repository,
        additional_environment,
    );
    git(&workspace.repository, &["checkout", "--quiet", "-"]);
    let missing = fixture_git_command()
        .arg("-C")
        .arg(&workspace.repository)
        .args(["--no-lazy-fetch", "cat-file", "-e", &prepared.promised_blob])
        .status()
        .unwrap();
    assert!(!missing.success());
    PromisorFixture {
        workspace,
        source: prepared.source,
        promised_blob: prepared.promised_blob,
    }
}

fn promisor_fixture() -> PromisorFixture {
    finish_promisor(prepare_promisor(), [])
}

#[test]
fn promisor_capture_hydrates_only_required_objects_from_existing_source_authority() {
    let fixture = promisor_fixture();

    let candidates = fixture.workspace.capture().unwrap();
    assert!(
        candidates.outputs()["changes"]
            .as_git_branch()
            .unwrap()
            .carrier()
            .is_some()
    );
    let hydrated = fixture_git_command()
        .arg("-C")
        .arg(&fixture.workspace.repository)
        .args(["--no-lazy-fetch", "cat-file", "-e", &fixture.promised_blob])
        .status()
        .unwrap();
    assert!(hydrated.success());
}

#[test]
fn unavailable_promised_objects_fail_capture_instead_of_admission() {
    let fixture = promisor_fixture();
    let unavailable_source = fixture.source.with_extension("unavailable");
    fs::rename(&fixture.source, &unavailable_source).unwrap();

    assert_eq!(
        capture_failure(fixture.workspace.capture()),
        GitCaptureFailure::RequiredObjectsUnavailable
    );
    assert_eq!(fixture.workspace.artifacts.git_reservation_usage(), (0, 0));
    assert_eq!(fixture.workspace.artifacts.staged_artifact_count(), 0);
}

#[test]
fn capture_rejects_post_admission_redirects_to_an_unrelated_promisor_source() {
    let fixture = promisor_fixture();
    let unrelated = fixture.workspace._temporary.path().join("unrelated.git");
    git_parent(&[
        "clone",
        "--quiet",
        "--mirror",
        &format!("file://{}", fixture.source.display()),
        unrelated.to_str().unwrap(),
    ]);
    git(
        &fixture.workspace.repository,
        &[
            "remote",
            "set-url",
            "origin",
            &format!("file://{}", unrelated.display()),
        ],
    );
    let unavailable_source = fixture.source.with_extension("unavailable");
    fs::rename(&fixture.source, unavailable_source).unwrap();

    assert_eq!(
        capture_failure(fixture.workspace.capture()),
        GitCaptureFailure::SourceAuthorityChanged
    );
    assert_eq!(fixture.workspace.artifacts.staged_artifact_count(), 0);
}

#[test]
fn git_config_environment_cannot_hide_post_admission_source_changes() {
    let prepared = prepare_promisor();
    let unrelated = prepared.temporary.path().join("unrelated.git");
    git_parent(&[
        "clone",
        "--quiet",
        "--mirror",
        &format!("file://{}", prepared.source.display()),
        unrelated.to_str().unwrap(),
    ]);
    let config_override = prepared.temporary.path().join("config-override");
    fs::write(&config_override, b"").unwrap();
    let fixture = finish_promisor(prepared, [("GIT_CONFIG", config_override.as_os_str())]);
    git(
        &fixture.workspace.repository,
        &[
            "remote",
            "set-url",
            "origin",
            &format!("file://{}", unrelated.display()),
        ],
    );
    let unavailable_source = fixture.source.with_extension("unavailable");
    fs::rename(&fixture.source, unavailable_source).unwrap();

    assert_eq!(
        capture_failure(fixture.workspace.capture()),
        GitCaptureFailure::SourceAuthorityChanged
    );
    assert_eq!(fixture.workspace.artifacts.staged_artifact_count(), 0);
}

#[test]
fn global_git_config_cannot_redirect_promisor_hydration() {
    let prepared = prepare_promisor();
    let unrelated = prepared.temporary.path().join("unrelated.git");
    git_parent(&[
        "clone",
        "--quiet",
        "--mirror",
        &format!("file://{}", prepared.source.display()),
        unrelated.to_str().unwrap(),
    ]);
    let home = prepared.temporary.path().join("home");
    fs::create_dir(&home).unwrap();
    fs::write(
        home.join(".gitconfig"),
        format!(
            "[url \"file://{}\"]\n\tinsteadOf = file://{}\n",
            unrelated.display(),
            prepared.source.display()
        ),
    )
    .unwrap();
    let fixture = finish_promisor(prepared, [("HOME", home.as_os_str())]);
    let unavailable_source = fixture.source.with_extension("unavailable");
    fs::rename(&fixture.source, unavailable_source).unwrap();

    assert_eq!(
        capture_failure(fixture.workspace.capture()),
        GitCaptureFailure::RequiredObjectsUnavailable
    );
    assert_eq!(fixture.workspace.artifacts.staged_artifact_count(), 0);
}

#[test]
fn git_config_parameters_cannot_redirect_promisor_hydration() {
    let prepared = prepare_promisor();
    let unrelated = prepared.temporary.path().join("unrelated.git");
    git_parent(&[
        "clone",
        "--quiet",
        "--mirror",
        &format!("file://{}", prepared.source.display()),
        unrelated.to_str().unwrap(),
    ]);
    let injected_config = OsString::from(format!(
        "'url.file://{}.insteadOf=file://{}'",
        unrelated.display(),
        prepared.source.display()
    ));
    let fixture = finish_promisor(
        prepared,
        [("GIT_CONFIG_PARAMETERS", injected_config.as_os_str())],
    );
    let unavailable_source = fixture.source.with_extension("unavailable");
    fs::rename(&fixture.source, unavailable_source).unwrap();

    assert_eq!(
        capture_failure(fixture.workspace.capture()),
        GitCaptureFailure::RequiredObjectsUnavailable
    );
    assert_eq!(fixture.workspace.artifacts.staged_artifact_count(), 0);
}

#[derive(Clone, Copy)]
enum PostStagingMutation {
    Head,
    Cleanliness,
}

struct MutationObserver {
    repository: PathBuf,
    mutation: PostStagingMutation,
}

impl CaptureBoundaryObserver for MutationObserver {
    fn reached(&self, boundary: CaptureBoundary) {
        if boundary.kind != CaptureBoundaryKind::BeforeGitRecheck {
            return;
        }
        match self.mutation {
            PostStagingMutation::Head => {
                fs::write(self.repository.join("concurrent.txt"), b"concurrent\n").unwrap();
                git(&self.repository, &["add", "concurrent.txt"]);
                git(&self.repository, &["commit", "--quiet", "-m", "concurrent"]);
            }
            PostStagingMutation::Cleanliness => {
                fs::write(self.repository.join("concurrent.txt"), b"dirty\n").unwrap();
            }
        }
    }
}

#[test]
fn post_staging_head_and_cleanliness_changes_rollback_provisional_state() {
    for mutation in [PostStagingMutation::Head, PostStagingMutation::Cleanliness] {
        let fixture = GitFixture::new();
        fixture.commit_file("tracked.txt", b"candidate\n", "candidate");
        let cancellation = CaptureCancellation::with_observer(Arc::new(MutationObserver {
            repository: fixture.repository.clone(),
            mutation,
        }));

        assert_eq!(
            capture_failure(
                fixture
                    .capture
                    .capture("changes", &fixture.artifacts, &cancellation,)
            ),
            GitCaptureFailure::WorkspaceChanged
        );
        assert_eq!(fixture.artifacts.git_reservation_usage(), (0, 0));
        assert_eq!(fixture.artifacts.git_budget_usage(), (0, 0));
        assert_eq!(fixture.artifacts.staged_artifact_count(), 0);
    }
}

struct ArmHeadRaceObserver {
    marker: PathBuf,
}

impl CaptureBoundaryObserver for ArmHeadRaceObserver {
    fn reached(&self, boundary: CaptureBoundary) {
        if boundary.kind == CaptureBoundaryKind::BeforeGitRecheck {
            fs::write(&self.marker, b"armed\n").unwrap();
        }
    }
}

#[test]
fn post_staging_recheck_rejects_head_changed_after_tree_observation() {
    let temporary = tempfile::tempdir().unwrap();
    let repository = temporary.path().join("repository");
    init_repository(&repository);
    fs::write(repository.join("base.txt"), b"base\n").unwrap();
    git(&repository, &["add", "base.txt"]);
    git(&repository, &["commit", "--quiet", "-m", "baseline"]);
    let armed = temporary.path().join("armed");
    let mutated = temporary.path().join("mutated");
    let wrapper = temporary.path().join("git-with-head-race");
    fs::write(
        &wrapper,
        "#!/bin/sh\ncase \"$*\" in\n  *\"^{tree}\"*)\n    \"$REAL_GIT\" \"$@\"\n    status=$?\n    if [ -f \"$ARMED\" ] && [ ! -f \"$MUTATED\" ]; then\n      : > \"$MUTATED\"\n      printf 'concurrent\\n' > concurrent.txt\n      \"$REAL_GIT\" add concurrent.txt\n      \"$REAL_GIT\" commit --quiet -m concurrent\n    fi\n    exit \"$status\"\n    ;;\nesac\nexec \"$REAL_GIT\" \"$@\"\n",
    )
    .unwrap();
    fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o700)).unwrap();
    let (admitted, artifacts) = admitted_capture(
        temporary.path(),
        &repository,
        [
            ("REAL_GIT", git_executable().as_os_str()),
            ("ARMED", armed.as_os_str()),
            ("MUTATED", mutated.as_os_str()),
        ],
    );
    let capture = GitCaptureContext::admit_with_program(
        admitted.execution(),
        &CaptureCancellation::default(),
        wrapper,
        Duration::from_secs(30),
    )
    .unwrap();
    fs::write(repository.join("candidate.txt"), b"candidate\n").unwrap();
    git(&repository, &["add", "candidate.txt"]);
    git(&repository, &["commit", "--quiet", "-m", "candidate"]);
    let cancellation =
        CaptureCancellation::with_observer(Arc::new(ArmHeadRaceObserver { marker: armed }));

    assert_eq!(
        capture_failure(capture.capture("changes", &artifacts, &cancellation)),
        GitCaptureFailure::WorkspaceChanged
    );
    assert!(mutated.is_file());
    assert_eq!(artifacts.git_reservation_usage(), (0, 0));
    assert_eq!(artifacts.staged_artifact_count(), 0);
}

#[test]
fn process_timeout_terminates_and_reaps_a_running_child() {
    let temporary = tempfile::tempdir().unwrap();
    let blocker = temporary.path().join("blocker");
    create_fifo(&blocker);
    let mut command = Command::new("cat");
    command
        .arg(blocker)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut child = ManagedProcessGroup::spawn(&mut command).unwrap();
    let pid = i32::try_from(child.child_mut().id())
        .ok()
        .and_then(Pid::from_raw)
        .unwrap();
    assert_eq!(rustix::process::getpgid(Some(pid)).unwrap(), pid);
    assert!(rustix::process::test_kill_process(pid).is_ok());

    let command_description = Arc::<str>::from("cat blocker");
    let failure = wait_managed_child(
        &mut child,
        &CaptureCancellation::default(),
        Duration::ZERO,
        Arc::clone(&command_description),
        || false,
        || false,
    )
    .unwrap_err();

    assert_eq!(
        failure,
        ProcessFailure::TimedOut {
            command: command_description,
            limit: Duration::ZERO,
        }
    );
    assert_eq!(
        admission_process_failure(failure),
        GitWorkspaceAdmissionFailure::GitTimedOut
    );
    assert!(child.try_wait().unwrap().is_some());
    assert!(rustix::process::test_kill_process(pid).is_err());
}

#[expect(
    clippy::disallowed_methods,
    reason = "the timeout only bounds failure if process-group termination leaves a pipe open"
)]
#[test]
fn reaping_a_completed_leader_first_terminates_its_pipe_holding_group() {
    let temporary = tempfile::tempdir().unwrap();
    let blocker = temporary.path().join("blocker");
    create_fifo(&blocker);
    let wrapper = temporary.path().join("leader-with-lingering-helper");
    fs::write(
        &wrapper,
        format!("#!/bin/sh\ncat '{}' &\n", blocker.display()),
    )
    .unwrap();
    fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o700)).unwrap();
    let mut command = Command::new(wrapper);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let mut child = ManagedProcessGroup::spawn(&mut command).unwrap();
    let stdout = child.child_mut().stdout.take().unwrap();
    let (finished, completion) = std::sync::mpsc::sync_channel(0);
    let reader = std::thread::spawn(move || {
        let mut stdout = stdout;
        let mut bytes = Vec::new();
        let result = std::io::Read::read_to_end(&mut stdout, &mut bytes);
        let _ = finished.send(result);
    });

    let leader_status = child.wait().unwrap();
    assert!(leader_status.success());
    completion
        .recv_timeout(Duration::from_secs(2))
        .expect("terminated process group should close inherited output")
        .unwrap();
    reader.join().unwrap();
}

#[test]
fn capture_timeout_names_git_command_and_limit() {
    let temporary = tempfile::tempdir().unwrap();
    let repository = temporary.path().join("repository");
    init_repository(&repository);
    fs::write(repository.join("base.txt"), b"base\n").unwrap();
    git(&repository, &["add", "base.txt"]);
    git(&repository, &["commit", "--quiet", "-m", "baseline"]);
    let blocker = temporary.path().join("pack-blocker");
    create_fifo(&blocker);
    let wrapper = temporary.path().join("git-with-timeout");
    fs::write(
        &wrapper,
        "#!/bin/sh\ncase \" $* \" in\n  *\" pack-objects --stdout --revs --no-sparse --window=0 --depth=0 \"*) IFS= read -r unexpected < \"$BLOCKER\"; exit 75 ;;
esac\nexec \"$REAL_GIT\" \"$@\"\n",
    )
    .unwrap();
    fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o700)).unwrap();
    let (admitted, artifacts) = admitted_capture(
        temporary.path(),
        &repository,
        [
            ("REAL_GIT", git_executable().as_os_str()),
            ("BLOCKER", blocker.as_os_str()),
        ],
    );
    let capture = GitCaptureContext::admit_with_program(
        admitted.execution(),
        &CaptureCancellation::default(),
        wrapper,
        Duration::from_secs(1),
    )
    .unwrap();
    fs::write(repository.join("change.txt"), b"change\n").unwrap();
    git(&repository, &["add", "change.txt"]);
    git(&repository, &["commit", "--quiet", "-m", "change"]);

    let failure =
        capture_failure(capture.capture("changes", &artifacts, &CaptureCancellation::default()));

    let GitCaptureFailure::CommandTimedOut(timeout) = failure else {
        panic!("capture timeout changed failure variant");
    };
    assert_eq!(
        timeout.command.as_ref(),
        "git pack-objects --stdout --revs --no-sparse --window=0 --depth=0"
    );
    assert_eq!(timeout.limit, Duration::from_secs(1));
    assert_eq!(artifacts.git_reservation_usage(), (0, 0));
}

#[test]
fn capture_cancellation_terminates_and_reaps_bundle_process_and_rolls_back() {
    let temporary = tempfile::tempdir().unwrap();
    let repository = temporary.path().join("repository");
    init_repository(&repository);
    fs::write(repository.join("base.txt"), b"base\n").unwrap();
    git(&repository, &["add", "base.txt"]);
    git(&repository, &["commit", "--quiet", "-m", "baseline"]);
    let marker = temporary.path().join("pack.pid");
    create_fifo(&marker);
    let blocker = temporary.path().join("pack-blocker");
    create_fifo(&blocker);
    let wrapper = temporary.path().join("git-wrapper");
    fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\ncase \" $* \" in\n  *\" pack-objects \"*) printf '%s\\n' \"$$\" > '{}'; IFS= read -r unexpected < \"$BLOCKER\"; exit 75 ;;\nesac\nexec \"$REAL_GIT\" \"$@\"\n",
            marker.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o700)).unwrap();
    let (admitted, artifacts) = admitted_capture(
        temporary.path(),
        &repository,
        [
            ("REAL_GIT", git_executable().as_os_str()),
            ("BLOCKER", blocker.as_os_str()),
        ],
    );
    let capture = GitCaptureContext::admit_with_program(
        admitted.execution(),
        &CaptureCancellation::default(),
        wrapper,
        Duration::from_secs(30),
    )
    .unwrap();
    fs::write(repository.join("change.txt"), b"change\n").unwrap();
    git(&repository, &["add", "change.txt"]);
    git(&repository, &["commit", "--quiet", "-m", "change"]);
    let cancellation = CaptureCancellation::default();
    let worker_cancellation = cancellation.clone();
    let capture =
        std::thread::spawn(move || capture.capture("changes", &artifacts, &worker_cancellation));
    let pid = fs::read_to_string(&marker).unwrap().trim().to_owned();
    assert!(!pid.is_empty());

    cancellation.cancel();
    assert_eq!(
        capture_failure(capture.join().unwrap()),
        GitCaptureFailure::Cancelled
    );
    #[cfg(target_os = "linux")]
    assert!(!Path::new("/proc").join(pid).exists());
}

fn repository_bytes(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &Path, current: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
        let mut entries = fs::read_dir(current)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let path = entry.path();
            let relative = path.strip_prefix(root).unwrap().to_owned();
            let file_type = entry.file_type().unwrap();
            if file_type.is_dir() {
                visit(root, &path, files);
            } else if file_type.is_symlink() {
                files.insert(
                    relative,
                    fs::read_link(path)
                        .unwrap()
                        .as_os_str()
                        .as_encoded_bytes()
                        .to_vec(),
                );
            } else {
                files.insert(relative, fs::read(path).unwrap());
            }
        }
    }

    let mut files = BTreeMap::new();
    visit(root, root, &mut files);
    files
}
