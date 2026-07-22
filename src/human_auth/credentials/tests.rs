use std::os::unix::fs::symlink;

use tempfile::TempDir;

use super::*;

struct Fixture {
    directory: TempDir,
    store: CredentialStore,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().expect("temporary directory should be created");
        fs::set_permissions(directory.path(), Permissions::from_mode(DIRECTORY_MODE))
            .expect("temporary directory mode should be private");
        let path = directory.path().join(NORMAL_FILE_NAME);
        let store = CredentialStore {
            lock_path: sibling_path(&path, path.file_name().unwrap(), ".lock"),
            path,
            lock_timeout: Duration::from_millis(75),
        };
        Self { directory, store }
    }

    fn write_raw(&self, bytes: &[u8]) {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(FILE_MODE)
            .open(&self.store.path)
            .expect("credential fixture should open");
        file.set_permissions(Permissions::from_mode(FILE_MODE))
            .expect("credential fixture mode should be private");
        file.write_all(bytes)
            .expect("credential fixture should be written");
    }
}

fn fingerprint(name: &str) -> DeploymentFingerprint {
    DeploymentFingerprint::new(
        format!("https://{name}.api.example"),
        format!("https://{name}.auth.example/"),
        format!("https://{name}.audience.example"),
        format!("{name}-client"),
    )
}

fn timestamp(value: &str) -> OffsetDateTime {
    OffsetDateTime::parse(value, &Rfc3339).expect("timestamp fixture should parse")
}

#[test]
fn environment_selects_override_or_normal_home_path() {
    let override_path = OsString::from("/private/test/credentials.json");
    let override_store = CredentialStore::from_lookup(|name| match name {
        CREDENTIALS_FILE_VARIABLE => Some(override_path.clone()),
        _ => None,
    })
    .expect("override should resolve");
    let home_store = CredentialStore::from_lookup(|name| match name {
        HOME_VARIABLE => Some(OsString::from("/private/home")),
        _ => None,
    })
    .expect("home should resolve");

    assert_eq!(override_store.path, PathBuf::from(override_path));
    assert_eq!(
        home_store.path,
        PathBuf::from("/private/home/.scherzo-cloud/credentials.json")
    );
}

#[test]
fn normal_home_store_creates_the_private_credential_directory() {
    let home = tempfile::tempdir().expect("temporary home should be created");
    fs::set_permissions(home.path(), Permissions::from_mode(0o700)).unwrap();
    let store = CredentialStore::from_lookup(|name| match name {
        HOME_VARIABLE => Some(home.path().as_os_str().to_owned()),
        _ => None,
    })
    .expect("home credential path should resolve");

    assert!(!store.remove(&fingerprint("primary")).unwrap());

    let directory = home.path().join(NORMAL_DIRECTORY_NAME);
    assert!(directory.is_dir());
    assert_eq!(
        fs::metadata(directory).unwrap().mode() & 0o7777,
        DIRECTORY_MODE
    );
}

#[test]
fn missing_file_is_empty_and_creates_only_private_lock_material() {
    let fixture = Fixture::new();
    let deployment = fingerprint("primary");

    assert!(
        !fixture
            .store
            .remove(&deployment)
            .expect("logout should succeed")
    );
    assert!(!fixture.store.path.exists());
    assert_eq!(
        fs::metadata(fixture.directory.path()).unwrap().mode() & 0o7777,
        DIRECTORY_MODE
    );
    assert_eq!(
        fs::metadata(&fixture.store.lock_path).unwrap().mode() & 0o7777,
        FILE_MODE
    );
}

#[test]
fn replacement_writes_schema_one_and_selects_exact_deployment() {
    let fixture = Fixture::new();
    let primary = fingerprint("primary");
    let other = fingerprint("other");
    let expiration = timestamp("2026-07-22T12:00:00Z");

    fixture
        .store
        .replace(&primary, "synthetic-access-token", expiration)
        .expect("credential should be stored");
    let selected = fixture
        .store
        .selected(&primary, timestamp("2026-07-22T11:00:00Z"))
        .expect("credential should load")
        .expect("credential should match");

    assert_eq!(selected.access_token(), "synthetic-access-token");
    assert_eq!(selected.expires_at(), expiration);
    assert!(
        fixture
            .store
            .selected(&other, timestamp("2026-07-22T11:00:00Z"))
            .expect("other deployment lookup should succeed")
            .is_none()
    );
    assert_eq!(
        fs::metadata(&fixture.store.path).unwrap().mode() & 0o7777,
        FILE_MODE
    );
    let value: serde_json::Value =
        serde_json::from_slice(&fs::read(&fixture.store.path).unwrap()).unwrap();
    assert_eq!(value["schemaVersion"], 1);
    assert_eq!(value["credentials"].as_array().unwrap().len(), 1);
    assert!(
        fs::read_dir(fixture.directory.path())
            .unwrap()
            .all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains(".tmp."))
    );
}

#[test]
fn replacing_one_deployment_never_creates_a_duplicate() {
    let fixture = Fixture::new();
    let deployment = fingerprint("primary");
    let expiration = timestamp("2026-07-22T12:00:00Z");

    fixture
        .store
        .replace(&deployment, "first", expiration)
        .unwrap();
    fixture
        .store
        .replace(&deployment, "second", expiration)
        .unwrap();

    let value: serde_json::Value =
        serde_json::from_slice(&fs::read(&fixture.store.path).unwrap()).unwrap();
    let credentials = value["credentials"].as_array().unwrap();
    assert_eq!(credentials.len(), 1);
    assert_eq!(credentials[0]["accessToken"], "second");
}

#[test]
fn conditional_removal_does_not_delete_a_concurrently_replaced_token() {
    let fixture = Fixture::new();
    let deployment = fingerprint("primary");
    let expiration = timestamp("2026-07-22T12:00:00Z");
    fixture
        .store
        .replace(&deployment, "replacement-token", expiration)
        .unwrap();

    let removed = fixture
        .store
        .remove_if_access_token_matches(&deployment, "rejected-old-token")
        .unwrap();

    assert!(!removed);
    let selected = fixture
        .store
        .selected(&deployment, timestamp("2026-07-22T11:00:00Z"))
        .unwrap()
        .expect("replacement credential should remain");
    assert_eq!(selected.access_token(), "replacement-token");
}

#[test]
fn token_size_boundary_is_enforced_without_modifying_existing_bytes() {
    let fixture = Fixture::new();
    let deployment = fingerprint("primary");
    let expiration = timestamp("2026-07-22T12:00:00Z");
    let maximum = "x".repeat(MAX_ACCESS_TOKEN_BYTES);
    fixture
        .store
        .replace(&deployment, &maximum, expiration)
        .expect("a 64 KiB token should be accepted");
    let original = fs::read(&fixture.store.path).unwrap();
    let oversized = "x".repeat(MAX_ACCESS_TOKEN_BYTES + 1);

    assert!(
        fixture
            .store
            .replace(&deployment, &oversized, expiration)
            .is_err()
    );
    assert_eq!(fs::read(&fixture.store.path).unwrap(), original);
}

#[test]
fn safety_margin_removes_only_the_expiring_selected_credential() {
    let fixture = Fixture::new();
    let expiring = fingerprint("expiring");
    let retained = fingerprint("retained");
    fixture
        .store
        .replace(
            &expiring,
            "expiring-token",
            timestamp("2026-07-22T11:00:30Z"),
        )
        .unwrap();
    fixture
        .store
        .replace(
            &retained,
            "retained-token",
            timestamp("2026-07-22T12:00:00Z"),
        )
        .unwrap();

    assert!(
        fixture
            .store
            .selected(&expiring, timestamp("2026-07-22T11:00:00Z"))
            .unwrap()
            .is_none()
    );
    assert!(
        fixture
            .store
            .selected(&retained, timestamp("2026-07-22T11:00:00Z"))
            .unwrap()
            .is_some()
    );
    let bytes = fs::read(&fixture.store.path).unwrap();
    assert!(
        !bytes
            .windows("expiring-token".len())
            .any(|part| part == b"expiring-token")
    );
    assert!(
        bytes
            .windows("retained-token".len())
            .any(|part| part == b"retained-token")
    );
}

#[test]
fn malformed_and_unknown_schema_files_are_preserved() {
    for bytes in [
        b"not json\n".as_slice(),
        br#"{"schemaVersion":2,"credentials":[]}"#,
        br#"{"schemaVersion":1,"credentials":[],"futureField":true}"#,
    ] {
        let fixture = Fixture::new();
        fixture.write_raw(bytes);

        assert!(fixture.store.remove(&fingerprint("primary")).is_err());
        assert_eq!(fs::read(&fixture.store.path).unwrap(), bytes);
    }
}

#[test]
fn unsafe_file_and_directory_modes_are_rejected_without_modification() {
    let fixture = Fixture::new();
    fixture.write_raw(br#"{"schemaVersion":1,"credentials":[]}"#);
    fs::set_permissions(&fixture.store.path, Permissions::from_mode(0o644)).unwrap();
    let original = fs::read(&fixture.store.path).unwrap();

    assert!(fixture.store.remove(&fingerprint("primary")).is_err());
    assert_eq!(fs::read(&fixture.store.path).unwrap(), original);

    fs::set_permissions(&fixture.store.path, Permissions::from_mode(FILE_MODE)).unwrap();
    fs::set_permissions(fixture.directory.path(), Permissions::from_mode(0o755)).unwrap();
    assert!(fixture.store.remove(&fingerprint("primary")).is_err());
}

#[test]
fn credential_symlink_is_rejected_without_touching_its_target() {
    let fixture = Fixture::new();
    let target = fixture.directory.path().join("target.json");
    fs::write(&target, b"target bytes").unwrap();
    symlink(&target, &fixture.store.path).unwrap();

    assert!(fixture.store.remove(&fingerprint("primary")).is_err());
    assert_eq!(fs::read(target).unwrap(), b"target bytes");
}

#[test]
fn credential_lock_symlink_is_rejected_without_touching_its_target() {
    let fixture = Fixture::new();
    let target = fixture.directory.path().join("lock-target");
    fs::write(&target, b"lock target bytes").unwrap();
    symlink(&target, &fixture.store.lock_path).unwrap();

    assert!(fixture.store.remove(&fingerprint("primary")).is_err());
    assert_eq!(fs::read(target).unwrap(), b"lock target bytes");
}

#[test]
fn busy_lock_respects_the_configured_deadline() {
    let fixture = Fixture::new();
    ensure_private_directory(fixture.directory.path()).unwrap();
    let lock = open_or_create_private_file(&fixture.store.lock_path).unwrap();
    lock.lock_exclusive().unwrap();
    let start = Instant::now();

    let result = fixture.store.remove(&fingerprint("primary"));

    assert!(matches!(result, Err(CredentialError::LockTimeout)));
    assert!(start.elapsed() >= fixture.store.lock_timeout);
    FileExt::unlock(&lock).unwrap();
}

#[test]
fn debug_output_never_contains_the_access_token() {
    let credential = StoredCredential {
        access_token: "unique-synthetic-secret".to_owned(),
        expires_at: timestamp("2026-07-22T12:00:00Z"),
    };
    let debug = format!("{credential:?}");

    assert!(!debug.contains("unique-synthetic-secret"));
    assert!(debug.contains("[REDACTED]"));
}
