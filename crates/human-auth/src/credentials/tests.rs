use std::os::unix::fs::symlink;

use anyhow::Context as _;

use tempfile::TempDir;

use super::*;

struct Fixture {
    directory: TempDir,
    store: CredentialStore,
}

impl Fixture {
    fn new() -> anyhow::Result<Self> {
        let directory = tempfile::tempdir().context("temporary directory should be created")?;
        fs::set_permissions(directory.path(), Permissions::from_mode(DIRECTORY_MODE))
            .context("temporary directory mode should be private")?;
        let path = directory.path().join(NORMAL_FILE_NAME);
        let store = CredentialStore {
            lock_path: sibling_path(
                &path,
                path.file_name().context("fixture value should exist")?,
                ".lock",
            ),
            path,
            lock_timeout: Duration::from_millis(75),
            refresh_lock_timeout: Duration::from_millis(75),
        };
        Ok(Self { directory, store })
    }

    fn write_raw(&self, bytes: &[u8]) -> anyhow::Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(FILE_MODE)
            .open(&self.store.path)
            .context("credential fixture should open")?;
        file.set_permissions(Permissions::from_mode(FILE_MODE))
            .context("credential fixture mode should be private")?;
        file.write_all(bytes)
            .context("credential fixture should be written")?;
        Ok(())
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

fn timestamp(value: &str) -> anyhow::Result<OffsetDateTime> {
    OffsetDateTime::parse(value, &Rfc3339).context("timestamp fixture should parse")
}

#[test]
fn environment_selects_override_or_normal_home_path() -> anyhow::Result<()> {
    let override_path = OsString::from("/private/test/credentials.json");
    let override_store = CredentialStore::from_lookup(|name| match name {
        CREDENTIALS_FILE_VARIABLE => Some(override_path.clone()),
        _ => None,
    })
    .context("override should resolve")?;
    let home_store = CredentialStore::from_lookup(|name| match name {
        HOME_VARIABLE => Some(OsString::from("/private/home")),
        _ => None,
    })
    .context("home should resolve")?;

    check_eq!(override_store.path, PathBuf::from(&override_path));
    check_eq!(
        home_store.path,
        PathBuf::from("/private/home/.scherzo/credentials.json")
    );
    Ok(())
}

#[test]
fn normal_home_store_writes_and_reads_private_credentials() -> anyhow::Result<()> {
    let home = tempfile::tempdir().context("temporary home should be created")?;
    fs::set_permissions(home.path(), Permissions::from_mode(0o700))
        .context("fixture value should exist")?;
    let store = CredentialStore::from_lookup(|name| match name {
        HOME_VARIABLE => Some(home.path().as_os_str().to_owned()),
        _ => None,
    })
    .context("home credential path should resolve")?;
    let deployment = fingerprint("primary");
    let expiration = timestamp("2026-07-22T12:00:00Z")?;

    store
        .replace(
            &deployment,
            "synthetic-access-token",
            expiration,
            "synthetic-refresh-token",
        )
        .context("credential should be stored")?;
    let selected = store
        .selected(&deployment)
        .context("credential should load")?
        .context("credential should match")?;

    let application_home = home.path().join(APPLICATION_HOME_DIRECTORY_NAME);
    check_eq!(store.path, application_home.join(NORMAL_FILE_NAME));
    check_eq!(selected.access_token(), "synthetic-access-token");
    check_eq!(
        fs::metadata(&application_home)
            .context("fixture value should exist")?
            .mode()
            & 0o7777,
        DIRECTORY_MODE
    );
    check_eq!(
        fs::metadata(&store.path)
            .context("fixture value should exist")?
            .mode()
            & 0o7777,
        FILE_MODE
    );
    Ok(())
}

#[test]
fn missing_file_is_empty_and_creates_only_private_lock_material() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    let deployment = fingerprint("primary");

    check!(
        !fixture
            .store
            .remove(&deployment)
            .context("logout should succeed")?
    );
    check!(!fixture.store.path.exists());
    check_eq!(
        fs::metadata(fixture.directory.path())
            .context("fixture value should exist")?
            .mode()
            & 0o7777,
        DIRECTORY_MODE
    );
    check_eq!(
        fs::metadata(&fixture.store.lock_path)
            .context("fixture value should exist")?
            .mode()
            & 0o7777,
        FILE_MODE
    );
    Ok(())
}

#[test]
fn replacement_writes_schema_one_and_selects_exact_deployment() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    let primary = fingerprint("primary");
    let other = fingerprint("other");
    let expiration = timestamp("2026-07-22T12:00:00Z")?;

    fixture
        .store
        .replace(
            &primary,
            "synthetic-access-token",
            expiration,
            "synthetic-refresh-token",
        )
        .context("credential should be stored")?;
    let selected = fixture
        .store
        .selected(&primary)
        .context("credential should load")?
        .context("credential should match")?;

    check_eq!(selected.access_token(), "synthetic-access-token");
    check_eq!(selected.expires_at(), expiration);
    check!(
        fixture
            .store
            .selected(&other)
            .context("other deployment lookup should succeed")?
            .is_none()
    );
    check_eq!(
        fs::metadata(&fixture.store.path)
            .context("fixture value should exist")?
            .mode()
            & 0o7777,
        FILE_MODE
    );
    let value: serde_json::Value = serde_json::from_slice(
        &fs::read(&fixture.store.path).context("fixture value should exist")?,
    )
    .context("fixture value should exist")?;
    check_eq!(value["schemaVersion"], 1);
    check_eq!(
        value["credentials"]
            .as_array()
            .context("fixture value should exist")?
            .len(),
        1
    );
    check_eq!(
        value["credentials"][0]["refreshToken"],
        "synthetic-refresh-token"
    );
    check!(
        fs::read_dir(fixture.directory.path())
            .context("fixture value should exist")?
            .collect::<std::io::Result<Vec<_>>>()?
            .iter()
            .all(|entry| !entry.file_name().to_string_lossy().contains(".tmp."))
    );
    Ok(())
}

#[test]
fn replacing_one_deployment_never_creates_a_duplicate() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    let deployment = fingerprint("primary");
    let expiration = timestamp("2026-07-22T12:00:00Z")?;

    fixture
        .store
        .replace(&deployment, "first", expiration, "first-refresh")
        .context("fixture value should exist")?;
    fixture
        .store
        .replace(&deployment, "second", expiration, "second-refresh")
        .context("fixture value should exist")?;

    let value: serde_json::Value = serde_json::from_slice(
        &fs::read(&fixture.store.path).context("fixture value should exist")?,
    )
    .context("fixture value should exist")?;
    let credentials = value["credentials"]
        .as_array()
        .context("fixture value should exist")?;
    check_eq!(credentials.len(), 1);
    check_eq!(credentials[0]["accessToken"], "second");
    check_eq!(credentials[0]["refreshToken"], "second-refresh");
    Ok(())
}

#[test]
fn conditional_removal_does_not_delete_a_concurrently_replaced_token() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    let deployment = fingerprint("primary");
    let expiration = timestamp("2026-07-22T12:00:00Z")?;
    fixture
        .store
        .replace(
            &deployment,
            "replacement-token",
            expiration,
            "replacement-refresh-token",
        )
        .context("fixture value should exist")?;

    let removed = fixture
        .store
        .remove_if_access_token_matches_under_authority(&deployment, "rejected-old-token")
        .context("fixture value should exist")?;

    check!(!removed);
    let selected = fixture
        .store
        .selected(&deployment)
        .context("fixture value should exist")?
        .context("replacement credential should remain")?;
    check_eq!(selected.access_token(), "replacement-token");
    Ok(())
}

#[test]
fn rotated_refresh_replacement_is_atomic_and_rejects_a_stale_overwrite() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    let deployment = fingerprint("primary");
    let expiration = timestamp("2026-07-22T12:00:00Z")?;
    fixture
        .store
        .replace(&deployment, "first-access", expiration, "first-refresh")
        .context("fixture value should exist")?;

    let rotated = fixture
        .store
        .replace_if_refresh_token_matches_until(
            &deployment,
            "first-refresh",
            "second-access",
            expiration,
            "second-refresh",
            None,
        )
        .context("fixture value should exist")?
        .context("fixture value should exist")?;
    check_eq!(rotated.access_token(), "second-access");
    check_eq!(rotated.refresh_token(), "second-refresh");

    let retained = fixture
        .store
        .replace_if_refresh_token_matches_until(
            &deployment,
            "first-refresh",
            "stale-access",
            expiration,
            "stale-refresh",
            None,
        )
        .context("fixture value should exist")?
        .context("fixture value should exist")?;
    check_eq!(retained.access_token(), "second-access");
    check_eq!(retained.refresh_token(), "second-refresh");
    let bytes = fs::read(&fixture.store.path).context("fixture value should exist")?;
    for stale in ["stale-access", "stale-refresh"] {
        check!(
            !bytes
                .windows(stale.len())
                .any(|part| part == stale.as_bytes())
        );
    }
    Ok(())
}

#[test]
fn refresh_lock_paths_are_stable_and_fingerprint_scoped() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    let primary = fixture
        .store
        .refresh_lock_path(&fingerprint("primary"))
        .context("fixture value should exist")?;
    let same = fixture
        .store
        .refresh_lock_path(&fingerprint("primary"))
        .context("fixture value should exist")?;
    let other = fixture
        .store
        .refresh_lock_path(&fingerprint("other"))
        .context("fixture value should exist")?;

    check_eq!(primary, same);
    check_ne!(primary, other);
    check_eq!(primary.parent(), Some(fixture.directory.path()));
    Ok(())
}

#[test]
fn token_size_boundary_is_enforced_without_modifying_existing_bytes() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    let deployment = fingerprint("primary");
    let expiration = timestamp("2026-07-22T12:00:00Z")?;
    let maximum = "x".repeat(MAX_ACCESS_TOKEN_BYTES);
    fixture
        .store
        .replace(&deployment, &maximum, expiration, "maximum-refresh")
        .context("a 64 KiB token should be accepted")?;
    let original = fs::read(&fixture.store.path).context("fixture value should exist")?;
    let oversized = "x".repeat(MAX_ACCESS_TOKEN_BYTES + 1);

    check!(
        fixture
            .store
            .replace(&deployment, &oversized, expiration, "oversized-refresh")
            .is_err()
    );
    check_eq!(
        fs::read(&fixture.store.path).context("fixture value should exist")?,
        original
    );
    Ok(())
}

#[test]
fn safety_margin_marks_only_the_expiring_selected_credential_for_refresh() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    let expiring = fingerprint("expiring");
    let retained = fingerprint("retained");
    fixture
        .store
        .replace(
            &expiring,
            "expiring-token",
            timestamp("2026-07-22T11:00:30Z")?,
            "expiring-refresh-token",
        )
        .context("fixture value should exist")?;
    fixture
        .store
        .replace(
            &retained,
            "retained-token",
            timestamp("2026-07-22T12:00:00Z")?,
            "retained-refresh-token",
        )
        .context("fixture value should exist")?;

    let now = timestamp("2026-07-22T11:00:00Z")?;
    check!(
        fixture
            .store
            .selected(&expiring)
            .context("fixture value should exist")?
            .context("fixture value should exist")?
            .needs_refresh(now)
    );
    check!(
        !fixture
            .store
            .selected(&retained)
            .context("fixture value should exist")?
            .context("fixture value should exist")?
            .needs_refresh(now)
    );
    let bytes = fs::read(&fixture.store.path).context("fixture value should exist")?;
    for token in ["expiring-token", "retained-token"] {
        check!(
            bytes
                .windows(token.len())
                .any(|part| part == token.as_bytes())
        );
    }
    Ok(())
}

#[test]
fn malformed_and_unknown_schema_files_are_preserved() -> anyhow::Result<()> {
    for bytes in [
        b"not json\n".as_slice(),
        br#"{"schemaVersion":2,"credentials":[]}"#,
        br#"{"schemaVersion":1,"credentials":[],"futureField":true}"#,
    ] {
        let fixture = Fixture::new()?;
        fixture.write_raw(bytes)?;

        check!(fixture.store.remove(&fingerprint("primary")).is_err());
        check_eq!(
            fs::read(&fixture.store.path).context("fixture value should exist")?,
            bytes
        );
    }
    Ok(())
}

#[test]
fn unsafe_file_and_directory_modes_are_rejected_without_modification() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    fixture.write_raw(br#"{"schemaVersion":1,"credentials":[]}"#)?;
    fs::set_permissions(&fixture.store.path, Permissions::from_mode(0o644))
        .context("fixture value should exist")?;
    let original = fs::read(&fixture.store.path).context("fixture value should exist")?;

    check!(fixture.store.remove(&fingerprint("primary")).is_err());
    check_eq!(
        fs::read(&fixture.store.path).context("fixture value should exist")?,
        original
    );

    fs::set_permissions(&fixture.store.path, Permissions::from_mode(FILE_MODE))
        .context("fixture value should exist")?;
    fs::set_permissions(fixture.directory.path(), Permissions::from_mode(0o755))
        .context("fixture value should exist")?;
    check!(fixture.store.remove(&fingerprint("primary")).is_err());
    Ok(())
}

#[test]
fn credential_symlink_is_rejected_without_touching_its_target() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    let target = fixture.directory.path().join("target.json");
    fs::write(&target, b"target bytes").context("fixture value should exist")?;
    symlink(&target, &fixture.store.path).context("fixture value should exist")?;

    check!(fixture.store.remove(&fingerprint("primary")).is_err());
    check_eq!(
        fs::read(&target).context("fixture value should exist")?,
        b"target bytes"
    );
    Ok(())
}

#[test]
fn credential_lock_symlink_is_rejected_without_touching_its_target() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    let target = fixture.directory.path().join("lock-target");
    fs::write(&target, b"lock target bytes").context("fixture value should exist")?;
    symlink(&target, &fixture.store.lock_path).context("fixture value should exist")?;

    check!(fixture.store.remove(&fingerprint("primary")).is_err());
    check_eq!(
        fs::read(&target).context("fixture value should exist")?,
        b"lock target bytes"
    );
    Ok(())
}

#[test]
fn busy_lock_respects_the_configured_deadline() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    ensure_private_directory(fixture.directory.path()).context("fixture value should exist")?;
    let lock = open_or_create_private_file(&fixture.store.lock_path)
        .context("fixture value should exist")?;
    FileExt::lock(&lock).context("fixture value should exist")?;
    let started = scherzo_cloud_support::monotonic_now();

    let result = fixture.store.remove(&fingerprint("primary"));

    check!(matches!(result, Err(CredentialError::LockTimeout)));
    check!(scherzo_cloud_support::elapsed(started) >= fixture.store.lock_timeout);
    FileExt::unlock(&lock).context("fixture value should exist")?;
    Ok(())
}

#[test]
fn debug_output_never_contains_tokens() -> anyhow::Result<()> {
    let credential = StoredCredential {
        access_token: SecretToken::new("unique-synthetic-access-secret".to_owned()),
        expires_at: timestamp("2026-07-22T12:00:00Z")?,
        refresh_token: SecretToken::new("unique-synthetic-refresh-secret".to_owned()),
    };
    let debug = format!("{credential:?}");

    check!(!debug.contains("unique-synthetic-access-secret"));
    check!(!debug.contains("unique-synthetic-refresh-secret"));
    check!(debug.contains("[REDACTED]"));
    Ok(())
}
