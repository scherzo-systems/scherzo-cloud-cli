#![allow(
    clippy::disallowed_macros,
    clippy::unwrap_used,
    reason = "filesystem regression assertions use panic shortcuts with fixture context"
)]

mod candidate {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/execution/owned_tree.rs"
    ));

    #[test]
    fn replacement_is_rejected_before_permission_restoration() {
        use std::fs::{self, Permissions};
        use std::os::unix::fs::PermissionsExt as _;
        use std::path::Path;

        fn mode(path: &Path) -> u32 {
            fs::symlink_metadata(path).unwrap().permissions().mode() & 0o7777
        }

        let temporary = tempfile::tempdir().unwrap();
        let owned = temporary.path().join("owned");
        let nested = owned.join("nested");
        let outside = temporary.path().join("outside");
        let replacement = outside.join("replacement");
        let displaced = outside.join("displaced");
        fs::create_dir(&owned).unwrap();
        fs::create_dir(&nested).unwrap();
        fs::create_dir(&outside).unwrap();
        fs::create_dir(&replacement).unwrap();
        fs::write(nested.join("original"), b"original").unwrap();
        let sentinel = replacement.join("sentinel");
        fs::write(&sentinel, b"outside authority").unwrap();
        fs::set_permissions(&sentinel, Permissions::from_mode(0o400)).unwrap();
        fs::set_permissions(&nested, Permissions::from_mode(0o500)).unwrap();
        fs::set_permissions(&replacement, Permissions::from_mode(0o500)).unwrap();

        let parent =
            rustix::fs::open(temporary.path(), directory_open_flags(), Mode::empty()).unwrap();
        let directory = open_directory_at(&parent, "owned").unwrap();

        let result = remove_open_tree_named(&parent, c"owned", &directory, &mut |_, identity| {
            if identity.to_bytes() == b"nested" {
                fs::set_permissions(&nested, Permissions::from_mode(0o700)).unwrap();
                fs::rename(&nested, &displaced).unwrap();
                fs::set_permissions(&displaced, Permissions::from_mode(0o500)).unwrap();
                fs::set_permissions(&replacement, Permissions::from_mode(0o700)).unwrap();
                fs::rename(&replacement, &nested).unwrap();
                fs::set_permissions(&nested, Permissions::from_mode(0o500)).unwrap();
            }
        });

        assert_eq!(result, Err(RemovalError::Replaced));
        let replacement_mode = mode(&nested);
        let displaced_mode = mode(&displaced);
        let sentinel_mode = mode(&nested.join("sentinel"));
        let sentinel_contents = fs::read(nested.join("sentinel")).unwrap();
        fs::set_permissions(&nested, Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&displaced, Permissions::from_mode(0o700)).unwrap();

        assert_eq!(
            replacement_mode, 0o500,
            "cleanup changed permissions on the unvalidated replacement"
        );
        assert_eq!(displaced_mode, 0o500);
        assert_eq!(sentinel_mode, 0o400);
        assert_eq!(sentinel_contents, b"outside authority");
    }
}
