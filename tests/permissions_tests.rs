use std::{fs, os::unix::fs::PermissionsExt, process::Command};

use tempfile::TempDir;

#[test]
fn test_deterministic_permissions() {
    let temp = TempDir::new().unwrap();
    let config_path = temp.path().join("config.toml");
    let root_dir = temp.path().join("output");

    fs::write(
        &config_path,
        r#"
files = 10
permissions = ["600", "755"]
seed = 1234
"#,
    )
    .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_ftzz"))
        .arg("--config")
        .arg(&config_path)
        .arg(&root_dir)
        .output()
        .unwrap();

    assert!(output.status.success());

    // Verify permissions of generated files
    // Files are named 0, 1, 2, ...
    for i in 0..10 {
        let file_path = root_dir.join(i.to_string());
        if file_path.exists() {
            let metadata = fs::metadata(&file_path).unwrap();
            let mode = metadata.permissions().mode() & 0o777;
            assert!(
                mode == 0o600 || mode == 0o755,
                "File {} has unexpected mode {:o}",
                i,
                mode
            );
        }
    }

    // Verify determinism: run again and check if it's the same
    let root_dir2 = temp.path().join("output2");
    let output2 = Command::new(env!("CARGO_BIN_EXE_ftzz"))
        .arg("--config")
        .arg(&config_path)
        .arg(&root_dir2)
        .output()
        .unwrap();

    assert!(output2.status.success());

    for i in 0..10 {
        let path1 = root_dir.join(i.to_string());
        let path2 = root_dir2.join(i.to_string());
        if path1.exists() && path2.exists() {
            let mode1 = fs::metadata(&path1).unwrap().permissions().mode() & 0o777;
            let mode2 = fs::metadata(&path2).unwrap().permissions().mode() & 0o777;
            assert_eq!(
                mode1, mode2,
                "File {} has inconsistent mode between runs",
                i
            );
        }
    }
}

#[test]
fn test_cli_permissions() {
    let temp = TempDir::new().unwrap();
    let root_dir = temp.path().join("output");

    let output = Command::new(env!("CARGO_BIN_EXE_ftzz"))
        .arg("-n")
        .arg("5")
        .arg("--permissions")
        .arg("644,700")
        .arg(&root_dir)
        .output()
        .unwrap();

    assert!(output.status.success());

    for i in 0..5 {
        let file_path = root_dir.join(i.to_string());
        if file_path.exists() {
            let metadata = fs::metadata(&file_path).unwrap();
            let mode = metadata.permissions().mode() & 0o777;
            assert!(
                mode == 0o644 || mode == 0o700,
                "File {} has unexpected mode {:o}",
                i,
                mode
            );
        }
    }
}
