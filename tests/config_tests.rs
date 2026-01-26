use std::{fs, process::Command};
use tempfile::TempDir;

#[test]
fn test_config_loading() {
    let temp = TempDir::new().unwrap();
    let config_path = temp.path().join("config.toml");
    let root_dir = temp.path().join("output");

    fs::write(
        &config_path,
        r#"
files = 100
max-depth = 2
seed = 42
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
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("About 100 files"));
    assert!(stdout.contains("maximum depth 2"));
}

#[test]
fn test_cli_overrides_config() {
    let temp = TempDir::new().unwrap();
    let config_path = temp.path().join("config.toml");
    let root_dir = temp.path().join("output");

    fs::write(
        &config_path,
        r#"
files = 100
max-depth = 2
"#,
    )
    .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_ftzz"))
        .arg("--config")
        .arg(&config_path)
        .arg("-n")
        .arg("50")
        .arg(&root_dir)
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("About 50 files"));
    assert!(stdout.contains("maximum depth 2"));
}

#[test]
fn test_missing_config_fails() {
    let temp = TempDir::new().unwrap();
    let root_dir = temp.path().join("output");

    let output = Command::new(env!("CARGO_BIN_EXE_ftzz"))
        .arg("--config")
        .arg("non_existent.toml")
        .arg(&root_dir)
        .output()
        .unwrap();

    assert!(!output.status.success());
}
