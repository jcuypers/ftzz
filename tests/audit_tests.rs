use std::{fs, process::Command};
use tempfile::TempDir;

#[test]
fn test_audit_trail_csv() {
    let temp = TempDir::new().unwrap();
    let root_dir = temp.path().join("output");
    let audit_file = temp.path().join("audit.csv");

    let output = Command::new(env!("CARGO_BIN_EXE_ftzz"))
        .arg(&root_dir)
        .arg("-n")
        .arg("10")
        .arg("--audit-output")
        .arg(&audit_file)
        .output()
        .unwrap();

    assert!(output.status.success());
    
    // Verify audit file was created
    assert!(audit_file.exists());
    
    // Read and verify CSV content
    let content = fs::read_to_string(&audit_file).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    
    // Should have header + entries
    assert!(lines.len() > 1);
    
    // Verify header
    assert_eq!(lines[0], "path,type,size,hash,permissions,owner");
    
    // Verify at least some entries exist
    assert!(lines.iter().any(|line| line.contains(",file,")));
    assert!(lines.iter().any(|line| line.contains(",directory,")));
}

#[test]
fn test_audit_trail_with_bytes() {
    let temp = TempDir::new().unwrap();
    let root_dir = temp.path().join("output");
    let audit_file = temp.path().join("audit.csv");

    let output = Command::new(env!("CARGO_BIN_EXE_ftzz"))
        .arg(&root_dir)
        .arg("-n")
        .arg("5")
        .arg("-b")
        .arg("100")
        .arg("--audit-output")
        .arg(&audit_file)
        .output()
        .unwrap();

    assert!(output.status.success());
    
    let content = fs::read_to_string(&audit_file).unwrap();
    
    // Files with content should have hashes
    let has_hash = content.lines().any(|line| {
        let parts: Vec<&str> = line.split(',').collect();
        parts.len() >= 4 && parts[1] == "file" && !parts[3].is_empty()
    });
    
    assert!(has_hash, "Files with content should have hashes");
}
