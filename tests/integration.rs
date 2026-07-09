#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
use std::process::Command;

#[test]
fn test_help_command() {
    let output = Command::new("cargo")
        .args(["run", "--", "help"])
        .output()
        .expect("Failed to run help");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "Help command failed");
    assert!(stdout.contains("Usage:"), "Help output missing usage");
    assert!(stdout.contains("Commands:"), "Help output missing commands");
    assert!(
        stdout.contains("rm-snapshot"),
        "Help output missing rm-snapshot"
    );
}
