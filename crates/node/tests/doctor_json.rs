use std::process::Command;

#[test]
fn doctor_json_reports_an_unreadable_configuration() {
    // Scripts parse `doctor --json`; a missing file printed no JSON and exited
    // 1 like any runtime failure, with the OS error repeated.
    let directory = tempfile::tempdir().expect("working directory");
    let output = Command::new(env!("CARGO_BIN_EXE_leani"))
        .args(["doctor", "--json", "--config", "missing.toml"])
        .current_dir(directory.path())
        .env_remove("LEANI_CONFIG")
        .output()
        .expect("run leani doctor");
    assert_eq!(output.status.code(), Some(3));
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).expect("JSON report");
    assert_eq!(report["valid"], false);
    let message = report["errors"][0]["message"]
        .as_str()
        .expect("error message");
    assert!(
        message.starts_with("failed to read configuration at missing.toml: "),
        "{message}"
    );
    assert_eq!(message.matches("os error").count(), 1, "{message}");
}
