use std::process::Command;

#[test]
fn help_names_the_token_variable_without_its_value() {
    // Audit Auth-3: `--help` printed the value of `LEANI_API_TOKEN`.
    let output = Command::new(env!("CARGO_BIN_EXE_leani"))
        .args(["subscribe", "--help"])
        .env("LEANI_API_TOKEN", "help-must-not-print-this-token")
        .output()
        .expect("run leani subscribe --help");
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).expect("UTF-8 help");
    assert!(help.contains("LEANI_API_TOKEN"), "{help}");
    assert!(!help.contains("help-must-not-print-this-token"), "{help}");
}

#[test]
fn help_names_the_checkpoint_provider_variable_without_its_value() {
    // Review 1, minor 9: provider URLs can carry API keys.
    for command in ["init", "subscribe"] {
        let output = Command::new(env!("CARGO_BIN_EXE_leani"))
            .args([command, "--help"])
            .env(
                "LEANI_CHECKPOINT_URLS",
                "https://help-must-not-print-this-key@provider.example/",
            )
            .output()
            .expect("run leani --help");
        assert!(output.status.success(), "{command}");
        let help = String::from_utf8(output.stdout).expect("UTF-8 help");
        assert!(help.contains("LEANI_CHECKPOINT_URLS"), "{command}: {help}");
        assert!(
            !help.contains("help-must-not-print-this-key"),
            "{command}: {help}"
        );
    }
}
