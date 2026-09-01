//! Built-in configuration contract for Ethereum block summaries.

use anyhow::{Context, Result};
use leani_processor_api::UndoPolicyMode;

use crate::config::{ProcessorConfig, ProcessorHistoryMode, PublishMode};

pub(crate) fn processor_config(instance: &str, finalized_only: bool) -> Result<ProcessorConfig> {
    let mut processor: ProcessorConfig = toml::from_str(
        r#"
id = "block-summary"
instance = "block-summary"
version = "1.0.0"
history_control = "node_owned"
history_mode = "on_demand"
start_block = 0
publish = "optimistic_and_finalized"

[state]
mode = "checkpointed"

[artifacts]
mode = "none"

[output]
mode = "full"

[delivery]
mode = "window"
max_bytes = "64MiB"
max_age = "24h"
on_limit = "pause"

[checkpoint]
mode = "automatic"
keep = 3

[undo]
mode = "unfinalized"
safety_blocks = 256

[settings]
"#,
    )
    .context("parse built-in block-summary processor contract")?;
    instance.clone_into(&mut processor.instance);
    processor.history_mode = ProcessorHistoryMode::OnDemand;
    if finalized_only {
        processor.publish = PublishMode::FinalizedOnly;
        processor.output.finalized_only = true;
        processor.undo.mode = UndoPolicyMode::None;
        processor.undo.safety_blocks = 0;
    }
    Ok(processor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn built_in_contract_is_header_only_and_on_demand() {
        let configured = processor_config("demo-blocks", false).expect("processor config");
        assert_eq!(configured.id, "block-summary");
        assert_eq!(configured.instance, "demo-blocks");
        assert_eq!(configured.version, "1.0.0");
        assert_eq!(configured.history_mode, ProcessorHistoryMode::OnDemand);
        assert!(configured.settings.is_empty());
    }
}
