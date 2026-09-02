//! Built-in configuration contract for Ethereum block summaries.

use leani_processor_block_summary::BLOCK_SUMMARY_VERSION;

use crate::{builtin_processors::processor_contract, config::ProcessorConfig};

pub(crate) fn processor_config(instance: &str, finalized_only: bool) -> ProcessorConfig {
    processor_contract(
        "block-summary",
        instance,
        BLOCK_SUMMARY_VERSION,
        0,
        finalized_only,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ProcessorHistoryMode;

    #[test]
    fn built_in_contract_is_receipt_free_and_on_demand() {
        let configured = processor_config("demo-blocks", false);
        assert_eq!(configured.id, "block-summary");
        assert_eq!(configured.instance, "demo-blocks");
        assert_eq!(configured.version, "1.1.0");
        assert_eq!(configured.history_mode, ProcessorHistoryMode::OnDemand);
        assert!(configured.settings.is_empty());
    }
}
