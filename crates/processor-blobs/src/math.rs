//! Exact EIP-4844 and EIP-7918 fee arithmetic.

use alloy_primitives::U256;
use leani_processor_api::ProcessorError;

pub const BLOB_GAS_PER_BLOB: u64 = 131_072;
const MIN_BLOB_BASE_FEE: u64 = 1;
const BLOB_BASE_COST: u64 = 1 << 13;

/// Integer Taylor expansion specified by EIP-4844.
///
/// # Errors
///
/// Returns an invariant error instead of wrapping a 256-bit intermediate.
pub fn fake_exponential(
    factor: U256,
    numerator: U256,
    denominator: U256,
) -> Result<U256, ProcessorError> {
    if denominator == U256::ZERO {
        return Err(ProcessorError::Invariant(
            "blob fee denominator is zero".to_owned(),
        ));
    }
    let mut output = U256::ZERO;
    let mut numerator_accumulator = factor
        .checked_mul(denominator)
        .ok_or_else(|| ProcessorError::Invariant("blob fee overflow".to_owned()))?;
    let mut iteration = U256::from(1_u64);
    while numerator_accumulator > U256::ZERO {
        output = output
            .checked_add(numerator_accumulator)
            .ok_or_else(|| ProcessorError::Invariant("blob fee overflow".to_owned()))?;
        let divisor = denominator
            .checked_mul(iteration)
            .ok_or_else(|| ProcessorError::Invariant("blob fee overflow".to_owned()))?;
        numerator_accumulator = numerator_accumulator
            .checked_mul(numerator)
            .ok_or_else(|| ProcessorError::Invariant("blob fee overflow".to_owned()))?
            / divisor;
        iteration = iteration
            .checked_add(U256::from(1_u64))
            .ok_or_else(|| ProcessorError::Invariant("blob fee overflow".to_owned()))?;
    }
    Ok(output / denominator)
}

/// Calculate the protocol blob base fee, EIP-4844's
/// `get_base_fee_per_blob_gas`: the price every unit of blob gas pays and
/// burns, for the update fraction of the fork active at the block.
///
/// EIP-7918 leaves this fee unchanged. Its reserve price
/// ([`calculate_eip7918_floor`]) only changes how excess blob gas evolves, so
/// callers report it beside the fee instead of applying it.
///
/// # Errors
///
/// Returns an invariant error for a zero update fraction or when a 256-bit
/// intermediate would overflow, which takes an excess of about 145 update
/// fractions, far beyond any fee a block could charge. Either way the
/// expansion ends within about 410 iterations for `u64` inputs.
pub fn get_blob_base_fee(
    excess_blob_gas: u64,
    update_fraction: u64,
) -> Result<U256, ProcessorError> {
    fake_exponential(
        U256::from(MIN_BLOB_BASE_FEE),
        U256::from(excess_blob_gas),
        U256::from(update_fraction),
    )
}

/// Calculate the EIP-7918 reserve price per blob gas: the execution base fee
/// times `BLOB_BASE_COST / GAS_PER_BLOB`.
///
/// # Errors
///
/// Returns an invariant error if the 256-bit multiplication overflows.
pub fn calculate_eip7918_floor(execution_base_fee: U256) -> Result<U256, ProcessorError> {
    execution_base_fee
        .checked_mul(U256::from(BLOB_BASE_COST))
        .map(|value| value / U256::from(BLOB_GAS_PER_BLOB))
        .ok_or_else(|| ProcessorError::Invariant("reserve fee overflow".to_owned()))
}

pub(crate) fn checked_mul(left: U256, right: U256) -> Result<U256, ProcessorError> {
    left.checked_mul(right)
        .ok_or_else(|| ProcessorError::Invariant("wei multiplication overflow".to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_base_fee_matches_the_exact_integer_reference() {
        // Outputs of the EIP-4844 `fake_exponential` in exact Python 3
        // integers, per mainnet update fraction: Cancun, Prague (and Fusaka),
        // BPO1, BPO2.
        let cases = [
            (3_338_477, 0, 1_u128),
            (3_338_477, 30_539_776, 9_393),
            (3_338_477, 107_610_112, 99_710_729_314_173),
            (5_007_716, 0, 1),
            (5_007_716, 30_539_776, 445),
            (5_007_716, 107_610_112, 2_150_273_305),
            (8_346_193, 0, 1),
            (8_346_193, 30_539_776, 38),
            (8_346_193, 107_610_112, 397_645),
            (11_684_671, 0, 1),
            (11_684_671, 30_539_776, 13),
            (11_684_671, 107_610_112, 9_991),
            // Above the former 10^30 cap.
            (
                3_338_477,
                250_000_000,
                332_584_186_920_530_080_845_367_541_284_883,
            ),
        ];
        for (update_fraction, excess_blob_gas, fee) in cases {
            assert_eq!(
                get_blob_base_fee(excess_blob_gas, update_fraction).expect("fee"),
                U256::from(fee),
                "update fraction {update_fraction}, excess {excess_blob_gas}"
            );
        }
    }

    #[test]
    fn eip7918_floor_is_execution_base_fee_divided_by_sixteen() {
        let base = U256::from(32_000_000_000_u64);
        assert_eq!(
            calculate_eip7918_floor(base).expect("floor"),
            U256::from(2_000_000_000_u64)
        );
    }
}
