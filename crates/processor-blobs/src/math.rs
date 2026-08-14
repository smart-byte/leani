//! Exact EIP-4844 and EIP-7918 fee arithmetic.

use alloy_primitives::U256;
use leani_processor_api::ProcessorError;

pub const BLOB_GAS_PER_BLOB: u64 = 131_072;
const MIN_BLOB_BASE_FEE: u64 = 1;
const BLOB_BASE_COST: u64 = 1 << 13;

fn max_blob_base_fee() -> U256 {
    U256::from(10_u64).pow(U256::from(30_u64))
}

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

/// Calculate the EIP-4844 blob base fee with the legacy application's safety
/// cap.
///
/// # Errors
///
/// Returns an invariant error for invalid parameters or arithmetic overflow.
pub fn get_blob_base_fee(
    excess_blob_gas: u64,
    update_fraction: u64,
) -> Result<U256, ProcessorError> {
    let fee = fake_exponential(
        U256::from(MIN_BLOB_BASE_FEE),
        U256::from(excess_blob_gas),
        U256::from(update_fraction),
    )?;
    Ok(fee.min(max_blob_base_fee()))
}

/// Calculate the EIP-7918 execution-fee-derived floor.
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

/// Calculate the blob fee with the EIP-7918 reserve floor.
///
/// # Errors
///
/// Returns an invariant error for invalid parameters or arithmetic overflow.
pub fn get_blob_base_fee_eip7918(
    excess_blob_gas: u64,
    execution_base_fee: U256,
    update_fraction: u64,
) -> Result<U256, ProcessorError> {
    let raw = get_blob_base_fee(excess_blob_gas, update_fraction)?;
    let floor = calculate_eip7918_floor(execution_base_fee)?;
    Ok(raw.max(floor))
}

pub(crate) fn checked_mul(left: U256, right: U256) -> Result<U256, ProcessorError> {
    left.checked_mul(right)
        .ok_or_else(|| ProcessorError::Invariant("wei multiplication overflow".to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_blobs_money_eip4844_vectors() {
        assert_eq!(
            get_blob_base_fee(30_539_776, 3_338_477).expect("fee"),
            U256::from(9_393_u64)
        );
        assert_eq!(
            get_blob_base_fee(107_610_112, 3_338_477).expect("fee"),
            U256::from(99_710_729_314_173_u64)
        );
    }

    #[test]
    fn matches_blobs_money_eip7691_vectors() {
        assert_eq!(
            get_blob_base_fee(30_539_776, 5_007_716).expect("fee"),
            U256::from(445_u64)
        );
        assert_eq!(
            get_blob_base_fee(107_610_112, 5_007_716).expect("fee"),
            U256::from(2_150_273_305_u64)
        );
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
