//! Lossless conversions at the Alloy boundary.

use alloy_primitives::{Address as AlloyAddress, B256, U256};

use crate::{Address, BlockHash, Quantity, TransactionHash};

impl From<B256> for BlockHash {
    fn from(value: B256) -> Self {
        let mut bytes = [0; 32];
        bytes.copy_from_slice(value.as_slice());
        Self(bytes)
    }
}

impl From<BlockHash> for B256 {
    fn from(value: BlockHash) -> Self {
        Self::from(value.0)
    }
}

impl From<B256> for TransactionHash {
    fn from(value: B256) -> Self {
        let mut bytes = [0; 32];
        bytes.copy_from_slice(value.as_slice());
        Self(bytes)
    }
}

impl From<TransactionHash> for B256 {
    fn from(value: TransactionHash) -> Self {
        Self::from(value.0)
    }
}

impl From<AlloyAddress> for Address {
    fn from(value: AlloyAddress) -> Self {
        let mut bytes = [0; 20];
        bytes.copy_from_slice(value.as_slice());
        Self(bytes)
    }
}

impl From<Address> for AlloyAddress {
    fn from(value: Address) -> Self {
        Self::from(value.0)
    }
}

impl From<U256> for Quantity {
    fn from(value: U256) -> Self {
        Self(value.to_be_bytes())
    }
}

impl From<Quantity> for U256 {
    fn from(value: Quantity) -> Self {
        Self::from_be_bytes(value.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloy_conversions_are_lossless() {
        let hash = B256::repeat_byte(0xa5);
        assert_eq!(B256::from(BlockHash::from(hash)), hash);
        assert_eq!(B256::from(TransactionHash::from(hash)), hash);

        let address = AlloyAddress::repeat_byte(0x42);
        assert_eq!(AlloyAddress::from(Address::from(address)), address);

        let quantity = U256::from_be_bytes([0xff; 32]);
        assert_eq!(
            <U256 as From<Quantity>>::from(Quantity::from(quantity)),
            quantity
        );
    }
}
