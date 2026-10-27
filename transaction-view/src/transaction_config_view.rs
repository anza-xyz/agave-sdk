use crate::{bytes::unchecked_copy_value, transaction_config_frame::TransactionConfigFrame};

#[derive(Debug, Clone, Copy)]
pub struct TransactionConfigView<'a> {
    pub(crate) transaction_config_frame: &'a TransactionConfigFrame,
    pub(crate) bytes: &'a [u8],
}

impl<'a> TransactionConfigView<'a> {
    #[inline(always)]
    pub fn priority_fee_lamports(&self) -> Option<u64> {
        // bit 0 and 1 have been sanitized to be in same state,
        self.transaction_config_frame.word_offset(0).map(|offset| {
            // SAFETY:
            // - The offsets are checked to be valid in the byte slice.
            // - u64 is valid for any bytes
            u64::from_le(unsafe { unchecked_copy_value(self.bytes, offset) })
        })
    }

    #[inline(always)]
    pub fn compute_unit_limit(&self) -> Option<u32> {
        self.transaction_config_frame.word_offset(2).map(|offset| {
            // SAFETY:
            // - The offsets are checked to be valid in the byte slice.
            // - u32 is valid for any bytes
            u32::from_le(unsafe { unchecked_copy_value(self.bytes, offset) })
        })
    }

    #[inline(always)]
    pub fn loaded_accounts_data_size_limit(&self) -> Option<u32> {
        self.transaction_config_frame.word_offset(3).map(|offset| {
            // SAFETY:
            // - The offsets are checked to be valid in the byte slice.
            // - u32 is valid for any bytes
            u32::from_le(unsafe { unchecked_copy_value(self.bytes, offset) })
        })
    }

    #[inline(always)]
    pub fn requested_heap_size(&self) -> Option<u32> {
        self.transaction_config_frame.word_offset(4).map(|offset| {
            // SAFETY:
            // - The offsets are checked to be valid in the byte slice.
            // - u32 is valid for any bytes
            u32::from_le(unsafe { unchecked_copy_value(self.bytes, offset) })
        })
    }

    #[inline(always)]
    pub fn mask(&self) -> u32 {
        self.transaction_config_frame.mask
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn u32le(x: u32) -> [u8; 4] {
        x.to_le_bytes()
    }

    fn u64_words_le(x: u64) -> ([u8; 4], [u8; 4]) {
        let bytes = x.to_le_bytes();
        (
            [bytes[0], bytes[1], bytes[2], bytes[3]],
            [bytes[4], bytes[5], bytes[6], bytes[7]],
        )
    }

    #[test]
    fn test_read_defaults_when_bits_unset() {
        let mask = 0u32;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&[1u8; 2]);
        let mask_offset = bytes.len();
        bytes.extend_from_slice(&mask.to_le_bytes());

        let mut offset = bytes.len();
        let frame =
            TransactionConfigFrame::try_new(&bytes, mask_offset, mask, &mut offset).unwrap();
        let view = TransactionConfigView {
            transaction_config_frame: &frame,
            bytes: &bytes,
        };

        assert!(view.priority_fee_lamports().is_none());
        assert!(view.compute_unit_limit().is_none());
        assert!(view.loaded_accounts_data_size_limit().is_none());
        assert!(view.requested_heap_size().is_none());
    }

    #[test]
    fn test_priority_fee_only() {
        let mask = 0b00011u32;
        let fee = 123_456_789u64;
        let (lo, hi) = u64_words_le(fee);

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&[9u8; 4]);
        let mask_offset = bytes.len();
        bytes.extend_from_slice(&mask.to_le_bytes());
        let values_offset = bytes.len();
        bytes.extend_from_slice(&lo);
        bytes.extend_from_slice(&hi);

        let mut offset = values_offset;
        let frame = TransactionConfigFrame::try_new(&bytes, mask_offset, mask, &mut offset)
            .inspect(|_| assert_eq!(offset, bytes.len()))
            .unwrap();
        assert!(frame.is_present());

        let view = TransactionConfigView {
            transaction_config_frame: &frame,
            bytes: &bytes,
        };
        assert_eq!(view.priority_fee_lamports().unwrap(), fee);
        assert!(view.compute_unit_limit().is_none());
        assert!(view.loaded_accounts_data_size_limit().is_none());
        assert!(view.requested_heap_size().is_none());
    }

    #[test]
    fn test_all_initial_fields_present() {
        // bits 0,1,2,3,4 => priority fee + cu + loaded data size + heap size
        let mask = 0b1_1111u32;
        let fee = 99u64;
        let cu = 1_400_000u32;
        let loaded = 64_000u32;
        let heap = 64 * 1024u32;

        let (fee_lo, fee_hi) = u64_words_le(fee);

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&[9u8; 7]);
        let mask_offset = bytes.len();
        bytes.extend_from_slice(&mask.to_le_bytes());
        let values_offset = bytes.len();
        bytes.extend_from_slice(&fee_lo);
        bytes.extend_from_slice(&fee_hi);
        bytes.extend_from_slice(&u32le(cu));
        bytes.extend_from_slice(&u32le(loaded));
        bytes.extend_from_slice(&u32le(heap));

        let mut offset = values_offset;
        let frame = TransactionConfigFrame::try_new(&bytes, mask_offset, mask, &mut offset)
            .inspect(|_| assert_eq!(offset, bytes.len()))
            .unwrap();
        assert!(frame.is_present());

        let view = TransactionConfigView {
            transaction_config_frame: &frame,
            bytes: &bytes,
        };
        assert_eq!(view.priority_fee_lamports().unwrap(), fee);
        assert_eq!(view.compute_unit_limit().unwrap(), cu);
        assert_eq!(view.loaded_accounts_data_size_limit().unwrap(), loaded);
        assert_eq!(view.requested_heap_size().unwrap(), heap);
    }

    #[test]
    fn test_sparse_bits() {
        // bits 2 and 4 only
        let mask = 0b10100u32;
        let cu = 777u32;
        let heap = 48 * 1024u32;

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&[1u8; 3]);
        let mask_offset = bytes.len();
        bytes.extend_from_slice(&mask.to_le_bytes());
        let values_offset = bytes.len();
        bytes.extend_from_slice(&u32le(cu)); // bit 2 -> word 0
        bytes.extend_from_slice(&u32le(heap)); // bit 4 -> word 1

        let mut offset = values_offset;
        let frame = TransactionConfigFrame::try_new(&bytes, mask_offset, mask, &mut offset)
            .inspect(|_| assert_eq!(offset, bytes.len()))
            .unwrap();

        let view = TransactionConfigView {
            transaction_config_frame: &frame,
            bytes: &bytes,
        };
        assert!(view.priority_fee_lamports().is_none());
        assert_eq!(view.compute_unit_limit().unwrap(), cu);
        assert!(view.loaded_accounts_data_size_limit().is_none());
        assert_eq!(view.requested_heap_size().unwrap(), heap);
    }
}
