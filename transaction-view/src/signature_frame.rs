use {
    crate::{
        bytes::{advance_offset_for_array, read_byte},
        result::{Result, TransactionViewError},
    },
    solana_signature::Signature,
};

/// Metadata for accessing transaction-level signatures in a transaction view.
#[derive(Debug, Clone)]
pub(crate) struct SignatureFrame {
    /// The number of signatures in the transaction.
    pub(crate) num_signatures: u8,
    /// Offset to the first signature in the transaction packet.
    pub(crate) offset: u16,
}

impl SignatureFrame {
    /// Get the number of signatures and the offset to the first signature in
    /// the transaction packet, starting at the given `offset`.
    #[inline(always)]
    pub(crate) fn try_new(bytes: &[u8], offset: &mut usize) -> Result<Self> {
        let num_signatures = read_byte(bytes, offset)?;
        // A high bit denotes a multi-byte compact-u16 count, which cannot be
        // distinguished from the version byte of a v1 transaction here.
        if num_signatures == 0 || num_signatures & 0x80 != 0 {
            return Err(TransactionViewError::ParseError);
        }

        let signature_offset = *offset as u16;
        advance_offset_for_array::<Signature>(bytes, offset, u16::from(num_signatures))?;

        Ok(Self {
            num_signatures,
            offset: signature_offset,
        })
    }

    /// Return the slice of signatures in the transaction.
    /// # Safety
    ///   - This function must be called with the same `bytes` slice that was
    ///     used to create the `SignatureFrame` instance.
    #[inline]
    pub(crate) unsafe fn signatures<'a>(&self, bytes: &'a [u8]) -> &'a [Signature] {
        // Verify at compile time there are no alignment constraints.
        const _: () = assert!(
            core::mem::align_of::<Signature>() == 1,
            "Signature alignment"
        );
        // The length of the slice is not greater than isize::MAX.
        const _: () =
            assert!(u8::MAX as usize * core::mem::size_of::<Signature>() <= isize::MAX as usize);

        // SAFETY:
        // - If this `SignatureFrame` was created from `bytes`:
        //     - the pointer is valid for the range and is properly aligned.
        // - `num_signatures` has been verified against the bounds if
        //   `SignatureFrame` was created successfully.
        // - `Signature` are just byte arrays; there is no possibility the
        //   `Signature` are not initialized properly.
        // - The lifetime of the returned slice is the same as the input
        //   `bytes`. This means it will not be mutated or deallocated while
        //   holding the slice.
        // - The length does not overflow `isize`.
        unsafe {
            core::slice::from_raw_parts(
                bytes.as_ptr().add(usize::from(self.offset)) as *const Signature,
                usize::from(self.num_signatures),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, solana_short_vec::ShortU16, wincode::Serialize};

    fn serialize_short_vec(vec: &Vec<Signature>) -> Vec<u8> {
        wincode::containers::Vec::<Signature, ShortU16>::serialize(vec).unwrap()
    }

    #[test]
    fn test_zero_signatures() {
        let bytes = serialize_short_vec(&vec![]);
        let mut offset = 0;
        assert!(SignatureFrame::try_new(&bytes, &mut offset).is_err());
    }

    #[test]
    fn test_one_signature() {
        let bytes = serialize_short_vec(&vec![Signature::default()]);
        let mut offset = 0;
        let frame = SignatureFrame::try_new(&bytes, &mut offset).unwrap();
        assert_eq!(frame.num_signatures, 1);
        assert_eq!(frame.offset, 1);
        assert_eq!(offset, 1 + core::mem::size_of::<Signature>());
    }

    #[test]
    fn test_signatures_above_packet_policy_limit() {
        let signatures = vec![Signature::default(); 13];
        let bytes = serialize_short_vec(&signatures);
        let mut offset = 0;
        let frame = SignatureFrame::try_new(&bytes, &mut offset).unwrap();
        assert_eq!(frame.num_signatures, 13);
        assert_eq!(frame.offset, 1);
        assert_eq!(offset, 1 + 13 * core::mem::size_of::<Signature>());
    }

    #[test]
    fn test_non_zero_offset() {
        let mut bytes = serialize_short_vec(&vec![Signature::default()]);
        bytes.insert(0, 0); // Insert a byte at the beginning of the packet.
        let mut offset = 1; // Start at the second byte.
        let frame = SignatureFrame::try_new(&bytes, &mut offset).unwrap();
        assert_eq!(frame.num_signatures, 1);
        assert_eq!(frame.offset, 2);
        assert_eq!(offset, 2 + core::mem::size_of::<Signature>());
    }

    #[test]
    fn test_multibyte_signature_count() {
        let signatures = vec![Signature::default(); 128];
        let bytes = serialize_short_vec(&signatures);
        let mut offset = 0;
        assert!(SignatureFrame::try_new(&bytes, &mut offset).is_err());
    }

    #[test]
    fn test_u16_max_signatures() {
        let signatures = vec![Signature::default(); u16::MAX as usize];
        let bytes = serialize_short_vec(&signatures);
        let mut offset = 0;
        assert!(SignatureFrame::try_new(&bytes, &mut offset).is_err());
    }
}
