use {
    crate::{
        address_table_lookup_frame::{AddressTableLookupFrame, AddressTableLookupIterator},
        bytes::{
            advance_offset_for_array, advance_offset_for_type, check_remaining, checked_offset,
            unchecked_copy_value, unchecked_read_byte,
        },
        instructions_frame::{InstructionsFrame, InstructionsIterator},
        message_header_frame::MessageHeaderFrame,
        result::{Result, TransactionViewError},
        static_account_keys_frame::StaticAccountKeysFrame,
        transaction_config_frame::TransactionConfigFrame,
        transaction_version::TransactionVersion,
    },
    solana_hash::Hash,
    solana_pubkey::Pubkey,
};

/// Framing data for a message, i.e. the signed portion of a transaction.
/// Offsets are relative to the start of the parsed buffer, which is the
/// whole transaction if the message was parsed as part of one.
#[derive(Debug, Clone)]
pub(crate) struct MessageFrame {
    /// Message header framing data.
    pub(crate) message_header: MessageHeaderFrame,
    /// Static account keys framing data.
    pub(crate) static_account_keys: StaticAccountKeysFrame,
    /// Recent blockhash offset.
    recent_blockhash_offset: u16,
    /// Instructions framing data.
    pub(crate) instructions: InstructionsFrame,
    /// Address table lookup framing data.
    pub(crate) address_table_lookup: AddressTableLookupFrame,
    /// Transaction config framing data
    transaction_config_frame: TransactionConfigFrame,
    /// The offset one past the last byte of the message.
    pub(crate) end_offset: u16,
}

impl MessageFrame {
    /// Parse a serialized message and verify basic structure.
    /// The `bytes` parameter must have no trailing data.
    pub(crate) fn try_new(bytes: &[u8]) -> Result<Self> {
        // Unlike transactions, legacy/v0 and v1 messages can only be told
        // apart by the full version byte, since v0 messages also have the
        // MSB set.
        let message_frame = if bytes.first() == Some(&solana_message::v1::V1_PREFIX) {
            Self::try_new_as_v1(bytes)?
        } else {
            Self::try_new_as_legacy_or_v0(bytes, 0)?
        };
        // Verify that the entire buffer was parsed.
        if usize::from(message_frame.end_offset) != bytes.len() {
            return Err(TransactionViewError::ParseError);
        }
        Ok(message_frame)
    }

    /// Parse a legacy or v0 message starting at `offset`.
    pub(crate) fn try_new_as_legacy_or_v0(bytes: &[u8], mut offset: usize) -> Result<Self> {
        let message_header = MessageHeaderFrame::try_new(bytes, &mut offset)?;
        let static_account_keys = StaticAccountKeysFrame::try_new(bytes, &mut offset)?;

        // The recent blockhash is the first account key after the static
        // account keys. The recent blockhash is always present in a valid
        // message and has a fixed size of 32 bytes.
        let recent_blockhash_offset = checked_offset(offset)?;
        advance_offset_for_type::<Hash>(bytes, &mut offset)?;

        let instructions = InstructionsFrame::try_new_for_legacy_and_v0(bytes, &mut offset)?;
        let address_table_lookup = match message_header.version {
            TransactionVersion::Legacy => AddressTableLookupFrame {
                num_address_table_lookups: 0,
                offset: 0,
                total_writable_lookup_accounts: 0,
                total_readonly_lookup_accounts: 0,
            },
            TransactionVersion::V0 => AddressTableLookupFrame::try_new(bytes, &mut offset)?,
            TransactionVersion::V1 => unreachable!("unexpected variant"),
        };

        Ok(Self {
            message_header,
            static_account_keys,
            recent_blockhash_offset,
            instructions,
            address_table_lookup,
            transaction_config_frame: TransactionConfigFrame::not_applicable(),
            end_offset: checked_offset(offset)?,
        })
    }

    /// Parse a v1 message. A v1 message starts at the beginning of `bytes`,
    /// whether on its own or as part of a transaction.
    pub(crate) fn try_new_as_v1(bytes: &[u8]) -> Result<Self> {
        let mut offset: usize = 0;

        // Fixed-size txv1 prefix up through NumAddresses:
        // VersionByte (u8)
        // LegacyHeader (u8, u8, u8)
        // TransactionConfigMask (u32)
        // LifetimeSpecifier ([u8; 32])
        // NumInstructions (u8)
        // NumAddresses (u8)
        const FIXED_V1_PREFIX_LEN: usize = 1 + 3 + 4 + core::mem::size_of::<Hash>() + 1 + 1;

        check_remaining(bytes, offset, FIXED_V1_PREFIX_LEN)?;

        // SAFETY: have checked bytes have enough space for prefix all the way up to
        //         NumAddresses.

        // message offset is the version byte, the first byte of a v1 message
        let message_offset = offset as u16;
        // Version Byte
        let version = unsafe { unchecked_read_byte(bytes, &mut offset) };
        let version = match version {
            solana_message::v1::V1_PREFIX => TransactionVersion::V1,
            _ => return Err(TransactionViewError::ParseError),
        };
        // Legacy Header
        let num_required_signatures = unsafe { unchecked_read_byte(bytes, &mut offset) };
        let num_readonly_signed_accounts = unsafe { unchecked_read_byte(bytes, &mut offset) };
        let num_readonly_unsigned_accounts = unsafe { unchecked_read_byte(bytes, &mut offset) };
        // Transaction Config Bit Mask
        let transaction_config_mask_offset = offset;
        let transaction_config_mask: u32 = unsafe { unchecked_copy_value(bytes, offset) };
        offset = offset.wrapping_add(core::mem::size_of::<u32>());
        // Lifetime specifier
        let recent_blockhash_offset = checked_offset(offset)?;
        offset = offset.wrapping_add(core::mem::size_of::<Hash>());
        // Num instructions and addresses
        let num_instructions = unsafe { unchecked_read_byte(bytes, &mut offset) };
        let num_addresses = unsafe { unchecked_read_byte(bytes, &mut offset) };

        // addresses
        let addresses_offset = checked_offset(offset)?;
        advance_offset_for_array::<Pubkey>(bytes, &mut offset, u16::from(num_addresses))?;
        // config value slots: one 4-byte slot per set bit in mask
        let transaction_config_frame = TransactionConfigFrame::try_new(
            bytes,
            transaction_config_mask_offset,
            transaction_config_mask,
            &mut offset,
        )?;
        // instruction headers and payloads
        let instructions = InstructionsFrame::try_new_for_v1(bytes, &mut offset, num_instructions)?;
        let frame = Self {
            message_header: MessageHeaderFrame {
                offset: message_offset,
                version,
                num_required_signatures,
                num_readonly_signed_accounts,
                num_readonly_unsigned_accounts,
            },
            static_account_keys: StaticAccountKeysFrame {
                num_static_accounts: num_addresses, // always static accounts in txv1
                offset: addresses_offset,
            },
            recent_blockhash_offset,
            instructions,
            // Don't have ATL in txv1
            address_table_lookup: AddressTableLookupFrame {
                num_address_table_lookups: 0,
                offset: 0,
                total_writable_lookup_accounts: 0,
                total_readonly_lookup_accounts: 0,
            },
            transaction_config_frame,
            end_offset: checked_offset(offset)?,
        };

        Ok(frame)
    }

    /// Return the version of the message.
    #[inline]
    pub(crate) fn version(&self) -> TransactionVersion {
        self.message_header.version
    }

    /// Return the number of required signatures in the message.
    #[inline]
    pub(crate) fn num_required_signatures(&self) -> u8 {
        self.message_header.num_required_signatures
    }

    /// Return the number of readonly signed static accounts in the message.
    #[inline]
    pub(crate) fn num_readonly_signed_static_accounts(&self) -> u8 {
        self.message_header.num_readonly_signed_accounts
    }

    /// Return the number of readonly unsigned static accounts in the message.
    #[inline]
    pub(crate) fn num_readonly_unsigned_static_accounts(&self) -> u8 {
        self.message_header.num_readonly_unsigned_accounts
    }

    /// Return the number of static account keys in the message.
    #[inline]
    pub(crate) fn num_static_account_keys(&self) -> u8 {
        self.static_account_keys.num_static_accounts
    }

    /// Return the number of instructions in the message.
    #[inline]
    pub(crate) fn num_instructions(&self) -> u16 {
        self.instructions.num_instructions()
    }

    /// Return the number of address table lookups in the message.
    #[inline]
    pub(crate) fn num_address_table_lookups(&self) -> u8 {
        self.address_table_lookup.num_address_table_lookups
    }

    /// Return the number of writable lookup accounts in the message.
    #[inline]
    pub(crate) fn total_writable_lookup_accounts(&self) -> u16 {
        self.address_table_lookup.total_writable_lookup_accounts
    }

    /// Return the number of readonly lookup accounts in the message.
    #[inline]
    pub(crate) fn total_readonly_lookup_accounts(&self) -> u16 {
        self.address_table_lookup.total_readonly_lookup_accounts
    }

    /// Return the range of the message as `(begin, end)`, where `end` is
    /// exclusive.
    #[inline]
    pub(crate) fn message_range(&self) -> (u16, u16) {
        (self.message_header.offset, self.end_offset)
    }

    /// Return transaction_config_frame
    #[inline]
    pub(crate) fn transaction_config_frame(&self) -> &TransactionConfigFrame {
        &self.transaction_config_frame
    }
}

// Separate implementation for `unsafe` accessor methods.
impl MessageFrame {
    /// Return the slice of static account keys in the message.
    ///
    /// # Safety
    ///  - This function must be called with the same `bytes` slice that was
    ///    used to create the `MessageFrame` instance.
    #[inline]
    pub(crate) unsafe fn static_account_keys<'a>(&self, bytes: &'a [u8]) -> &'a [Pubkey] {
        // Verify at compile time there are no alignment constraints.
        const _: () = assert!(core::mem::align_of::<Pubkey>() == 1, "Pubkey alignment");
        // The length of the slice is not greater than isize::MAX.
        const _: () =
            assert!(u8::MAX as usize * core::mem::size_of::<Pubkey>() <= isize::MAX as usize);

        // SAFETY:
        // - If this `MessageFrame` was created from `bytes`:
        //     - the pointer is valid for the range and is properly aligned.
        // - `num_static_accounts` has been verified against the bounds if
        //   `MessageFrame` was created successfully.
        // - `Pubkey` are just byte arrays; there is no possibility the
        //   `Pubkey` are not initialized properly.
        // - The lifetime of the returned slice is the same as the input
        //   `bytes`. This means it will not be mutated or deallocated while
        //   holding the slice.
        // - The length does not overflow `isize`.
        unsafe {
            core::slice::from_raw_parts(
                bytes
                    .as_ptr()
                    .add(usize::from(self.static_account_keys.offset))
                    as *const Pubkey,
                usize::from(self.static_account_keys.num_static_accounts),
            )
        }
    }

    /// Return the recent blockhash in the message.
    /// # Safety
    /// - This function must be called with the same `bytes` slice that was
    ///   used to create the `MessageFrame` instance.
    #[inline]
    pub(crate) unsafe fn recent_blockhash<'a>(&self, bytes: &'a [u8]) -> &'a Hash {
        // Verify at compile time there are no alignment constraints.
        const _: () = assert!(core::mem::align_of::<Hash>() == 1, "Hash alignment");

        // SAFETY:
        // - The pointer is correctly aligned (no alignment constraints).
        // - `Hash` is just a byte array; there is no possibility the `Hash`
        //   is not initialized properly.
        // - Aliasing rules are respected because the lifetime of the returned
        //   reference is the same as the input/source `bytes`.
        unsafe {
            &*(bytes
                .as_ptr()
                .add(usize::from(self.recent_blockhash_offset)) as *const Hash)
        }
    }

    /// Return an iterator over the instructions in the message.
    /// # Safety
    /// - This function must be called with the same `bytes` slice that was
    ///   used to create the `MessageFrame` instance.
    #[inline]
    pub(crate) unsafe fn instructions_iter<'a>(
        &'a self,
        bytes: &'a [u8],
    ) -> InstructionsIterator<'a> {
        self.instructions.iter(bytes)
    }

    /// Return an iterator over the address table lookups in the message.
    /// # Safety
    /// - This function must be called with the same `bytes` slice that was
    ///   used to create the `MessageFrame` instance.
    #[inline]
    pub(crate) unsafe fn address_table_lookup_iter<'a>(
        &self,
        bytes: &'a [u8],
    ) -> AddressTableLookupIterator<'a> {
        AddressTableLookupIterator {
            bytes,
            offset: usize::from(self.address_table_lookup.offset),
            num_address_table_lookups: self.address_table_lookup.num_address_table_lookups,
            index: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_message::{Message, VersionedMessage},
    };

    #[test]
    fn test_try_new_as_v1_rejects_legacy_message() {
        // A legacy message with one required signature starts with 0x01,
        // which differs from the v1 version byte only in the MSB.
        let bytes = wincode::serialize(&VersionedMessage::Legacy(Message::new(
            &[],
            Some(&Pubkey::new_unique()),
        )))
        .unwrap();
        assert_eq!(bytes[0], 1);

        assert!(matches!(
            MessageFrame::try_new_as_v1(&bytes),
            Err(TransactionViewError::ParseError),
        ));
    }
}
