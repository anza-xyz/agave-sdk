use {
    crate::{
        bytes::{advance_offset_for_array, checked_offset},
        message_frame::MessageFrame,
        result::{Result, TransactionViewError},
        signature_frame::SignatureFrame,
    },
    solana_signature::Signature,
};

#[derive(Debug, Clone)]
pub(crate) struct TransactionFrame {
    /// Signature framing data.
    pub(crate) signature: SignatureFrame,
    /// Message framing data.
    pub(crate) message: MessageFrame,
    /// The length of the serialized transaction in bytes. This may be less
    /// than the length of the buffer the frame was parsed from if trailing
    /// bytes were allowed.
    pub(crate) data_len: u16,
}

impl TransactionFrame {
    /// Parse a serialized transaction and verify basic structure.
    /// The `bytes` parameter must have no trailing data.
    pub(crate) fn try_new(bytes: &[u8]) -> Result<Self> {
        let transaction_frame = Self::try_new_from_prefix(bytes)?;
        // Verify that the entire buffer was parsed.
        if usize::from(transaction_frame.data_len) != bytes.len() {
            return Err(TransactionViewError::ParseError);
        }
        Ok(transaction_frame)
    }

    /// Parse a serialized transaction from the front of `bytes` and verify
    /// basic structure. Any bytes after the serialized transaction are
    /// ignored; [`Self::data_len`] holds where the transaction ends.
    pub(crate) fn try_new_from_prefix(bytes: &[u8]) -> Result<Self> {
        if Self::is_legacy_or_v0(bytes)? {
            Self::try_new_as_legacy_or_v0(bytes)
        } else {
            Self::try_new_as_v1(bytes)
        }
    }

    fn try_new_as_legacy_or_v0(bytes: &[u8]) -> Result<Self> {
        let mut offset = 0;
        let signature = SignatureFrame::try_new(bytes, &mut offset)?;
        let message = MessageFrame::try_new_as_legacy_or_v0(bytes, offset)?;
        Ok(Self {
            signature,
            data_len: message.end_offset,
            message,
        })
    }

    fn try_new_as_v1(bytes: &[u8]) -> Result<Self> {
        let message = MessageFrame::try_new_as_v1(bytes)?;
        // signatures follow the message, one per required signature
        let num_signatures = message.num_required_signatures();
        let mut offset = usize::from(message.end_offset);
        advance_offset_for_array::<Signature>(bytes, &mut offset, u16::from(num_signatures))?;
        Ok(Self {
            signature: SignatureFrame {
                num_signatures,
                offset: message.end_offset,
            },
            message,
            data_len: checked_offset(offset)?,
        })
    }

    fn is_legacy_or_v0(bytes: &[u8]) -> Result<bool> {
        let first_byte = *bytes.first().ok_or(TransactionViewError::ParseError)?;

        // In wire format:
        // - Legacy/v0 transactions start with signatures (compact-u16 count).
        //   Packet size limits keep the signature count well below 128, so the
        //   first byte never has MSB set.
        // - v1 transactions start with a version byte with MSB = 1.
        Ok((first_byte & solana_message::MESSAGE_VERSION_PREFIX) == 0)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::transaction_version::TransactionVersion,
        solana_hash::Hash,
        solana_message::{
            AddressLookupTableAccount, Message, MessageHeader, VersionedMessage,
            compiled_instruction::CompiledInstruction, v0, v1,
        },
        solana_pubkey::Pubkey,
        solana_signature::Signature,
        solana_system_interface::instruction::{self as system_instruction, SystemInstruction},
        solana_transaction::versioned::VersionedTransaction,
    };

    fn verify_transaction_view_frame(tx: &VersionedTransaction) {
        let bytes = wincode::serialize(tx).unwrap();
        let frame = TransactionFrame::try_new(&bytes).unwrap();

        assert_eq!(frame.signature.num_signatures, tx.signatures.len() as u8);
        assert_eq!(frame.signature.offset as usize, 1);

        assert_eq!(
            frame.message.message_header.num_required_signatures,
            tx.message.header().num_required_signatures
        );
        assert_eq!(
            frame.message.message_header.num_readonly_signed_accounts,
            tx.message.header().num_readonly_signed_accounts
        );
        assert_eq!(
            frame.message.message_header.num_readonly_unsigned_accounts,
            tx.message.header().num_readonly_unsigned_accounts
        );

        assert_eq!(
            frame.message.static_account_keys.num_static_accounts,
            tx.message.static_account_keys().len() as u8
        );
        assert_eq!(
            frame.message.instructions.num_instructions(),
            tx.message.instructions().len() as u16
        );
        assert_eq!(
            frame.message.address_table_lookup.num_address_table_lookups,
            tx.message
                .address_table_lookups()
                .map(|x| x.len() as u8)
                .unwrap_or(0)
        );
    }

    fn minimally_sized_transaction() -> VersionedTransaction {
        VersionedTransaction {
            signatures: vec![Signature::default()], // 1 signature to be valid.
            message: VersionedMessage::Legacy(Message {
                header: MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                account_keys: vec![Pubkey::default()],
                recent_blockhash: Hash::default(),
                instructions: vec![],
            }),
        }
    }

    fn simple_transfer() -> VersionedTransaction {
        let payer = Pubkey::new_unique();
        VersionedTransaction {
            signatures: vec![Signature::default()], // 1 signature to be valid.
            message: VersionedMessage::Legacy(Message::new(
                &[system_instruction::transfer(
                    &payer,
                    &Pubkey::new_unique(),
                    1,
                )],
                Some(&payer),
            )),
        }
    }

    fn simple_transfer_v0() -> VersionedTransaction {
        let payer = Pubkey::new_unique();
        VersionedTransaction {
            signatures: vec![Signature::default()], // 1 signature to be valid.
            message: VersionedMessage::V0(
                v0::Message::try_compile(
                    &payer,
                    &[system_instruction::transfer(
                        &payer,
                        &Pubkey::new_unique(),
                        1,
                    )],
                    &[],
                    Hash::default(),
                )
                .unwrap(),
            ),
        }
    }

    fn multiple_transfers() -> VersionedTransaction {
        let payer = Pubkey::new_unique();
        VersionedTransaction {
            signatures: vec![Signature::default()], // 1 signature to be valid.
            message: VersionedMessage::Legacy(Message::new(
                &[
                    system_instruction::transfer(&payer, &Pubkey::new_unique(), 1),
                    system_instruction::transfer(&payer, &Pubkey::new_unique(), 1),
                ],
                Some(&payer),
            )),
        }
    }

    fn v0_with_single_lookup() -> VersionedTransaction {
        let payer = Pubkey::new_unique();
        let to = Pubkey::new_unique();
        VersionedTransaction {
            signatures: vec![Signature::default()], // 1 signature to be valid.
            message: VersionedMessage::V0(
                v0::Message::try_compile(
                    &payer,
                    &[system_instruction::transfer(&payer, &to, 1)],
                    &[AddressLookupTableAccount {
                        key: Pubkey::new_unique(),
                        addresses: vec![to],
                    }],
                    Hash::default(),
                )
                .unwrap(),
            ),
        }
    }

    fn v0_with_multiple_lookups() -> VersionedTransaction {
        let payer = Pubkey::new_unique();
        let to1 = Pubkey::new_unique();
        let to2 = Pubkey::new_unique();
        VersionedTransaction {
            signatures: vec![Signature::default()], // 1 signature to be valid.
            message: VersionedMessage::V0(
                v0::Message::try_compile(
                    &payer,
                    &[
                        system_instruction::transfer(&payer, &to1, 1),
                        system_instruction::transfer(&payer, &to2, 1),
                    ],
                    &[
                        AddressLookupTableAccount {
                            key: Pubkey::new_unique(),
                            addresses: vec![to1],
                        },
                        AddressLookupTableAccount {
                            key: Pubkey::new_unique(),
                            addresses: vec![to2],
                        },
                    ],
                    Hash::default(),
                )
                .unwrap(),
            ),
        }
    }

    fn simple_v1_transaction() -> VersionedTransaction {
        let payer = Pubkey::new_unique();
        let program = Pubkey::new_unique();
        let other = Pubkey::new_unique();

        VersionedTransaction {
            signatures: vec![Signature::default()],
            message: VersionedMessage::V1(v1::Message {
                header: MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 1,
                },
                config: v1::TransactionConfig {
                    priority_fee: Some(123),
                    compute_unit_limit: Some(456),
                    loaded_accounts_data_size_limit: Some(789),
                    heap_size: Some(1024),
                },
                lifetime_specifier: Hash::default(),
                account_keys: vec![payer, other, program],
                instructions: vec![
                    CompiledInstruction {
                        program_id_index: 2,
                        accounts: vec![0, 1],
                        data: vec![10, 11, 12],
                    },
                    CompiledInstruction {
                        program_id_index: 2,
                        accounts: vec![],
                        data: vec![99],
                    },
                ],
            }),
        }
    }

    #[test]
    fn test_minimal_sized_transaction() {
        verify_transaction_view_frame(&minimally_sized_transaction());
    }

    #[test]
    fn test_simple_transfer() {
        verify_transaction_view_frame(&simple_transfer());
    }

    #[test]
    fn test_simple_transfer_v0() {
        verify_transaction_view_frame(&simple_transfer_v0());
    }

    #[test]
    fn test_v0_with_lookup() {
        verify_transaction_view_frame(&v0_with_single_lookup());
    }

    #[test]
    fn test_trailing_byte() {
        let tx = simple_transfer();
        let mut bytes = wincode::serialize(&tx).unwrap();
        bytes.push(0);
        assert!(TransactionFrame::try_new(&bytes).is_err());
    }

    #[test]
    fn test_insufficient_bytes() {
        let tx = simple_transfer();
        let bytes = wincode::serialize(&tx).unwrap();
        assert!(TransactionFrame::try_new(&bytes[..bytes.len().wrapping_sub(1)]).is_err());
    }

    #[test]
    fn test_signature_overflow() {
        let tx = simple_transfer();
        let mut bytes = wincode::serialize(&tx).unwrap();
        // Set the number of signatures to u16::MAX
        bytes[0] = 0xff;
        bytes[1] = 0xff;
        bytes[2] = 0xff;
        assert!(TransactionFrame::try_new(&bytes).is_err());
    }

    #[test]
    fn test_account_key_overflow() {
        let tx = simple_transfer();
        let mut bytes = wincode::serialize(&tx).unwrap();
        // Set the number of accounts to u16::MAX
        let offset = 1 + core::mem::size_of::<Signature>() + 3;
        bytes[offset] = 0xff;
        bytes[offset + 1] = 0xff;
        bytes[offset + 2] = 0xff;
        assert!(TransactionFrame::try_new(&bytes).is_err());
    }

    #[test]
    fn test_instructions_overflow() {
        let tx = simple_transfer();
        let mut bytes = wincode::serialize(&tx).unwrap();
        // Set the number of instructions to u16::MAX
        let offset = 1
            + core::mem::size_of::<Signature>()
            + 3
            + 1
            + 3 * core::mem::size_of::<Pubkey>()
            + core::mem::size_of::<Hash>();
        bytes[offset] = 0xff;
        bytes[offset + 1] = 0xff;
        bytes[offset + 2] = 0xff;
        assert!(TransactionFrame::try_new(&bytes).is_err());
    }

    #[test]
    fn test_alt_overflow() {
        let tx = simple_transfer_v0();
        let ix_bytes = tx.message.instructions()[0].data.len();
        let mut bytes = wincode::serialize(&tx).unwrap();
        // Set the number of instructions to u16::MAX
        let offset = 1 // byte for num signatures
            + core::mem::size_of::<Signature>() // signature
            + 1 // version byte
            + 3 // message header
            + 1 // byte for num account keys
            + 3 * core::mem::size_of::<Pubkey>() // account keys
            + core::mem::size_of::<Hash>() // recent blockhash
            + 1 // byte for num instructions
            + 1 // program index
            + 1 // byte for num accounts
            + 2 // bytes for account index
            + 1 // byte for data length
            + ix_bytes;
        bytes[offset] = 0x01;
        assert!(TransactionFrame::try_new(&bytes).is_err());
    }

    #[test]
    fn test_basic_accessors() {
        let tx = simple_transfer();
        let bytes = wincode::serialize(&tx).unwrap();
        let frame = TransactionFrame::try_new(&bytes).unwrap();

        assert_eq!(frame.signature.num_signatures, 1);
        assert!(matches!(
            frame.message.version(),
            TransactionVersion::Legacy
        ));
        assert_eq!(frame.message.num_required_signatures(), 1);
        assert_eq!(frame.message.num_readonly_signed_static_accounts(), 0);
        assert_eq!(frame.message.num_readonly_unsigned_static_accounts(), 1);
        assert_eq!(frame.message.num_static_account_keys(), 3);
        assert_eq!(frame.message.num_instructions(), 1);
        assert_eq!(frame.message.num_address_table_lookups(), 0);

        // SAFETY: `bytes` is the same slice used to create `frame`.
        unsafe {
            let signatures = frame.signature.signatures(&bytes);
            assert_eq!(signatures, &tx.signatures);

            let static_account_keys = frame.message.static_account_keys(&bytes);
            assert_eq!(static_account_keys, tx.message.static_account_keys());

            let recent_blockhash = frame.message.recent_blockhash(&bytes);
            assert_eq!(recent_blockhash, tx.message.recent_blockhash());
        }
    }

    #[test]
    fn test_instructions_iter_empty() {
        let tx = minimally_sized_transaction();
        let bytes = wincode::serialize(&tx).unwrap();
        let frame = TransactionFrame::try_new(&bytes).unwrap();

        // SAFETY: `bytes` is the same slice used to create `frame`.
        unsafe {
            let mut iter = frame.message.instructions_iter(&bytes);
            assert!(iter.next().is_none());
        }
    }

    #[test]
    fn test_instructions_iter_single() {
        let tx = simple_transfer();
        let bytes = wincode::serialize(&tx).unwrap();
        let frame = TransactionFrame::try_new(&bytes).unwrap();

        // SAFETY: `bytes` is the same slice used to create `frame`.
        unsafe {
            let mut iter = frame.message.instructions_iter(&bytes);
            let ix = iter.next().unwrap();
            assert_eq!(ix.program_id_index, 2);
            assert_eq!(ix.accounts, &[0, 1]);
            assert_eq!(
                ix.data,
                &wincode::serialize(&SystemInstruction::Transfer { lamports: 1 }).unwrap()
            );
            assert!(iter.next().is_none());
        }
    }

    #[test]
    fn test_instructions_iter_multiple() {
        let tx = multiple_transfers();
        let bytes = wincode::serialize(&tx).unwrap();
        let frame = TransactionFrame::try_new(&bytes).unwrap();

        // SAFETY: `bytes` is the same slice used to create `frame`.
        unsafe {
            let mut iter = frame.message.instructions_iter(&bytes);
            let ix = iter.next().unwrap();
            assert_eq!(ix.program_id_index, 3);
            assert_eq!(ix.accounts, &[0, 1]);
            assert_eq!(
                ix.data,
                &wincode::serialize(&SystemInstruction::Transfer { lamports: 1 }).unwrap()
            );
            let ix = iter.next().unwrap();
            assert_eq!(ix.program_id_index, 3);
            assert_eq!(ix.accounts, &[0, 2]);
            assert_eq!(
                ix.data,
                &wincode::serialize(&SystemInstruction::Transfer { lamports: 1 }).unwrap()
            );
            assert!(iter.next().is_none());
        }
    }

    #[test]
    fn test_address_table_lookup_iter_empty() {
        let tx = simple_transfer();
        let bytes = wincode::serialize(&tx).unwrap();
        let frame = TransactionFrame::try_new(&bytes).unwrap();

        // SAFETY: `bytes` is the same slice used to create `frame`.
        unsafe {
            let mut iter = frame.message.address_table_lookup_iter(&bytes);
            assert!(iter.next().is_none());
        }
    }

    #[test]
    fn test_address_table_lookup_iter_single() {
        let tx = v0_with_single_lookup();
        let bytes = wincode::serialize(&tx).unwrap();
        let frame = TransactionFrame::try_new(&bytes).unwrap();

        let atls_actual = tx.message.address_table_lookups().unwrap();
        // SAFETY: `bytes` is the same slice used to create `frame`.
        unsafe {
            let mut iter = frame.message.address_table_lookup_iter(&bytes);
            let lookup = iter.next().unwrap();
            assert_eq!(lookup.account_key, &atls_actual[0].account_key);
            assert_eq!(lookup.writable_indexes, atls_actual[0].writable_indexes);
            assert_eq!(lookup.readonly_indexes, atls_actual[0].readonly_indexes);
            assert!(iter.next().is_none());
        }
    }

    #[test]
    fn test_address_table_lookup_iter_multiple() {
        let tx = v0_with_multiple_lookups();
        let bytes = wincode::serialize(&tx).unwrap();
        let frame = TransactionFrame::try_new(&bytes).unwrap();

        let atls_actual = tx.message.address_table_lookups().unwrap();
        // SAFETY: `bytes` is the same slice used to create `frame`.
        unsafe {
            let mut iter = frame.message.address_table_lookup_iter(&bytes);

            let lookup = iter.next().unwrap();
            assert_eq!(lookup.account_key, &atls_actual[0].account_key);
            assert_eq!(lookup.writable_indexes, atls_actual[0].writable_indexes);
            assert_eq!(lookup.readonly_indexes, atls_actual[0].readonly_indexes);

            let lookup = iter.next().unwrap();
            assert_eq!(lookup.account_key, &atls_actual[1].account_key);
            assert_eq!(lookup.writable_indexes, atls_actual[1].writable_indexes);
            assert_eq!(lookup.readonly_indexes, atls_actual[1].readonly_indexes);

            assert!(iter.next().is_none());
        }
    }

    #[test]
    fn test_v1_transaction_frame_parses() {
        let tx = simple_v1_transaction();
        let bytes = wincode::serialize(&tx).unwrap();

        let frame = TransactionFrame::try_new(&bytes).unwrap();

        assert!(matches!(frame.message.version(), TransactionVersion::V1));
        assert_eq!(frame.signature.num_signatures, 1);
        assert_eq!(frame.message.num_required_signatures(), 1);
        assert_eq!(frame.message.num_readonly_signed_static_accounts(), 0);
        assert_eq!(frame.message.num_readonly_unsigned_static_accounts(), 1);
        assert_eq!(frame.message.num_static_account_keys(), 3);
        assert_eq!(frame.message.num_instructions(), 2);

        // txv1 should not have ALTs
        assert_eq!(frame.message.num_address_table_lookups(), 0);
        assert_eq!(frame.message.total_writable_lookup_accounts(), 0);
        assert_eq!(frame.message.total_readonly_lookup_accounts(), 0);

        // new v1-only frame metadata
        assert!(frame.signature.offset > frame.message.message_header.offset);
    }

    #[test]
    fn test_v1_is_not_legacy_or_v0() {
        let tx = simple_v1_transaction();
        let bytes = wincode::serialize(&tx).unwrap();

        assert!(!TransactionFrame::is_legacy_or_v0(&bytes).unwrap());
    }

    #[test]
    fn test_legacy_is_legacy_or_v0() {
        let payer = Pubkey::new_unique();
        let tx = VersionedTransaction {
            signatures: vec![Signature::default()],
            message: VersionedMessage::Legacy(solana_message::Message::new(&[], Some(&payer))),
        };
        let bytes = wincode::serialize(&tx).unwrap();

        assert!(TransactionFrame::is_legacy_or_v0(&bytes).unwrap());
    }

    #[test]
    fn test_is_legacy_or_v0_empty_bytes() {
        assert!(matches!(
            TransactionFrame::is_legacy_or_v0(&[]),
            Err(TransactionViewError::ParseError),
        ));
    }

    #[test]
    fn test_v1_rejects_unknown_version() {
        let tx = simple_v1_transaction();
        let mut bytes = wincode::serialize(&tx).unwrap();

        // First byte is version-tagged for versioned messages.
        // Flip underlying version to an unsupported value.
        bytes[0] = solana_message::MESSAGE_VERSION_PREFIX | 2;

        assert!(matches!(
            TransactionFrame::try_new(&bytes),
            Err(TransactionViewError::ParseError),
        ));
    }

    #[test]
    fn test_v1_rejects_trailing_byte() {
        let tx = simple_v1_transaction();
        let mut bytes = wincode::serialize(&tx).unwrap();
        bytes.push(0);

        assert!(matches!(
            TransactionFrame::try_new(&bytes),
            Err(TransactionViewError::ParseError),
        ));
    }

    #[test]
    fn test_rejects_bytes_with_unrepresentable_frame_offsets() {
        let mut bytes = Vec::new();
        bytes.push(v1::V1_PREFIX);
        bytes.extend_from_slice(&[1, 0, 0]);
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&[0; 32]);
        bytes.push(1);
        bytes.push(1);
        bytes.extend_from_slice(&[1; 32]);
        bytes.extend_from_slice(&[0, 0]);
        bytes.extend_from_slice(&u16::MAX.to_le_bytes());
        bytes.extend(std::iter::repeat_n(0, u16::MAX as usize));
        bytes.extend_from_slice(&[0; 64]);

        assert!(bytes.len() > u16::MAX as usize);
        assert!(matches!(
            TransactionFrame::try_new(&bytes),
            Err(TransactionViewError::ParseError),
        ));
    }

    #[test]
    fn test_v1_rejects_truncated_bytes() {
        let tx = simple_v1_transaction();
        let bytes = wincode::serialize(&tx).unwrap();

        assert!(matches!(
            TransactionFrame::try_new(&bytes[..bytes.len() - 1]),
            Err(TransactionViewError::ParseError),
        ));
    }

    #[test]
    fn test_v1_instruction_iteration() {
        let tx = simple_v1_transaction();
        let bytes = wincode::serialize(&tx).unwrap();
        let frame = TransactionFrame::try_new(&bytes).unwrap();

        let mut iter = unsafe { frame.message.instructions_iter(&bytes) };

        let ix0 = iter.next().unwrap();
        assert_eq!(ix0.program_id_index, 2);
        assert_eq!(ix0.accounts, &[0, 1]);
        assert_eq!(ix0.data, &[10, 11, 12]);

        let ix1 = iter.next().unwrap();
        assert_eq!(ix1.program_id_index, 2);
        assert_eq!(ix1.accounts, &[] as &[u8]);
        assert_eq!(ix1.data, &[99]);

        assert!(iter.next().is_none());
    }
}
