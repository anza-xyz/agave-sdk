use {
    crate::{
        address_table_lookup_frame::AddressTableLookupIterator,
        instructions_frame::InstructionsIterator,
        message_frame::MessageFrame,
        result::Result,
        sanitize::{SanitizeConfig, sanitize_message},
        transaction_config_frame::TransactionConfigView,
        transaction_data::TransactionData,
        transaction_version::TransactionVersion,
    },
    core::fmt::{Debug, Formatter},
    solana_hash::Hash,
    solana_pubkey::Pubkey,
    solana_svm_transaction::{
        instruction::SVMInstruction, message_address_table_lookup::SVMMessageAddressTableLookup,
        svm_message::SVMStaticMessage,
    },
};

// alias for convenience
pub type UnsanitizedMessageView<D> = MessageView<false, D>;
pub type SanitizedMessageView<D> = MessageView<true, D>;

/// A view into a serialized message.
///
/// A message is the signed portion of a transaction, i.e. the transaction
/// without its signatures. This struct provides access to the message data
/// without deserializing it, in the same way
/// [`TransactionView`](crate::transaction_view::TransactionView) does for
/// transactions.
///
/// A `MessageView` is either created from a serialized message, or borrowed
/// from a transaction through
/// [`TransactionView::message`](crate::transaction_view::TransactionView::message).
/// In either case, [`Self::data`] returns only the serialized message.
#[derive(Clone)]
pub struct MessageView<const SANITIZED: bool, D: TransactionData> {
    /// The parsed buffer. For a view borrowed from a transaction, this is the
    /// transaction's buffer, which may also hold signatures and trailing bytes.
    pub(crate) data: D,
    pub(crate) frame: MessageFrame,
}

impl<D: TransactionData> MessageView<false, D> {
    /// Creates a new `MessageView` without running sanitization checks.
    /// The `data` must contain a serialized message with no trailing bytes.
    pub fn try_new_unsanitized(data: D) -> Result<Self> {
        let frame = MessageFrame::try_new(data.data())?;
        Ok(Self { data, frame })
    }

    /// Sanitizes the message view, returning a sanitized view on success.
    ///
    /// A message is sanitized if the transaction formed by signing it would
    /// pass [`TransactionView::sanitize`](crate::transaction_view::TransactionView::sanitize).
    pub fn sanitize(self, config: &SanitizeConfig) -> Result<SanitizedMessageView<D>> {
        sanitize_message(&self, config)?;
        Ok(self.into_sanitized())
    }

    /// Marks the view as sanitized. The caller must have run the
    /// sanitization checks.
    #[inline]
    pub(crate) fn into_sanitized(self) -> SanitizedMessageView<D> {
        SanitizedMessageView {
            data: self.data,
            frame: self.frame,
        }
    }
}

impl<D: TransactionData> MessageView<true, D> {
    /// Creates a new `MessageView`, running sanitization checks.
    pub fn try_new_sanitized(data: D, config: &SanitizeConfig) -> Result<Self> {
        let unsanitized_view = MessageView::try_new_unsanitized(data)?;
        unsanitized_view.sanitize(config)
    }
}

impl<const SANITIZED: bool, D: TransactionData> MessageView<SANITIZED, D> {
    /// Return the version of the message.
    #[inline]
    pub fn version(&self) -> TransactionVersion {
        self.frame.version()
    }

    /// Return the number of required signatures in the message.
    #[inline]
    pub fn num_required_signatures(&self) -> u8 {
        self.frame.num_required_signatures()
    }

    /// Return the number of readonly signed static accounts in the message.
    #[inline]
    pub fn num_readonly_signed_static_accounts(&self) -> u8 {
        self.frame.num_readonly_signed_static_accounts()
    }

    /// Return the number of readonly unsigned static accounts in the message.
    #[inline]
    pub fn num_readonly_unsigned_static_accounts(&self) -> u8 {
        self.frame.num_readonly_unsigned_static_accounts()
    }

    /// Return the number of static account keys in the message.
    #[inline]
    pub fn num_static_account_keys(&self) -> u8 {
        self.frame.num_static_account_keys()
    }

    /// Return the number of instructions in the message.
    #[inline]
    pub fn num_instructions(&self) -> u16 {
        self.frame.num_instructions()
    }

    /// Return the number of address table lookups in the message.
    #[inline]
    pub fn num_address_table_lookups(&self) -> u8 {
        self.frame.num_address_table_lookups()
    }

    /// Return the number of writable lookup accounts in the message.
    #[inline]
    pub fn total_writable_lookup_accounts(&self) -> u16 {
        self.frame.total_writable_lookup_accounts()
    }

    /// Return the number of readonly lookup accounts in the message.
    #[inline]
    pub fn total_readonly_lookup_accounts(&self) -> u16 {
        self.frame.total_readonly_lookup_accounts()
    }

    /// Return the slice of static account keys in the message.
    #[inline]
    pub fn static_account_keys(&self) -> &[Pubkey] {
        let data = self.data.data();
        // SAFETY: `frame` was created from `data`.
        unsafe { self.frame.static_account_keys(data) }
    }

    /// Return the recent blockhash in the message.
    #[inline]
    pub fn recent_blockhash(&self) -> &Hash {
        let data = self.data.data();
        // SAFETY: `frame` was created from `data`.
        unsafe { self.frame.recent_blockhash(data) }
    }

    /// Return an iterator over the instructions in the message.
    #[inline]
    pub fn instructions_iter(&self) -> InstructionsIterator<'_> {
        let data = self.data.data();
        // SAFETY: `frame` was created from `data`.
        unsafe { self.frame.instructions_iter(data) }
    }

    /// Return an iterator over the address table lookups in the message.
    #[inline]
    pub fn address_table_lookup_iter(&self) -> AddressTableLookupIterator<'_> {
        let data = self.data.data();
        // SAFETY: `frame` was created from `data`.
        unsafe { self.frame.address_table_lookup_iter(data) }
    }

    /// Return Some(TransactionConfigView) for V1, None for legacy/V0
    #[inline]
    pub fn transaction_config(&self) -> Option<TransactionConfigView<'_>> {
        let transaction_config_frame = self.frame.transaction_config_frame();
        transaction_config_frame
            .is_present()
            .then_some(TransactionConfigView {
                transaction_config_frame,
                bytes: self.data.data(),
            })
    }

    /// Return the serialized message data.
    /// For a view borrowed from a transaction, this does not include the
    /// signatures.
    #[inline]
    pub fn data(&self) -> &[u8] {
        let (start, end) = self.frame.message_range();
        &self.data.data()[usize::from(start)..usize::from(end)]
    }
}

// Implementation that relies on sanitization checks having been run.
impl<D: TransactionData> MessageView<true, D> {
    /// Return an iterator over the instructions paired with their program ids.
    pub fn program_instructions_iter(
        &self,
    ) -> impl Iterator<Item = (&Pubkey, SVMInstruction<'_>)> + Clone {
        self.instructions_iter().map(|ix| {
            let program_id_index = usize::from(ix.program_id_index);
            let program_id = &self.static_account_keys()[program_id_index];
            (program_id, ix)
        })
    }

    /// Return the number of unsigned static account keys.
    #[inline]
    pub(crate) fn num_static_unsigned_static_accounts(&self) -> u8 {
        self.num_static_account_keys()
            .wrapping_sub(self.num_required_signatures())
    }

    /// Return the number of writable unsigned static accounts.
    #[inline]
    pub(crate) fn num_writable_unsigned_static_accounts(&self) -> u8 {
        self.num_static_unsigned_static_accounts()
            .wrapping_sub(self.num_readonly_unsigned_static_accounts())
    }

    /// Return the number of writable signed static accounts.
    #[inline]
    pub(crate) fn num_writable_signed_static_accounts(&self) -> u8 {
        self.num_required_signatures()
            .wrapping_sub(self.num_readonly_signed_static_accounts())
    }

    /// Return the total number of accounts in the message.
    #[inline]
    pub fn total_num_accounts(&self) -> u16 {
        u16::from(self.num_static_account_keys())
            .wrapping_add(self.total_writable_lookup_accounts())
            .wrapping_add(self.total_readonly_lookup_accounts())
    }

    /// Return the number of requested writable keys.
    #[inline]
    pub fn num_requested_write_locks(&self) -> u64 {
        u64::from(
            u16::from(
                (self.num_static_account_keys())
                    .wrapping_sub(self.num_readonly_signed_static_accounts())
                    .wrapping_sub(self.num_readonly_unsigned_static_accounts()),
            )
            .wrapping_add(self.total_writable_lookup_accounts()),
        )
    }
}

// Manual implementation of `Debug` - avoids bound on `D`.
// Prints nicely formatted struct-ish fields even for the iterator fields.
impl<const SANITIZED: bool, D: TransactionData> Debug for MessageView<SANITIZED, D> {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("MessageView")
            .field("frame", &self.frame)
            .field("static_account_keys", &self.static_account_keys())
            .field("recent_blockhash", &self.recent_blockhash())
            .field("instructions", &self.instructions_iter())
            .field("address_table_lookups", &self.address_table_lookup_iter())
            .finish()
    }
}

impl<D: TransactionData> SVMStaticMessage for MessageView<true, D> {
    fn version(&self) -> solana_transaction::versioned::TransactionVersion {
        self.version().into()
    }

    fn num_transaction_signatures(&self) -> u64 {
        self.num_required_signatures() as u64
    }

    fn num_write_locks(&self) -> u64 {
        self.num_requested_write_locks()
    }

    fn num_readonly_signed_static_accounts(&self) -> u8 {
        self.num_readonly_signed_static_accounts()
    }

    fn num_readonly_unsigned_static_accounts(&self) -> u8 {
        self.num_readonly_unsigned_static_accounts()
    }

    fn recent_blockhash(&self) -> &Hash {
        self.recent_blockhash()
    }

    fn num_instructions(&self) -> usize {
        self.num_instructions() as usize
    }

    fn instructions_iter(&self) -> impl Iterator<Item = SVMInstruction<'_>> {
        self.instructions_iter()
    }

    fn program_instructions_iter(
        &self,
    ) -> impl Iterator<Item = (&Pubkey, SVMInstruction<'_>)> + Clone {
        self.program_instructions_iter()
    }

    fn static_account_keys(&self) -> &[Pubkey] {
        self.static_account_keys()
    }

    fn fee_payer(&self) -> &Pubkey {
        &self.static_account_keys()[0]
    }

    fn num_lookup_tables(&self) -> usize {
        self.num_address_table_lookups() as usize
    }

    fn message_address_table_lookups(
        &self,
    ) -> impl Iterator<Item = SVMMessageAddressTableLookup<'_>> {
        self.address_table_lookup_iter()
    }

    fn is_signer(&self, index: usize) -> bool {
        index < usize::from(self.num_required_signatures())
    }

    fn is_invoked(&self, key_index: usize) -> bool {
        let Ok(index) = u8::try_from(key_index) else {
            return false;
        };
        self.instructions_iter()
            .any(|ix| ix.program_id_index == index)
    }
}

impl<D: TransactionData> SVMStaticMessage for &MessageView<true, D> {
    fn version(&self) -> solana_transaction::versioned::TransactionVersion {
        <MessageView<true, D> as SVMStaticMessage>::version(self)
    }

    fn num_transaction_signatures(&self) -> u64 {
        <MessageView<true, D> as SVMStaticMessage>::num_transaction_signatures(self)
    }

    fn num_write_locks(&self) -> u64 {
        <MessageView<true, D> as SVMStaticMessage>::num_write_locks(self)
    }

    fn num_readonly_signed_static_accounts(&self) -> u8 {
        <MessageView<true, D> as SVMStaticMessage>::num_readonly_signed_static_accounts(self)
    }

    fn num_readonly_unsigned_static_accounts(&self) -> u8 {
        <MessageView<true, D> as SVMStaticMessage>::num_readonly_unsigned_static_accounts(self)
    }

    fn recent_blockhash(&self) -> &Hash {
        <MessageView<true, D> as SVMStaticMessage>::recent_blockhash(self)
    }

    fn num_instructions(&self) -> usize {
        <MessageView<true, D> as SVMStaticMessage>::num_instructions(self)
    }

    fn instructions_iter(&self) -> impl Iterator<Item = SVMInstruction<'_>> {
        <MessageView<true, D> as SVMStaticMessage>::instructions_iter(self)
    }

    fn program_instructions_iter(
        &self,
    ) -> impl Iterator<Item = (&Pubkey, SVMInstruction<'_>)> + Clone {
        <MessageView<true, D> as SVMStaticMessage>::program_instructions_iter(self)
    }

    fn static_account_keys(&self) -> &[Pubkey] {
        <MessageView<true, D> as SVMStaticMessage>::static_account_keys(self)
    }

    fn fee_payer(&self) -> &Pubkey {
        <MessageView<true, D> as SVMStaticMessage>::fee_payer(self)
    }

    fn num_lookup_tables(&self) -> usize {
        <MessageView<true, D> as SVMStaticMessage>::num_lookup_tables(self)
    }

    fn message_address_table_lookups(
        &self,
    ) -> impl Iterator<Item = SVMMessageAddressTableLookup<'_>> {
        <MessageView<true, D> as SVMStaticMessage>::message_address_table_lookups(self)
    }

    fn is_signer(&self, index: usize) -> bool {
        <MessageView<true, D> as SVMStaticMessage>::is_signer(self, index)
    }

    fn is_invoked(&self, key_index: usize) -> bool {
        <MessageView<true, D> as SVMStaticMessage>::is_invoked(self, key_index)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{result::TransactionViewError, transaction_view::TransactionView},
        solana_message::{
            AddressLookupTableAccount, Message, MessageHeader, VersionedMessage,
            compiled_instruction::CompiledInstruction, v0, v1,
        },
        solana_signature::Signature,
        solana_system_interface::instruction as system_instruction,
        solana_transaction::versioned::VersionedTransaction,
    };

    // Current protocol values; production callers supply these from agave.
    fn test_sanitize_config() -> SanitizeConfig {
        SanitizeConfig {
            min_requested_heap_size: 32 * 1024,
            max_requested_heap_size: 256 * 1024,
            max_instructions: 64,
            max_accounts_per_instruction: 255,
        }
    }

    /// Sign `message` with a placeholder signature per required signer.
    fn sign(message: VersionedMessage) -> VersionedTransaction {
        VersionedTransaction {
            signatures: vec![
                Signature::default();
                usize::from(message.header().num_required_signatures)
            ],
            message,
        }
    }

    fn verify_message_view(message: VersionedMessage) {
        let message_bytes = wincode::serialize(&message).unwrap();
        let view =
            MessageView::try_new_sanitized(message_bytes.as_slice(), &test_sanitize_config())
                .unwrap();

        assert_eq!(view.data(), message_bytes);
        assert_eq!(
            view.num_required_signatures(),
            message.header().num_required_signatures
        );
        assert_eq!(
            view.num_readonly_signed_static_accounts(),
            message.header().num_readonly_signed_accounts
        );
        assert_eq!(
            view.num_readonly_unsigned_static_accounts(),
            message.header().num_readonly_unsigned_accounts
        );
        assert_eq!(view.static_account_keys(), message.static_account_keys());
        assert_eq!(view.recent_blockhash(), message.recent_blockhash());
        assert!(
            view.instructions_iter()
                .eq(message.instructions().iter().map(SVMInstruction::from))
        );
        assert!(
            view.address_table_lookup_iter().eq(message
                .address_table_lookups()
                .unwrap_or_default()
                .iter()
                .map(SVMMessageAddressTableLookup::from))
        );
        assert_eq!(
            view.transaction_config().is_some(),
            matches!(message, VersionedMessage::V1(_))
        );

        // The message embedded in the signed transaction is the same message.
        let transaction = sign(message);
        assert_eq!(SVMStaticMessage::version(&view), transaction.version());
        let transaction_bytes = wincode::serialize(&transaction).unwrap();
        let transaction_view =
            TransactionView::try_new_unsanitized(transaction_bytes.as_slice()).unwrap();
        assert_eq!(transaction_view.message().data(), message_bytes);
    }

    #[test]
    fn test_legacy_message() {
        let payer = Pubkey::new_unique();
        verify_message_view(VersionedMessage::Legacy(Message::new(
            &[
                system_instruction::transfer(&payer, &Pubkey::new_unique(), 1),
                system_instruction::transfer(&payer, &Pubkey::new_unique(), 1),
            ],
            Some(&payer),
        )));
    }

    #[test]
    fn test_v0_message() {
        let payer = Pubkey::new_unique();
        let to = Pubkey::new_unique();
        verify_message_view(VersionedMessage::V0(
            v0::Message::try_compile(
                &payer,
                &[system_instruction::transfer(&payer, &to, 1)],
                &[AddressLookupTableAccount {
                    key: Pubkey::new_unique(),
                    addresses: vec![to],
                }],
                Hash::new_from_array([7; 32]),
            )
            .unwrap(),
        ));
    }

    #[test]
    fn test_v1_message() {
        verify_message_view(VersionedMessage::V1(v1::Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            config: v1::TransactionConfig {
                priority_fee: Some(111),
                compute_unit_limit: Some(222),
                loaded_accounts_data_size_limit: Some(333),
                heap_size: Some(32 * 1024),
            },
            lifetime_specifier: Hash::new_from_array([7; 32]),
            account_keys: vec![Pubkey::new_unique(), Pubkey::new_unique()],
            instructions: vec![CompiledInstruction {
                program_id_index: 1,
                accounts: vec![0],
                data: vec![1, 2, 3, 4],
            }],
        }));
    }

    #[test]
    fn test_try_new_unsanitized_rejects_invalid_bytes() {
        let mut trailing_bytes = wincode::serialize(&VersionedMessage::Legacy(Message::new(
            &[],
            Some(&Pubkey::new_unique()),
        )))
        .unwrap();
        trailing_bytes.push(0);
        let unknown_version = [solana_message::MESSAGE_VERSION_PREFIX | 2, 1, 0, 0];

        for bytes in [&[][..], &unknown_version[..], &trailing_bytes[..]] {
            assert_eq!(
                MessageView::try_new_unsanitized(bytes).unwrap_err(),
                TransactionViewError::ParseError
            );
        }
    }

    #[test]
    fn test_sanitize_matches_signed_transaction() {
        fn message(
            v1: bool,
            num_required_signatures: u8,
            num_account_keys: u8,
            data_len: usize,
        ) -> VersionedMessage {
            let header = MessageHeader {
                num_required_signatures,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            };
            let account_keys = (0..num_account_keys)
                .map(|_| Pubkey::new_unique())
                .collect();
            let instructions = vec![CompiledInstruction {
                program_id_index: num_account_keys - 1,
                accounts: vec![0],
                data: vec![0; data_len],
            }];
            if v1 {
                VersionedMessage::V1(v1::Message {
                    header,
                    config: v1::TransactionConfig::empty(),
                    lifetime_specifier: Hash::default(),
                    account_keys,
                    instructions,
                })
            } else {
                VersionedMessage::Legacy(Message {
                    header,
                    account_keys,
                    recent_blockhash: Hash::default(),
                    instructions,
                })
            }
        }

        let mut cases = vec![
            // no fee payer
            (message(false, 0, 2, 0), false),
            // signer without a static account key
            (message(false, 3, 2, 0), false),
            // too many signers
            (message(true, 13, 14, 0), false),
        ];
        for (v1, max_transaction_size) in [
            (false, solana_packet::PACKET_DATA_SIZE),
            (true, v1::MAX_TRANSACTION_SIZE),
        ] {
            // The length of the instruction data prefix is the same for all
            // data lengths from 128 up to the maximum transaction size.
            let signed_len = |data_len| {
                wincode::serialize(&sign(message(v1, 1, 2, data_len)))
                    .unwrap()
                    .len()
            };
            let max_data_len = max_transaction_size - signed_len(128) + 128;
            cases.push((message(v1, 1, 2, max_data_len), true));
            cases.push((message(v1, 1, 2, max_data_len + 1), false));
        }

        for (message, expect_ok) in cases {
            let message_bytes = wincode::serialize(&message).unwrap();
            let transaction_bytes = wincode::serialize(&sign(message)).unwrap();
            assert_eq!(
                MessageView::try_new_sanitized(message_bytes.as_slice(), &test_sanitize_config())
                    .is_ok(),
                expect_ok
            );
            assert_eq!(
                TransactionView::try_new_sanitized(
                    transaction_bytes.as_slice(),
                    &test_sanitize_config()
                )
                .is_ok(),
                expect_ok
            );
        }
    }
}
