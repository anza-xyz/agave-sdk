use {
    crate::{
        address_table_lookup_frame::AddressTableLookupIterator,
        instructions_frame::InstructionsIterator,
        result::Result,
        sanitize::{SanitizeConfig, sanitize},
        transaction_config_frame::TransactionConfigView,
        transaction_data::TransactionData,
        transaction_frame::TransactionFrame,
        transaction_version::TransactionVersion,
    },
    core::fmt::{Debug, Formatter},
    solana_hash::Hash,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    solana_svm_transaction::{
        instruction::SVMInstruction, message_address_table_lookup::SVMMessageAddressTableLookup,
        svm_message::SVMStaticMessage,
    },
};

// alias for convenience
pub type UnsanitizedTransactionView<D> = TransactionView<false, D>;
pub type SanitizedTransactionView<D> = TransactionView<true, D>;

/// A view into a serialized transaction.
///
/// This struct provides access to the transaction data without
/// deserializing it. This is done by parsing and caching metadata
/// about the layout of the serialized transaction.
/// The owned `data` is abstracted through the `TransactionData` trait,
/// so that different containers for the serialized transaction can be used.
pub struct TransactionView<const SANITIZED: bool, D: TransactionData> {
    data: D,
    frame: TransactionFrame,
}

impl<D: TransactionData> TransactionView<false, D> {
    /// Creates a new `TransactionView` without running sanitization checks.
    pub fn try_new_unsanitized(data: D) -> Result<Self> {
        let frame = TransactionFrame::try_new(data.data())?;
        Ok(Self { data, frame })
    }

    /// Creates a new `TransactionView` without running sanitization checks,
    /// parsing a single transaction from the front of `data`.
    ///
    /// Unlike [`Self::try_new_unsanitized`], `data` may contain trailing
    /// bytes after the serialized transaction. Returns the view and the
    /// number of bytes the transaction occupies, i.e. the offset at which
    /// the next item in the buffer begins. The view only exposes the
    /// serialized transaction: trailing bytes are not part of
    /// [`Self::data`].
    pub fn try_new_unsanitized_from_prefix(data: D) -> Result<(Self, usize)> {
        let frame = TransactionFrame::try_new_from_prefix(data.data())?;
        let consumed_len = usize::from(frame.data_len());
        Ok((Self { data, frame }, consumed_len))
    }

    /// Sanitizes the transaction view, returning a sanitized view on success.
    pub fn sanitize(self, config: &SanitizeConfig) -> Result<SanitizedTransactionView<D>> {
        sanitize(&self, config)?;
        Ok(SanitizedTransactionView {
            data: self.data,
            frame: self.frame,
        })
    }
}

impl<D: TransactionData> TransactionView<true, D> {
    /// Creates a new `TransactionView`, running sanitization checks.
    pub fn try_new_sanitized(data: D, config: &SanitizeConfig) -> Result<Self> {
        let unsanitized_view = TransactionView::try_new_unsanitized(data)?;
        unsanitized_view.sanitize(config)
    }

    /// Creates a new `TransactionView`, running sanitization checks,
    /// parsing a single transaction from the front of `data`.
    ///
    /// Unlike [`Self::try_new_sanitized`], `data` may contain trailing bytes
    /// after the serialized transaction. See
    /// [`TransactionView::try_new_unsanitized_from_prefix`].
    pub fn try_new_sanitized_from_prefix(
        data: D,
        config: &SanitizeConfig,
    ) -> Result<(Self, usize)> {
        let (unsanitized_view, consumed_len) =
            TransactionView::try_new_unsanitized_from_prefix(data)?;
        Ok((unsanitized_view.sanitize(config)?, consumed_len))
    }
}

impl<const SANITIZED: bool, D: TransactionData> TransactionView<SANITIZED, D> {
    /// Return the number of signatures in the transaction.
    #[inline]
    pub fn num_signatures(&self) -> u8 {
        self.frame.num_signatures()
    }

    /// Return the version of the transaction.
    #[inline]
    pub fn version(&self) -> TransactionVersion {
        self.frame.version()
    }

    /// Return the number of required signatures in the transaction.
    #[inline]
    pub fn num_required_signatures(&self) -> u8 {
        self.frame.num_required_signatures()
    }

    /// Return the number of readonly signed static accounts in the transaction.
    #[inline]
    pub fn num_readonly_signed_static_accounts(&self) -> u8 {
        self.frame.num_readonly_signed_static_accounts()
    }

    /// Return the number of readonly unsigned static accounts in the transaction.
    #[inline]
    pub fn num_readonly_unsigned_static_accounts(&self) -> u8 {
        self.frame.num_readonly_unsigned_static_accounts()
    }

    /// Return the number of static account keys in the transaction.
    #[inline]
    pub fn num_static_account_keys(&self) -> u8 {
        self.frame.num_static_account_keys()
    }

    /// Return the number of instructions in the transaction.
    #[inline]
    pub fn num_instructions(&self) -> u16 {
        self.frame.num_instructions()
    }

    /// Return the number of address table lookups in the transaction.
    #[inline]
    pub fn num_address_table_lookups(&self) -> u8 {
        self.frame.num_address_table_lookups()
    }

    /// Return the number of writable lookup accounts in the transaction.
    #[inline]
    pub fn total_writable_lookup_accounts(&self) -> u16 {
        self.frame.total_writable_lookup_accounts()
    }

    /// Return the number of readonly lookup accounts in the transaction.
    #[inline]
    pub fn total_readonly_lookup_accounts(&self) -> u16 {
        self.frame.total_readonly_lookup_accounts()
    }

    /// Return the slice of signatures in the transaction.
    #[inline]
    pub fn signatures(&self) -> &[Signature] {
        let data = self.data();
        // SAFETY: `frame` was created from `data`.
        unsafe { self.frame.signatures(data) }
    }

    /// Return the slice of static account keys in the transaction.
    #[inline]
    pub fn static_account_keys(&self) -> &[Pubkey] {
        let data = self.data();
        // SAFETY: `frame` was created from `data`.
        unsafe { self.frame.static_account_keys(data) }
    }

    /// Return the recent blockhash in the transaction.
    #[inline]
    pub fn recent_blockhash(&self) -> &Hash {
        let data = self.data();
        // SAFETY: `frame` was created from `data`.
        unsafe { self.frame.recent_blockhash(data) }
    }

    /// Return an iterator over the instructions in the transaction.
    #[inline]
    pub fn instructions_iter(&self) -> InstructionsIterator<'_> {
        let data = self.data();
        // SAFETY: `frame` was created from `data`.
        unsafe { self.frame.instructions_iter(data) }
    }

    /// Return an iterator over the address table lookups in the transaction.
    #[inline]
    pub fn address_table_lookup_iter(&self) -> AddressTableLookupIterator<'_> {
        let data = self.data();
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
                bytes: self.data(),
            })
    }

    /// Return the full serialized transaction data.
    /// If the view was created with trailing bytes allowed, the returned
    /// slice ends where the serialized transaction ends and does not include
    /// the trailing bytes; the underlying buffer remains available through
    /// [`Self::inner_data`].
    #[inline]
    pub fn data(&self) -> &[u8] {
        let data_length: usize = self.frame.data_len().into();
        &self.data.data()[..data_length]
    }

    /// Return the serialized **message** data.
    /// This does not include the signatures.
    #[inline]
    pub fn message_data(&self) -> &[u8] {
        let (start, end) = self.frame.message_range();
        &self.data()[usize::from(start)..usize::from(end)]
    }

    #[inline]
    pub fn inner_data(&self) -> &D {
        &self.data
    }

    #[inline]
    pub fn into_inner_data(self) -> D {
        self.data
    }
}

// Implementation that relies on sanitization checks having been run.
impl<D: TransactionData> TransactionView<true, D> {
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

    /// Return the number of writable unsigned static accounts.
    #[inline]
    pub(crate) fn num_writable_signed_static_accounts(&self) -> u8 {
        self.num_required_signatures()
            .wrapping_sub(self.num_readonly_signed_static_accounts())
    }

    /// Return the total number of accounts in the transactions.
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
impl<const SANITIZED: bool, D: TransactionData> Debug for TransactionView<SANITIZED, D> {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("TransactionView")
            .field("frame", &self.frame)
            .field("signatures", &self.signatures())
            .field("static_account_keys", &self.static_account_keys())
            .field("recent_blockhash", &self.recent_blockhash())
            .field("instructions", &self.instructions_iter())
            .field("address_table_lookups", &self.address_table_lookup_iter())
            .finish()
    }
}

impl<D: TransactionData> SVMStaticMessage for TransactionView<true, D> {
    fn version(&self) -> solana_transaction::versioned::TransactionVersion {
        self.version().into()
    }

    fn num_transaction_signatures(&self) -> u64 {
        self.num_required_signatures() as u64
    }

    fn num_write_locks(&self) -> u64 {
        self.num_requested_write_locks()
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
}

impl<D: TransactionData> SVMStaticMessage for &TransactionView<true, D> {
    fn version(&self) -> solana_transaction::versioned::TransactionVersion {
        <TransactionView<true, D> as SVMStaticMessage>::version(self)
    }

    fn num_transaction_signatures(&self) -> u64 {
        <TransactionView<true, D> as SVMStaticMessage>::num_transaction_signatures(self)
    }

    fn num_write_locks(&self) -> u64 {
        <TransactionView<true, D> as SVMStaticMessage>::num_write_locks(self)
    }

    fn recent_blockhash(&self) -> &Hash {
        <TransactionView<true, D> as SVMStaticMessage>::recent_blockhash(self)
    }

    fn num_instructions(&self) -> usize {
        <TransactionView<true, D> as SVMStaticMessage>::num_instructions(self)
    }

    fn instructions_iter(&self) -> impl Iterator<Item = SVMInstruction<'_>> {
        <TransactionView<true, D> as SVMStaticMessage>::instructions_iter(self)
    }

    fn program_instructions_iter(
        &self,
    ) -> impl Iterator<Item = (&Pubkey, SVMInstruction<'_>)> + Clone {
        <TransactionView<true, D> as SVMStaticMessage>::program_instructions_iter(self)
    }

    fn static_account_keys(&self) -> &[Pubkey] {
        <TransactionView<true, D> as SVMStaticMessage>::static_account_keys(self)
    }

    fn fee_payer(&self) -> &Pubkey {
        <TransactionView<true, D> as SVMStaticMessage>::fee_payer(self)
    }

    fn num_lookup_tables(&self) -> usize {
        <TransactionView<true, D> as SVMStaticMessage>::num_lookup_tables(self)
    }

    fn message_address_table_lookups(
        &self,
    ) -> impl Iterator<Item = SVMMessageAddressTableLookup<'_>> {
        <TransactionView<true, D> as SVMStaticMessage>::message_address_table_lookups(self)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_message::{
            Message, MessageHeader, VersionedMessage, compiled_instruction::CompiledInstruction, v1,
        },
        solana_pubkey::Pubkey,
        solana_signature::Signature,
        solana_system_interface::instruction as system_instruction,
        solana_transaction::versioned::VersionedTransaction,
    };

    fn verify_transaction_view_frame(tx: &VersionedTransaction) {
        let bytes = wincode::serialize(tx).unwrap();
        let view = TransactionView::try_new_unsanitized(bytes.as_ref()).unwrap();

        assert_eq!(view.num_signatures(), tx.signatures.len() as u8);

        assert_eq!(
            view.num_required_signatures(),
            tx.message.header().num_required_signatures
        );
        assert_eq!(
            view.num_readonly_signed_static_accounts(),
            tx.message.header().num_readonly_signed_accounts
        );
        assert_eq!(
            view.num_readonly_unsigned_static_accounts(),
            tx.message.header().num_readonly_unsigned_accounts
        );

        assert_eq!(
            view.num_static_account_keys(),
            tx.message.static_account_keys().len() as u8
        );
        assert_eq!(
            view.num_instructions(),
            tx.message.instructions().len() as u16
        );
        assert_eq!(
            view.num_address_table_lookups(),
            tx.message
                .address_table_lookups()
                .map(|x| x.len() as u8)
                .unwrap_or(0)
        );

        assert!(view.transaction_config().is_none());
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

    #[test]
    fn test_multiple_transfers() {
        verify_transaction_view_frame(&multiple_transfers());
    }

    fn simple_v1_transaction() -> VersionedTransaction {
        let payer = Pubkey::new_unique();
        let program = Pubkey::new_unique();

        VersionedTransaction {
            signatures: vec![Signature::default()],
            message: VersionedMessage::V1(v1::Message {
                header: MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                config: v1::TransactionConfig {
                    priority_fee: Some(111),
                    compute_unit_limit: Some(222),
                    loaded_accounts_data_size_limit: Some(333),
                    heap_size: Some(1024),
                },
                lifetime_specifier: Hash::default(),
                account_keys: vec![payer, program],
                instructions: vec![CompiledInstruction {
                    program_id_index: 1,
                    accounts: vec![0],
                    data: vec![1, 2, 3, 4],
                }],
            }),
        }
    }

    #[test]
    fn test_v1_transaction_config_present() {
        let tx = simple_v1_transaction();
        let bytes = wincode::serialize(&tx).unwrap();
        let view = TransactionView::try_new_unsanitized(bytes.as_ref()).unwrap();

        assert!(matches!(view.version(), TransactionVersion::V1));

        let config = view.transaction_config().expect("v1 should have config");
        assert_eq!(config.priority_fee_lamports().unwrap(), 111);
        assert_eq!(config.compute_unit_limit().unwrap(), 222);
        assert_eq!(config.loaded_accounts_data_size_limit().unwrap(), 333);
        assert_eq!(config.requested_heap_size().unwrap(), 1024);
    }

    #[test]
    fn test_v1_message_data_excludes_signatures() {
        let tx = simple_v1_transaction();
        let bytes = wincode::serialize(&tx).unwrap();
        let view = TransactionView::try_new_unsanitized(bytes.as_ref()).unwrap();

        let message_data = view.message_data();

        // For v1, message_data should stop before the signatures region.
        assert!(message_data.len() < bytes.len());

        let full_message = &bytes
            [usize::from(view.frame.message_offset())..usize::from(view.frame.signatures_offset())];
        assert_eq!(message_data, full_message);
    }

    #[test]
    fn test_v1_signatures_accessible() {
        let tx = simple_v1_transaction();
        let bytes = wincode::serialize(&tx).unwrap();
        let view = TransactionView::try_new_unsanitized(bytes.as_ref()).unwrap();

        assert_eq!(view.signatures().len(), 1);
        assert_eq!(view.static_account_keys().len(), 2);

        let instructions: Vec<_> = view.instructions_iter().collect();
        assert_eq!(instructions.len(), 1);
        assert_eq!(instructions[0].program_id_index, 1);
        assert_eq!(instructions[0].accounts, &[0]);
        assert_eq!(instructions[0].data, &[1, 2, 3, 4]);
    }

    // Current protocol values; production callers supply these from agave.
    fn test_sanitize_config() -> SanitizeConfig {
        SanitizeConfig {
            min_requested_heap_size: 32 * 1024,
            max_requested_heap_size: 256 * 1024,
            max_instructions: 64,
            max_accounts_per_instruction: 255,
        }
    }

    fn append_trailing_bytes(transaction_bytes: &[u8]) -> Vec<u8> {
        let mut bytes_with_trailing = transaction_bytes.to_vec();
        bytes_with_trailing.extend_from_slice(&[0xAA; 7]);
        bytes_with_trailing
    }

    #[test]
    fn test_try_new_unsanitized_rejects_trailing_bytes() {
        // given a serialized transaction followed by trailing bytes
        let bytes_with_trailing =
            append_trailing_bytes(&wincode::serialize(&multiple_transfers()).unwrap());

        // when parsing with the strict constructor
        let result = TransactionView::try_new_unsanitized(bytes_with_trailing.as_slice());

        // then parsing fails
        assert!(result.is_err());
    }

    #[test]
    fn test_from_prefix_ignores_trailing_bytes_legacy() {
        // given a serialized legacy transaction followed by trailing bytes
        let transaction_bytes = wincode::serialize(&multiple_transfers()).unwrap();
        let bytes_with_trailing = append_trailing_bytes(&transaction_bytes);

        // when parsing from the prefix of the buffer
        let (view, consumed_len) =
            TransactionView::try_new_unsanitized_from_prefix(bytes_with_trailing.as_slice())
                .unwrap();

        // then the view exposes exactly the transaction, without the trailing bytes
        assert_eq!(consumed_len, transaction_bytes.len());
        assert_eq!(view.data(), transaction_bytes.as_slice());
        assert!(matches!(view.version(), TransactionVersion::Legacy));
        assert_eq!(view.num_instructions(), 2);
    }

    #[test]
    fn test_from_prefix_ignores_trailing_bytes_v1() {
        // given a serialized v1 transaction followed by trailing bytes
        let transaction_bytes = wincode::serialize(&simple_v1_transaction()).unwrap();
        let bytes_with_trailing = append_trailing_bytes(&transaction_bytes);

        // when parsing from the prefix of the buffer
        let (view, consumed_len) =
            TransactionView::try_new_unsanitized_from_prefix(bytes_with_trailing.as_slice())
                .unwrap();

        // then the view exposes exactly the transaction, without the trailing bytes
        assert_eq!(consumed_len, transaction_bytes.len());
        assert_eq!(view.data(), transaction_bytes.as_slice());
        assert!(matches!(view.version(), TransactionVersion::V1));
        assert_eq!(view.signatures().len(), 1);
    }

    #[test]
    fn test_from_prefix_still_rejects_truncated_transaction() {
        // given a serialized transaction with its last byte removed
        let transaction_bytes = wincode::serialize(&multiple_transfers()).unwrap();
        let truncated_bytes = &transaction_bytes[..transaction_bytes.len() - 1];

        // when parsing from the prefix of the buffer
        let result = TransactionView::try_new_unsanitized_from_prefix(truncated_bytes);

        // then parsing fails
        assert!(result.is_err());
    }

    #[test]
    fn test_sanitized_from_prefix() {
        // given a serialized transaction followed by trailing bytes
        let transaction_bytes = wincode::serialize(&multiple_transfers()).unwrap();
        let bytes_with_trailing = append_trailing_bytes(&transaction_bytes);

        // when parsing from the prefix of the buffer and sanitizing
        let (view, consumed_len) = TransactionView::try_new_sanitized_from_prefix(
            bytes_with_trailing.as_slice(),
            &test_sanitize_config(),
        )
        .unwrap();

        // then sanitization passes and the view excludes the trailing bytes
        assert_eq!(consumed_len, transaction_bytes.len());
        assert_eq!(view.data(), transaction_bytes.as_slice());
    }

    #[test]
    fn test_from_prefix_splits_concatenated_transactions_from_bytes_buffer() {
        // given a `Bytes` buffer holding two serialized transactions back-to-back
        let first_transaction_bytes = wincode::serialize(&multiple_transfers()).unwrap();
        let second_transaction_bytes = wincode::serialize(&simple_v1_transaction()).unwrap();
        let buffer = bytes::Bytes::from(
            [
                first_transaction_bytes.as_slice(),
                second_transaction_bytes.as_slice(),
            ]
            .concat(),
        );

        // when parsing the first transaction and continuing from where it ends
        let (first_view, first_consumed_len) =
            TransactionView::try_new_unsanitized_from_prefix(buffer.clone()).unwrap();
        let remaining_buffer = buffer.slice(first_consumed_len..);
        let second_view = TransactionView::try_new_unsanitized(remaining_buffer).unwrap();

        // then each view exposes exactly its own transaction
        assert_eq!(first_view.data(), first_transaction_bytes.as_slice());
        assert!(matches!(first_view.version(), TransactionVersion::Legacy));
        assert_eq!(second_view.data(), second_transaction_bytes.as_slice());
        assert!(matches!(second_view.version(), TransactionVersion::V1));
    }
}
