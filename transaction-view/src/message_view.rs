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
        transaction_view::TransactionView,
    },
    core::fmt::{Debug, Formatter},
    solana_hash::Hash,
    solana_pubkey::Pubkey,
    solana_svm_transaction::instruction::SVMInstruction,
};

// alias for convenience
pub type UnsanitizedMessageView<D> = MessageView<false, D>;
pub type SanitizedMessageView<D> = MessageView<true, D>;
pub type UnsanitizedMessageViewRef<'a> = MessageViewRef<'a, false>;
pub type SanitizedMessageViewRef<'a> = MessageViewRef<'a, true>;

/// A view into a serialized message.
///
/// This struct parses a serialized message on its own, and provides access
/// to it through the [`MessageViewRef`] returned by [`Self::message`].
/// The owned `data` is abstracted through the `TransactionData` trait,
/// so that different containers for the serialized message can be used.
#[derive(Clone)]
pub struct MessageView<const SANITIZED: bool, D: TransactionData> {
    data: D,
    message_frame: MessageFrame,
}

impl<D: TransactionData> MessageView<false, D> {
    /// Creates a new `MessageView` without running sanitization checks.
    /// The `data` must contain a serialized message with no trailing bytes.
    pub fn try_new_unsanitized(data: D) -> Result<Self> {
        let message_frame = MessageFrame::try_new(data.data())?;
        Ok(Self {
            data,
            message_frame,
        })
    }

    /// Sanitizes the message view, returning a sanitized view on success.
    ///
    /// A message is sanitized if the transaction formed by signing it would
    /// pass [`TransactionView::sanitize`](crate::transaction_view::TransactionView::sanitize).
    /// Like it, this does not reject duplicate account keys, which are only
    /// checked when the accounts of a transaction are locked. Callers that do
    /// not lock the accounts of the message must check for them.
    pub fn sanitize(self, config: &SanitizeConfig) -> Result<SanitizedMessageView<D>> {
        sanitize_message(self.message(), config)?;
        Ok(SanitizedMessageView {
            data: self.data,
            message_frame: self.message_frame,
        })
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
    /// Return a view of the message.
    #[inline]
    pub fn message(&self) -> MessageViewRef<'_, SANITIZED> {
        MessageViewRef {
            data: &self.data.data()[..usize::from(self.message_frame.end_offset())],
            message_frame: &self.message_frame,
        }
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

// Manual implementation of `Debug` - avoids bound on `D`.
impl<const SANITIZED: bool, D: TransactionData> Debug for MessageView<SANITIZED, D> {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("MessageView")
            .field("message", &self.message())
            .finish()
    }
}

/// A view into a serialized message.
///
/// A message is the signed portion of a transaction, i.e. the transaction
/// without its signatures. This struct provides access to the message data
/// without deserializing it. It borrows the data and framing it views, so it
/// is cheap to copy.
#[derive(Clone, Copy)]
pub struct MessageViewRef<'a, const SANITIZED: bool> {
    data: &'a [u8],
    message_frame: &'a MessageFrame,
}

impl<'a, const SANITIZED: bool> MessageViewRef<'a, SANITIZED> {
    /// Creates a view of the message of `transaction_view`.
    #[inline]
    pub(crate) fn from_transaction_view<D: TransactionData>(
        transaction_view: &'a TransactionView<SANITIZED, D>,
    ) -> Self {
        Self {
            data: transaction_view.data(),
            message_frame: transaction_view.message_frame(),
        }
    }

    /// Return the version of the message.
    #[inline]
    pub fn version(&self) -> TransactionVersion {
        self.message_frame.version()
    }

    /// Return the number of required signatures in the message.
    #[inline]
    pub fn num_required_signatures(&self) -> u8 {
        self.message_frame.num_required_signatures()
    }

    /// Return the number of readonly signed static accounts in the message.
    #[inline]
    pub fn num_readonly_signed_static_accounts(&self) -> u8 {
        self.message_frame.num_readonly_signed_static_accounts()
    }

    /// Return the number of readonly unsigned static accounts in the message.
    #[inline]
    pub fn num_readonly_unsigned_static_accounts(&self) -> u8 {
        self.message_frame.num_readonly_unsigned_static_accounts()
    }

    /// Return the number of static account keys in the message.
    #[inline]
    pub fn num_static_account_keys(&self) -> u8 {
        self.message_frame.num_static_account_keys()
    }

    /// Return the number of instructions in the message.
    #[inline]
    pub fn num_instructions(&self) -> u16 {
        self.message_frame.num_instructions()
    }

    /// Return the number of address table lookups in the message.
    #[inline]
    pub fn num_address_table_lookups(&self) -> u8 {
        self.message_frame.num_address_table_lookups()
    }

    /// Return the number of writable lookup accounts in the message.
    #[inline]
    pub fn total_writable_lookup_accounts(&self) -> u16 {
        self.message_frame.total_writable_lookup_accounts()
    }

    /// Return the number of readonly lookup accounts in the message.
    #[inline]
    pub fn total_readonly_lookup_accounts(&self) -> u16 {
        self.message_frame.total_readonly_lookup_accounts()
    }

    /// Return the slice of static account keys in the message.
    #[inline]
    pub fn static_account_keys(&self) -> &'a [Pubkey] {
        // SAFETY: `message_frame` was created from `data`.
        unsafe { self.message_frame.static_account_keys(self.data) }
    }

    /// Return the recent blockhash in the message.
    #[inline]
    pub fn recent_blockhash(&self) -> &'a Hash {
        // SAFETY: `message_frame` was created from `data`.
        unsafe { self.message_frame.recent_blockhash(self.data) }
    }

    /// Return an iterator over the instructions in the message.
    #[inline]
    pub fn instructions_iter(&self) -> InstructionsIterator<'a> {
        // SAFETY: `message_frame` was created from `data`.
        unsafe { self.message_frame.instructions_iter(self.data) }
    }

    /// Return an iterator over the address table lookups in the message.
    #[inline]
    pub fn address_table_lookup_iter(&self) -> AddressTableLookupIterator<'a> {
        // SAFETY: `message_frame` was created from `data`.
        unsafe { self.message_frame.address_table_lookup_iter(self.data) }
    }

    /// Return Some(TransactionConfigView) for V1, None for legacy/V0
    #[inline]
    pub fn transaction_config(&self) -> Option<TransactionConfigView<'a>> {
        let transaction_config_frame = self.message_frame.transaction_config_frame();
        transaction_config_frame
            .is_present()
            .then_some(TransactionConfigView {
                transaction_config_frame,
                bytes: self.data,
            })
    }

    /// Return the serialized message data.
    /// This does not include the signatures.
    #[inline]
    pub fn data(&self) -> &'a [u8] {
        let (start, end) = self.message_frame.message_range();
        &self.data[usize::from(start)..usize::from(end)]
    }
}

// Implementation that relies on sanitization checks having been run.
impl<'a> MessageViewRef<'a, true> {
    /// Return an iterator over the instructions paired with their program ids.
    pub fn program_instructions_iter(
        &self,
    ) -> impl Iterator<Item = (&'a Pubkey, SVMInstruction<'a>)> + Clone + use<'a> {
        let static_account_keys = self.static_account_keys();
        self.instructions_iter().map(move |ix| {
            let program_id_index = usize::from(ix.program_id_index);
            let program_id = &static_account_keys[program_id_index];
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

// Manual implementation of `Debug` - avoids printing the raw buffer.
// Prints nicely formatted struct-ish fields even for the iterator fields.
impl<const SANITIZED: bool> Debug for MessageViewRef<'_, SANITIZED> {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("MessageViewRef")
            .field("message_frame", self.message_frame)
            .field("static_account_keys", &self.static_account_keys())
            .field("recent_blockhash", &self.recent_blockhash())
            .field("instructions", &self.instructions_iter())
            .field("address_table_lookups", &self.address_table_lookup_iter())
            .finish()
    }
}

/// Implements `SVMStaticMessage` for `$ty` by forwarding to the accessors of
/// `$view`, a sanitized transaction or message view. `$self` must be `self`:
/// it is passed in so that `$view` can refer to it.
macro_rules! impl_svm_static_message {
    ([$($generics:tt)*] $ty:ty, |$self:ident| $view:expr) => {
        impl<$($generics)*> ::solana_svm_transaction::svm_message::SVMStaticMessage for $ty {
            fn version(&$self) -> ::solana_transaction::versioned::TransactionVersion {
                $view.version().into()
            }

            fn num_transaction_signatures(&$self) -> u64 {
                $view.num_required_signatures() as u64
            }

            fn num_write_locks(&$self) -> u64 {
                $view.num_requested_write_locks()
            }

            fn num_readonly_signed_static_accounts(&$self) -> u8 {
                $view.num_readonly_signed_static_accounts()
            }

            fn num_readonly_unsigned_static_accounts(&$self) -> u8 {
                $view.num_readonly_unsigned_static_accounts()
            }

            fn recent_blockhash(&$self) -> &::solana_hash::Hash {
                $view.recent_blockhash()
            }

            fn num_instructions(&$self) -> usize {
                $view.num_instructions() as usize
            }

            fn instructions_iter(
                &$self,
            ) -> impl Iterator<Item = ::solana_svm_transaction::instruction::SVMInstruction<'_>>
            {
                $view.instructions_iter()
            }

            fn program_instructions_iter(
                &$self,
            ) -> impl Iterator<
                Item = (
                    &::solana_pubkey::Pubkey,
                    ::solana_svm_transaction::instruction::SVMInstruction<'_>,
                ),
            > + Clone {
                $view.program_instructions_iter()
            }

            fn static_account_keys(&$self) -> &[::solana_pubkey::Pubkey] {
                $view.static_account_keys()
            }

            fn fee_payer(&$self) -> &::solana_pubkey::Pubkey {
                &$view.static_account_keys()[0]
            }

            fn num_lookup_tables(&$self) -> usize {
                $view.num_address_table_lookups() as usize
            }

            fn message_address_table_lookups(
                &$self,
            ) -> impl Iterator<
                Item = ::solana_svm_transaction::message_address_table_lookup::SVMMessageAddressTableLookup<'_>,
            > {
                $view.address_table_lookup_iter()
            }

            fn is_signer(&$self, index: usize) -> bool {
                index < usize::from($view.num_required_signatures())
            }

            fn is_invoked(&$self, key_index: usize) -> bool {
                let Ok(index) = u8::try_from(key_index) else {
                    return false;
                };
                $view
                    .instructions_iter()
                    .any(|ix| ix.program_id_index == index)
            }
        }
    };
}
pub(crate) use impl_svm_static_message;

impl_svm_static_message!([] MessageViewRef<'_, true>, |self| self);

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            result::TransactionViewError,
            sanitize::tests::{
                create_legacy_transaction, create_v0_transaction, create_v1_transaction,
                multiple_transfers, test_config,
            },
            transaction_view::TransactionView,
        },
        solana_message::{
            AddressLookupTableAccount, Message, MessageHeader, VersionedMessage,
            compiled_instruction::CompiledInstruction, v0, v1,
        },
        solana_signature::Signature,
        solana_svm_transaction::{
            message_address_table_lookup::SVMMessageAddressTableLookup,
            svm_message::SVMStaticMessage,
        },
        solana_system_interface::instruction as system_instruction,
        solana_transaction::versioned::VersionedTransaction,
    };

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
        let message_view =
            MessageView::try_new_sanitized(message_bytes.as_slice(), &test_config()).unwrap();
        let message_view_ref = message_view.message();

        assert_eq!(message_view_ref.data(), message_bytes);
        assert_eq!(
            message_view_ref.num_required_signatures(),
            message.header().num_required_signatures
        );
        assert_eq!(
            message_view_ref.num_readonly_signed_static_accounts(),
            message.header().num_readonly_signed_accounts
        );
        assert_eq!(
            message_view_ref.num_readonly_unsigned_static_accounts(),
            message.header().num_readonly_unsigned_accounts
        );
        assert_eq!(
            message_view_ref.static_account_keys(),
            message.static_account_keys()
        );
        assert_eq!(
            message_view_ref.recent_blockhash(),
            message.recent_blockhash()
        );
        assert!(
            message_view_ref
                .instructions_iter()
                .eq(message.instructions().iter().map(SVMInstruction::from))
        );
        assert!(
            message_view_ref.address_table_lookup_iter().eq(message
                .address_table_lookups()
                .unwrap_or_default()
                .iter()
                .map(SVMMessageAddressTableLookup::from))
        );
        assert_eq!(
            message_view_ref.transaction_config().is_some(),
            matches!(message, VersionedMessage::V1(_))
        );

        // The message embedded in the signed transaction is the same message.
        let transaction = sign(message);
        assert_eq!(
            SVMStaticMessage::version(&message_view_ref),
            transaction.version()
        );
        let transaction_bytes = wincode::serialize(&transaction).unwrap();
        let transaction_view =
            TransactionView::try_new_unsanitized(transaction_bytes.as_slice()).unwrap();
        assert_eq!(transaction_view.message().data(), message_bytes);
    }

    #[test]
    fn test_legacy_message() {
        verify_message_view(multiple_transfers().message);
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
        fn transaction(
            version: TransactionVersion,
            num_required_signatures: u8,
            num_account_keys: u8,
            data_len: usize,
        ) -> VersionedTransaction {
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
            match version {
                TransactionVersion::Legacy => create_legacy_transaction(
                    num_required_signatures,
                    header,
                    account_keys,
                    instructions,
                ),
                TransactionVersion::V0 => create_v0_transaction(
                    num_required_signatures,
                    header,
                    account_keys,
                    instructions,
                    vec![],
                ),
                TransactionVersion::V1 => create_v1_transaction(
                    num_required_signatures,
                    header,
                    account_keys,
                    instructions,
                    v1::TransactionConfig::empty(),
                ),
            }
        }

        let mut cases = vec![
            // no fee payer
            (transaction(TransactionVersion::Legacy, 0, 2, 0), false),
            // signer without a static account key
            (transaction(TransactionVersion::Legacy, 3, 2, 0), false),
            // too many signers
            (transaction(TransactionVersion::V1, 13, 14, 0), false),
        ];
        for (version, max_transaction_size) in [
            (TransactionVersion::Legacy, solana_packet::PACKET_DATA_SIZE),
            (TransactionVersion::V0, solana_packet::PACKET_DATA_SIZE),
            (TransactionVersion::V1, v1::MAX_TRANSACTION_SIZE),
        ] {
            // The length of the instruction data prefix is the same for all
            // data lengths from 128 up to the maximum transaction size.
            let signed_len = |data_len| {
                wincode::serialize(&transaction(version, 1, 2, data_len))
                    .unwrap()
                    .len()
            };
            let max_data_len = max_transaction_size - signed_len(128) + 128;
            cases.push((transaction(version, 1, 2, max_data_len), true));
            cases.push((transaction(version, 1, 2, max_data_len + 1), false));
        }

        for (transaction, expect_ok) in cases {
            let message_bytes = wincode::serialize(&transaction.message).unwrap();
            let transaction_bytes = wincode::serialize(&transaction).unwrap();
            assert_eq!(
                MessageView::try_new_sanitized(message_bytes.as_slice(), &test_config()).is_ok(),
                expect_ok
            );
            assert_eq!(
                TransactionView::try_new_sanitized(transaction_bytes.as_slice(), &test_config())
                    .is_ok(),
                expect_ok
            );
        }
    }
}
