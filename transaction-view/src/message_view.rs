use {
    crate::{
        address_table_lookup_frame::AddressTableLookupIterator,
        instructions_frame::InstructionsIterator, message_frame::MessageFrame,
        transaction_config_frame::TransactionConfigView, transaction_data::TransactionData,
        transaction_version::TransactionVersion, transaction_view::TransactionView,
    },
    core::fmt::{Debug, Formatter},
    solana_hash::Hash,
    solana_pubkey::Pubkey,
    solana_svm_transaction::instruction::SVMInstruction,
};

// alias for convenience
pub type UnsanitizedMessageViewRef<'a> = MessageViewRef<'a, false>;
pub type SanitizedMessageViewRef<'a> = MessageViewRef<'a, true>;

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
