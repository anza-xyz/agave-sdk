use {
    crate::{
        address_table_lookup_frame::AddressTableLookupIterator,
        instructions_frame::InstructionsIterator,
        message_frame::MessageFrame,
        result::Result,
        sanitize::{SanitizeConfig, sanitize_message_body},
        transaction_config_frame::TransactionConfigView,
        transaction_data::TransactionData,
        transaction_version::TransactionVersion,
    },
    core::fmt::{Debug, Formatter},
    solana_hash::Hash,
    solana_pubkey::Pubkey,
    solana_svm_transaction::instruction::SVMInstruction,
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
/// A `MessageView` is borrowed from a transaction through
/// [`TransactionView::message`](crate::transaction_view::TransactionView::message).
/// [`Self::data`] returns only the serialized message.
#[derive(Clone)]
pub struct MessageView<const SANITIZED: bool, D: TransactionData> {
    /// The parsed buffer, i.e. the transaction's buffer, which also holds the
    /// signatures and may hold trailing bytes.
    data: D,
    frame: MessageFrame,
}

impl<D: TransactionData> MessageView<false, D> {
    /// Creates a `MessageView` from a frame parsed from `data`.
    ///
    /// # Safety
    /// - `frame` must have been parsed from `data.data()`.
    #[inline]
    pub(crate) unsafe fn from_frame(data: D, frame: MessageFrame) -> Self {
        Self { data, frame }
    }

    /// Runs the sanitization checks that only concern the message, returning
    /// a sanitized view on success.
    pub(crate) fn sanitize_body(self, config: &SanitizeConfig) -> Result<SanitizedMessageView<D>> {
        sanitize_message_body(&self, config)?;
        Ok(SanitizedMessageView {
            data: self.data,
            frame: self.frame,
        })
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
    /// This does not include the signatures.
    #[inline]
    pub fn data(&self) -> &[u8] {
        let (start, end) = self.frame.message_range();
        &self.data.data()[usize::from(start)..usize::from(end)]
    }

    /// Return the parsed buffer.
    #[inline]
    pub(crate) fn inner_data(&self) -> &D {
        &self.data
    }

    /// Return the parsed buffer.
    #[inline]
    pub(crate) fn into_inner_data(self) -> D {
        self.data
    }

    /// Return the framing data of the message.
    #[inline]
    pub(crate) fn frame(&self) -> &MessageFrame {
        &self.frame
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
