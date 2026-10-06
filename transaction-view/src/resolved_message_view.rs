use {
    crate::{
        message_view::{MessageView, SanitizedMessageViewRef, impl_svm_static_message},
        result::{Result, TransactionViewError},
        transaction_data::TransactionData,
        transaction_version::TransactionVersion,
    },
    core::{
        fmt::{Debug, Formatter},
        ops::Deref,
    },
    solana_message::{
        AccountKeys,
        v0::{LoadedAddresses, LoadedAddressesView},
    },
    solana_pubkey::Pubkey,
    solana_sdk_ids::bpf_loader_upgradeable,
    solana_svm_transaction::svm_message::SVMMessage,
    std::{collections::HashSet, hash::BuildHasher},
};

/// A parsed and sanitized message view that has had all address lookups
/// resolved.
///
/// The address source defaults to [`LoadedAddresses`]. Other source types can
/// be used when [`LoadedAddressesView`] implements `From<&A>` for the source.
#[derive(Clone)]
pub struct ResolvedMessageView<D: TransactionData, A = LoadedAddresses> {
    /// The parsed and sanitized message view.
    message_view: MessageView<true, D>,
    /// The resolved address lookups.
    resolved_addresses: Option<A>,
    /// A cache for whether an address is writable.
    // Sanitized messages are guaranteed to have a maximum of 256 keys,
    // because account indexing is done with a u8.
    writable_cache: [bool; 256],
}

impl<D: TransactionData, A> Deref for ResolvedMessageView<D, A> {
    type Target = MessageView<true, D>;

    fn deref(&self) -> &Self::Target {
        &self.message_view
    }
}

impl<D: TransactionData> ResolvedMessageView<D> {
    /// Given a parsed and sanitized message view, and a set of resolved
    /// addresses, create a resolved message view.
    pub fn try_new<S: BuildHasher>(
        message_view: MessageView<true, D>,
        resolved_addresses: Option<LoadedAddresses>,
        reserved_account_keys: &HashSet<Pubkey, S>,
    ) -> Result<Self> {
        Self::try_new_with_source(message_view, resolved_addresses, reserved_account_keys)
    }
}

impl<D: TransactionData, A> ResolvedMessageView<D, A>
where
    for<'a> LoadedAddressesView<'a>: From<&'a A>,
{
    /// Given a parsed and sanitized message view, and a generic source of
    /// resolved addresses, create a resolved message view.
    pub fn try_new_with_source<S: BuildHasher>(
        message_view: MessageView<true, D>,
        resolved_addresses: Option<A>,
        reserved_account_keys: &HashSet<Pubkey, S>,
    ) -> Result<Self> {
        let resolved_addresses_view = resolved_addresses.as_ref().map(LoadedAddressesView::from);
        verify_resolved_addresses(message_view.message(), resolved_addresses_view)?;

        let writable_cache = cache_is_writable(
            message_view.message(),
            resolved_addresses_view,
            reserved_account_keys,
        );
        Ok(Self {
            message_view,
            resolved_addresses,
            writable_cache,
        })
    }

    /// Returns a borrowed view of the resolved addresses.
    fn loaded_addresses_view(&self) -> Option<LoadedAddressesView<'_>> {
        self.resolved_addresses
            .as_ref()
            .map(LoadedAddressesView::from)
    }
}

impl<D: TransactionData, A> ResolvedMessageView<D, A> {
    pub fn into_view(self) -> MessageView<true, D> {
        self.message_view
    }
}

impl_svm_static_message!(
    [D: TransactionData, A] ResolvedMessageView<D, A>,
    |self| self.message_view.message()
);

impl<D: TransactionData, A> SVMMessage for ResolvedMessageView<D, A>
where
    for<'a> LoadedAddressesView<'a>: From<&'a A>,
{
    fn account_keys(&self) -> AccountKeys<'_> {
        AccountKeys::new_with_loaded_addresses_view(
            self.message_view.message().static_account_keys(),
            self.loaded_addresses_view(),
        )
    }

    fn is_writable(&self, index: usize) -> bool {
        self.writable_cache.get(index).copied().unwrap_or(false)
    }
}

impl<D: TransactionData, A> Debug for ResolvedMessageView<D, A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ResolvedMessageView")
            .field("message_view", &self.message_view)
            .finish()
    }
}

/// Checks that the resolved addresses match the address table lookups of the
/// message.
pub(crate) fn verify_resolved_addresses(
    view: SanitizedMessageViewRef<'_>,
    resolved_addresses_view: Option<LoadedAddressesView<'_>>,
) -> Result<()> {
    // verify that the number of readable and writable match up.
    // This is a basic sanity check to make sure we're not passing a totally
    // invalid set of resolved addresses.
    // Additionally if it is a v0 message it *must* have resolved
    // addresses, even if they are empty.
    if matches!(view.version(), TransactionVersion::V0) && resolved_addresses_view.is_none() {
        return Err(TransactionViewError::AddressLookupMismatch);
    }
    if let Some(loaded_addresses_view) = resolved_addresses_view {
        if loaded_addresses_view.writable.len()
            != usize::from(view.total_writable_lookup_accounts())
            || loaded_addresses_view.readonly.len()
                != usize::from(view.total_readonly_lookup_accounts())
        {
            return Err(TransactionViewError::AddressLookupMismatch);
        }
    } else if view.total_writable_lookup_accounts() != 0
        || view.total_readonly_lookup_accounts() != 0
    {
        return Err(TransactionViewError::AddressLookupMismatch);
    }

    Ok(())
}

/// Helper function to check if an address is writable,
/// and cache the result.
/// This is done so we avoid recomputing the expensive checks each time we call
/// `is_writable` - since there is more to it than just checking index.
pub(crate) fn cache_is_writable<S: BuildHasher>(
    view: SanitizedMessageViewRef<'_>,
    resolved_addresses: Option<LoadedAddressesView<'_>>,
    reserved_account_keys: &HashSet<Pubkey, S>,
) -> [bool; 256] {
    // Build account keys so that we can iterate over and check if
    // an address is writable.
    let account_keys =
        AccountKeys::new_with_loaded_addresses_view(view.static_account_keys(), resolved_addresses);

    let mut is_writable_cache = [false; 256];
    let num_static_account_keys = usize::from(view.num_static_account_keys());
    let num_writable_lookup_accounts = usize::from(view.total_writable_lookup_accounts());
    let num_signed_accounts = usize::from(view.num_required_signatures());
    let num_writable_unsigned_static_accounts =
        usize::from(view.num_writable_unsigned_static_accounts());
    let num_writable_signed_static_accounts =
        usize::from(view.num_writable_signed_static_accounts());

    for (index, key) in account_keys.iter().enumerate() {
        let is_requested_write = {
            // If the account is a resolved address, check if it is writable.
            if index >= num_static_account_keys {
                let loaded_address_index = index.wrapping_sub(num_static_account_keys);
                loaded_address_index < num_writable_lookup_accounts
            } else if index >= num_signed_accounts {
                let unsigned_account_index = index.wrapping_sub(num_signed_accounts);
                unsigned_account_index < num_writable_unsigned_static_accounts
            } else {
                index < num_writable_signed_static_accounts
            }
        };

        // If the key is reserved it cannot be writable.
        is_writable_cache[index] = is_requested_write && !reserved_account_keys.contains(key);
    }

    // If a program account is locked, it cannot be writable unless the
    // upgradable loader is present.
    // However, checking for the upgradable loader is somewhat expensive, so
    // we only do it if we find a writable program id.
    let mut is_upgradable_loader_present = None;
    for ix in view.instructions_iter() {
        let program_id_index = usize::from(ix.program_id_index);
        if is_writable_cache[program_id_index]
            && !*is_upgradable_loader_present.get_or_insert_with(|| {
                for key in account_keys.iter() {
                    if key == &bpf_loader_upgradeable::ID {
                        return true;
                    }
                }
                false
            })
        {
            is_writable_cache[program_id_index] = false;
        }
    }

    is_writable_cache
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            resolved_transaction_view::ResolvedTransactionView, sanitize::tests::test_config,
            transaction_view::TransactionView,
        },
        solana_hash::Hash,
        solana_message::{
            MessageHeader, VersionedMessage,
            compiled_instruction::CompiledInstruction,
            v0::{self, MessageAddressTableLookup},
        },
        solana_pubkey::PubkeyHasherBuilder,
        solana_sdk_ids::system_program,
        solana_signature::Signature,
        solana_transaction::versioned::VersionedTransaction,
        std::assert_matches,
    };

    #[test]
    fn test_resolves_like_signed_transaction() {
        let payer = Pubkey::new_unique();
        let program_id = Pubkey::new_unique();
        let writable_lookup_key = Pubkey::new_unique();
        let readonly_lookup_key = Pubkey::new_unique();
        let message = VersionedMessage::V0(v0::Message {
            // Requests write locks on all static keys.
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![payer, system_program::id(), program_id],
            recent_blockhash: Hash::default(),
            instructions: vec![CompiledInstruction {
                program_id_index: 2,
                accounts: vec![],
                data: vec![],
            }],
            address_table_lookups: vec![MessageAddressTableLookup {
                account_key: Pubkey::new_unique(),
                writable_indexes: vec![0],
                readonly_indexes: vec![1],
            }],
        });
        let loaded_addresses = LoadedAddresses {
            writable: vec![writable_lookup_key],
            readonly: vec![readonly_lookup_key],
        };
        let mut reserved_account_keys = HashSet::with_hasher(PubkeyHasherBuilder::default());
        reserved_account_keys.insert(system_program::id());

        let message_bytes = wincode::serialize(&message).unwrap();
        let message_view =
            MessageView::try_new_sanitized(message_bytes.as_slice(), &test_config()).unwrap();
        assert_matches!(
            ResolvedMessageView::try_new(message_view.clone(), None, &reserved_account_keys),
            Err(TransactionViewError::AddressLookupMismatch),
            "a v0 message with address table lookups must have resolved addresses"
        );
        let resolved_message_view = ResolvedMessageView::try_new(
            message_view,
            Some(loaded_addresses.clone()),
            &reserved_account_keys,
        )
        .unwrap();

        let transaction_bytes = wincode::serialize(&VersionedTransaction {
            signatures: vec![Signature::default()],
            message,
        })
        .unwrap();
        let transaction_view =
            TransactionView::try_new_sanitized(transaction_bytes.as_slice(), &test_config())
                .unwrap();
        let resolved_transaction_view = ResolvedTransactionView::try_new(
            transaction_view,
            Some(loaded_addresses),
            &reserved_account_keys,
        )
        .unwrap();

        let expected_keys_and_writability = vec![
            (payer, true),
            // Reserved keys are demoted to readonly.
            (system_program::id(), false),
            // Invoked programs are demoted to readonly, unless the
            // upgradeable loader is present.
            (program_id, false),
            // Loaded addresses follow the static keys, writable ones first.
            (writable_lookup_key, true),
            (readonly_lookup_key, false),
        ];
        assert_eq!(
            keys_and_writability(&resolved_message_view),
            expected_keys_and_writability
        );
        assert_eq!(
            keys_and_writability(&resolved_transaction_view),
            expected_keys_and_writability
        );
    }

    /// Returns each account key of `message` and whether it is writable.
    fn keys_and_writability(message: &impl SVMMessage) -> Vec<(Pubkey, bool)> {
        message
            .account_keys()
            .iter()
            .enumerate()
            .map(|(index, key)| (*key, message.is_writable(index)))
            .collect()
    }
}
