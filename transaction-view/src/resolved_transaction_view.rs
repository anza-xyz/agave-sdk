use {
    crate::{
        message_view::impl_svm_static_message,
        resolved_message_view::{cache_is_writable, verify_resolved_addresses},
        result::Result,
        transaction_data::TransactionData,
        transaction_view::TransactionView,
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
    solana_signature::Signature,
    solana_svm_transaction::{svm_message::SVMMessage, svm_transaction::SVMStaticTransaction},
    std::{collections::HashSet, hash::BuildHasher},
};

/// A parsed and sanitized transaction view that has had all address lookups
/// resolved.
///
/// The address source defaults to [`LoadedAddresses`]. Other source types can
/// be used when [`LoadedAddressesView`] implements `From<&A>` for the source.
#[derive(Clone)]
pub struct ResolvedTransactionView<D: TransactionData, A = LoadedAddresses> {
    /// The parsed and sanitized transaction view.
    view: TransactionView<true, D>,
    /// The resolved address lookups.
    resolved_addresses: Option<A>,
    /// A cache for whether an address is writable.
    // Sanitized transactions are guaranteed to have a maximum of 256 keys,
    // because account indexing is done with a u8.
    writable_cache: [bool; 256],
}

impl<D: TransactionData, A> Deref for ResolvedTransactionView<D, A> {
    type Target = TransactionView<true, D>;

    fn deref(&self) -> &Self::Target {
        &self.view
    }
}

impl<D: TransactionData> ResolvedTransactionView<D> {
    /// Given a parsed and sanitized transaction view, and a set of resolved
    /// addresses, create a resolved transaction view.
    pub fn try_new<S: BuildHasher>(
        view: TransactionView<true, D>,
        resolved_addresses: Option<LoadedAddresses>,
        reserved_account_keys: &HashSet<Pubkey, S>,
    ) -> Result<Self> {
        Self::try_new_with_source(view, resolved_addresses, reserved_account_keys)
    }
}

impl<D: TransactionData, A> ResolvedTransactionView<D, A>
where
    for<'a> LoadedAddressesView<'a>: From<&'a A>,
{
    /// Given a parsed and sanitized transaction view, and a generic source of
    /// resolved addresses, create a resolved transaction view.
    pub fn try_new_with_source<S: BuildHasher>(
        view: TransactionView<true, D>,
        resolved_addresses: Option<A>,
        reserved_account_keys: &HashSet<Pubkey, S>,
    ) -> Result<Self> {
        let resolved_addresses_view = resolved_addresses.as_ref().map(LoadedAddressesView::from);

        verify_resolved_addresses(view.message(), resolved_addresses_view)?;

        let writable_cache = cache_is_writable(
            view.message(),
            resolved_addresses_view,
            reserved_account_keys,
        );
        Ok(Self {
            view,
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

impl<D: TransactionData, A> ResolvedTransactionView<D, A> {
    pub fn into_view(self) -> TransactionView<true, D> {
        self.view
    }
}

impl_svm_static_message!([D: TransactionData, A] ResolvedTransactionView<D, A>, |self| self.view);

impl<D: TransactionData, A> SVMMessage for ResolvedTransactionView<D, A>
where
    for<'a> LoadedAddressesView<'a>: From<&'a A>,
{
    fn account_keys(&self) -> AccountKeys<'_> {
        AccountKeys::new_with_loaded_addresses_view(
            self.view.static_account_keys(),
            self.loaded_addresses_view(),
        )
    }

    fn is_writable(&self, index: usize) -> bool {
        self.writable_cache.get(index).copied().unwrap_or(false)
    }
}

impl<D: TransactionData, A> SVMStaticTransaction for ResolvedTransactionView<D, A> {
    fn signature(&self) -> &Signature {
        &self.view.signatures()[0]
    }

    fn signatures(&self) -> &[Signature] {
        self.view.signatures()
    }
}

impl<D: TransactionData, A> Debug for ResolvedTransactionView<D, A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedTransactionView")
            .field("view", &self.view)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            result::TransactionViewError, sanitize::SanitizeConfig,
            transaction_view::SanitizedTransactionView,
        },
        solana_hash::Hash,
        solana_message::{
            MessageHeader, VersionedMessage,
            compiled_instruction::CompiledInstruction,
            v0::{self, MessageAddressTableLookup},
        },
        solana_pubkey::PubkeyHasherBuilder,
        solana_sdk_ids::{bpf_loader_upgradeable, system_program, sysvar},
        solana_signature::Signature,
        solana_transaction::versioned::VersionedTransaction,
    };

    // Current protocol values; production callers supply these from agave.
    fn test_config() -> SanitizeConfig {
        SanitizeConfig {
            min_requested_heap_size: 32 * 1024,
            max_requested_heap_size: 256 * 1024,
            max_instructions: 64,
            max_accounts_per_instruction: 255,
        }
    }

    #[test]
    fn test_expected_loaded_addresses() {
        // Expected addresses passed in, but `None` was passed.
        let static_keys = vec![Pubkey::new_unique(), Pubkey::new_unique()];
        let transaction = VersionedTransaction {
            signatures: vec![Signature::default()],
            message: VersionedMessage::V0(v0::Message {
                header: MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                instructions: vec![],
                account_keys: static_keys,
                address_table_lookups: vec![MessageAddressTableLookup {
                    account_key: Pubkey::new_unique(),
                    writable_indexes: vec![0],
                    readonly_indexes: vec![1],
                }],
                recent_blockhash: Hash::default(),
            }),
        };
        let bytes = wincode::serialize(&transaction).unwrap();
        let view =
            SanitizedTransactionView::try_new_sanitized(bytes.as_ref(), &test_config()).unwrap();
        let result = ResolvedTransactionView::try_new(
            view,
            None,
            &HashSet::with_hasher(PubkeyHasherBuilder::default()),
        );
        assert!(matches!(
            result,
            Err(TransactionViewError::AddressLookupMismatch)
        ));
    }

    #[test]
    fn test_unexpected_loaded_addresses() {
        // Expected no addresses passed in, but `Some` was passed.
        let static_keys = vec![Pubkey::new_unique(), Pubkey::new_unique()];
        let loaded_addresses = LoadedAddresses {
            writable: vec![Pubkey::new_unique()],
            readonly: vec![],
        };
        let transaction = VersionedTransaction {
            signatures: vec![Signature::default()],
            message: VersionedMessage::V0(v0::Message {
                header: MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                instructions: vec![],
                account_keys: static_keys,
                address_table_lookups: vec![],
                recent_blockhash: Hash::default(),
            }),
        };
        let bytes = wincode::serialize(&transaction).unwrap();
        let view =
            SanitizedTransactionView::try_new_sanitized(bytes.as_ref(), &test_config()).unwrap();
        let result = ResolvedTransactionView::try_new(
            view,
            Some(loaded_addresses),
            &HashSet::with_hasher(PubkeyHasherBuilder::default()),
        );
        assert!(matches!(
            result,
            Err(TransactionViewError::AddressLookupMismatch)
        ));
    }

    #[test]
    fn test_mismatched_loaded_address_lengths() {
        // Loaded addresses only has 1 writable address, no readonly.
        // The message ATL has 1 writable and 1 readonly.
        let static_keys = vec![Pubkey::new_unique(), Pubkey::new_unique()];
        let loaded_addresses = LoadedAddresses {
            writable: vec![Pubkey::new_unique()],
            readonly: vec![],
        };
        let transaction = VersionedTransaction {
            signatures: vec![Signature::default()],
            message: VersionedMessage::V0(v0::Message {
                header: MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                instructions: vec![],
                account_keys: static_keys,
                address_table_lookups: vec![MessageAddressTableLookup {
                    account_key: Pubkey::new_unique(),
                    writable_indexes: vec![0],
                    readonly_indexes: vec![1],
                }],
                recent_blockhash: Hash::default(),
            }),
        };
        let bytes = wincode::serialize(&transaction).unwrap();
        let view =
            SanitizedTransactionView::try_new_sanitized(bytes.as_ref(), &test_config()).unwrap();
        let result = ResolvedTransactionView::try_new(
            view,
            Some(loaded_addresses),
            &HashSet::with_hasher(PubkeyHasherBuilder::default()),
        );
        assert!(matches!(
            result,
            Err(TransactionViewError::AddressLookupMismatch)
        ));
    }

    #[test]
    fn test_is_writable() {
        let mut reserved_account_keys: HashSet<Pubkey, PubkeyHasherBuilder> =
            HashSet::with_hasher(PubkeyHasherBuilder::default());
        reserved_account_keys.extend([sysvar::clock::id(), system_program::id()]);
        // Create a versioned transaction.
        let create_transaction_with_keys =
            |static_keys: Vec<Pubkey>, loaded_addresses: &LoadedAddresses| VersionedTransaction {
                signatures: vec![Signature::default()],
                message: VersionedMessage::V0(v0::Message {
                    header: MessageHeader {
                        num_required_signatures: 1,
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 1,
                    },
                    account_keys: static_keys[..2].to_vec(),
                    recent_blockhash: Hash::default(),
                    instructions: vec![],
                    address_table_lookups: vec![MessageAddressTableLookup {
                        account_key: Pubkey::new_unique(),
                        writable_indexes: (0..loaded_addresses.writable.len())
                            .map(|x| (static_keys.len() + x) as u8)
                            .collect(),
                        readonly_indexes: (0..loaded_addresses.readonly.len())
                            .map(|x| {
                                (static_keys.len() + loaded_addresses.writable.len() + x) as u8
                            })
                            .collect(),
                    }],
                }),
            };

        let key0 = Pubkey::new_unique();
        let key1 = Pubkey::new_unique();
        let key2 = Pubkey::new_unique();
        {
            let static_keys = vec![sysvar::clock::id(), key0];
            let loaded_addresses = LoadedAddresses {
                writable: vec![key1],
                readonly: vec![key2],
            };
            let transaction = create_transaction_with_keys(static_keys, &loaded_addresses);
            let bytes = wincode::serialize(&transaction).unwrap();
            let view = SanitizedTransactionView::try_new_sanitized(bytes.as_ref(), &test_config())
                .unwrap();
            let resolved_view = ResolvedTransactionView::try_new(
                view,
                Some(loaded_addresses),
                &reserved_account_keys,
            )
            .unwrap();

            // demote reserved static key to readonly
            let expected = vec![false, false, true, false];
            for (index, expected) in expected.into_iter().enumerate() {
                assert_eq!(resolved_view.is_writable(index), expected);
            }
        }

        {
            let static_keys = vec![system_program::id(), key0];
            let loaded_addresses = LoadedAddresses {
                writable: vec![key1],
                readonly: vec![key2],
            };
            let transaction = create_transaction_with_keys(static_keys, &loaded_addresses);
            let bytes = wincode::serialize(&transaction).unwrap();
            let view = SanitizedTransactionView::try_new_sanitized(bytes.as_ref(), &test_config())
                .unwrap();
            let resolved_view = ResolvedTransactionView::try_new(
                view,
                Some(loaded_addresses),
                &reserved_account_keys,
            )
            .unwrap();

            // demote reserved static key to readonly
            let expected = vec![false, false, true, false];
            for (index, expected) in expected.into_iter().enumerate() {
                assert_eq!(resolved_view.is_writable(index), expected);
            }
        }

        {
            let static_keys = vec![key0, key1];
            let loaded_addresses = LoadedAddresses {
                writable: vec![system_program::id()],
                readonly: vec![key2],
            };
            let transaction = create_transaction_with_keys(static_keys, &loaded_addresses);
            let bytes = wincode::serialize(&transaction).unwrap();
            let view = SanitizedTransactionView::try_new_sanitized(bytes.as_ref(), &test_config())
                .unwrap();
            let resolved_view = ResolvedTransactionView::try_new(
                view,
                Some(loaded_addresses),
                &reserved_account_keys,
            )
            .unwrap();

            // demote loaded key to readonly
            let expected = vec![true, false, false, false];
            for (index, expected) in expected.into_iter().enumerate() {
                assert_eq!(resolved_view.is_writable(index), expected);
            }
        }
    }

    #[test]
    fn test_demote_writable_program() {
        let reserved_account_keys: HashSet<Pubkey, PubkeyHasherBuilder> =
            HashSet::with_hasher(PubkeyHasherBuilder::default());
        let key0 = Pubkey::new_unique();
        let key1 = Pubkey::new_unique();
        let key2 = Pubkey::new_unique();
        let key3 = Pubkey::new_unique();
        let key4 = Pubkey::new_unique();
        let loaded_addresses = LoadedAddresses {
            writable: vec![key3, key4],
            readonly: vec![],
        };
        let create_transaction_with_static_keys =
            |static_keys: Vec<Pubkey>, loaded_addresses: &LoadedAddresses| VersionedTransaction {
                signatures: vec![Signature::default()],
                message: VersionedMessage::V0(v0::Message {
                    header: MessageHeader {
                        num_required_signatures: 1,
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 0,
                    },
                    instructions: vec![CompiledInstruction {
                        program_id_index: 1,
                        accounts: vec![0],
                        data: vec![],
                    }],
                    account_keys: static_keys,
                    address_table_lookups: vec![MessageAddressTableLookup {
                        account_key: Pubkey::new_unique(),
                        writable_indexes: (0..loaded_addresses.writable.len())
                            .map(|x| x as u8)
                            .collect(),
                        readonly_indexes: (0..loaded_addresses.readonly.len())
                            .map(|x| (loaded_addresses.writable.len() + x) as u8)
                            .collect(),
                    }],
                    recent_blockhash: Hash::default(),
                }),
            };

        // Demote writable program - static
        {
            let static_keys = vec![key0, key1, key2];
            let transaction = create_transaction_with_static_keys(static_keys, &loaded_addresses);
            let bytes = wincode::serialize(&transaction).unwrap();
            let view = SanitizedTransactionView::try_new_sanitized(bytes.as_ref(), &test_config())
                .unwrap();
            let resolved_view = ResolvedTransactionView::try_new(
                view,
                Some(loaded_addresses.clone()),
                &reserved_account_keys,
            )
            .unwrap();

            let expected = vec![true, false, true, true, true];
            for (index, expected) in expected.into_iter().enumerate() {
                assert_eq!(resolved_view.is_writable(index), expected);
            }
        }

        // Do not demote writable program - static address: upgradable loader
        {
            let static_keys = vec![key0, key1, bpf_loader_upgradeable::ID];
            let transaction = create_transaction_with_static_keys(static_keys, &loaded_addresses);
            let bytes = wincode::serialize(&transaction).unwrap();
            let view = SanitizedTransactionView::try_new_sanitized(bytes.as_ref(), &test_config())
                .unwrap();
            let resolved_view = ResolvedTransactionView::try_new(
                view,
                Some(loaded_addresses.clone()),
                &reserved_account_keys,
            )
            .unwrap();

            let expected = vec![true, true, true, true, true];
            for (index, expected) in expected.into_iter().enumerate() {
                assert_eq!(resolved_view.is_writable(index), expected);
            }
        }

        // Do not demote writable program - loaded address: upgradable loader
        {
            let static_keys = vec![key0, key1, key2];
            let loaded_addresses = LoadedAddresses {
                writable: vec![key3],
                readonly: vec![bpf_loader_upgradeable::ID],
            };
            let transaction = create_transaction_with_static_keys(static_keys, &loaded_addresses);
            let bytes = wincode::serialize(&transaction).unwrap();
            let view = SanitizedTransactionView::try_new_sanitized(bytes.as_ref(), &test_config())
                .unwrap();

            let resolved_view = ResolvedTransactionView::try_new(
                view,
                Some(loaded_addresses.clone()),
                &reserved_account_keys,
            )
            .unwrap();

            let expected = vec![true, true, true, true, false];
            for (index, expected) in expected.into_iter().enumerate() {
                assert_eq!(resolved_view.is_writable(index), expected);
            }
        }
    }
}
