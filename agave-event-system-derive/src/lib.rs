#![cfg_attr(docsrs, feature(doc_cfg))]

use {
    proc_macro::TokenStream,
    proc_macro_crate::{FoundCrate, crate_name},
    proc_macro2::Span,
    quote::{format_ident, quote},
    syn::{DeriveInput, LitStr, parse::Parser, parse_macro_input},
};

/// Implements `agave_event_system::Event` for a type.
///
/// Types with dynamically sized fields must provide `max_serialized_size`.
/// The value must be greater than the lower bound reported by wincode's type
/// metadata.
///
/// ```rust,ignore
/// #[agave_event_system::event]
/// #[derive(Debug, PartialEq, Eq)]
/// enum SlotEvents {
///     Completed { slot: u64 },
///     Dropped {slot: u64 },
/// }
///
/// #[agave_event_system::event(
///     max_serialized_size = 1024,
/// )]
/// struct Message {
///     contents: String,
/// }
/// ```
#[proc_macro_attribute]
pub fn event(args: TokenStream, item: TokenStream) -> TokenStream {
    event_impl(args.into(), parse_macro_input!(item as DeriveInput))
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn event_impl(
    args: proc_macro2::TokenStream,
    input: DeriveInput,
) -> syn::Result<proc_macro2::TokenStream> {
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "Event cannot be derived for generic types on stable Rust; implement it by \
             hand instead (see `agave_event_system::Event::QueueCell`)",
        ));
    }

    let EventArgs {
        max_serialized_size,
    } = event_args(args)?;
    let ident = &input.ident;
    let event_system_crate = event_system_crate()?;
    let event_macro_path = quote!(#event_system_crate::__private::event_macro);
    let event_macro_path = LitStr::new(&event_macro_path.to_string(), Span::call_site());
    let max_serialized_size = match max_serialized_size {
        Some(max_serialized_size) => quote!(::core::option::Option::Some(#max_serialized_size)),
        None => quote!(::core::option::Option::None),
    };

    Ok(quote! {
        #[doc(hidden)]
        #[allow(unused_imports)]
        use #event_system_crate::__private::*;

        #[derive(
            #event_system_crate::__private::wincode::SchemaRead,
            #event_system_crate::__private::wincode::SchemaWrite,
            #event_system_crate::__private::wincode_dynamic::SchemaDynamic,
        )]
        #[wincode(crate = #event_macro_path)]
        #input

        // SAFETY: QueueCell is a byte array, so every bit pattern is valid,
        // it has no padding, and AsMut exposes its complete representation.
        unsafe impl #event_system_crate::Event for #ident {
            type QueueCell = [u8; #event_system_crate::event_queue_cell_size(
                <Self as #event_system_crate::__private::wincode_dynamic::SchemaDynamic>::SERIALIZED_SIZE,
                #max_serialized_size,
            )];
        }
    })
}

fn event_system_crate() -> syn::Result<proc_macro2::TokenStream> {
    match crate_name("agave-event-system") {
        Ok(FoundCrate::Itself) => Ok(quote!(::agave_event_system)),
        Ok(FoundCrate::Name(name)) => {
            let ident = format_ident!("{}", name.replace('-', "_"));
            Ok(quote!(::#ident))
        }
        Err(error) => Err(syn::Error::new(Span::call_site(), error)),
    }
}

struct EventArgs {
    max_serialized_size: Option<usize>,
}

fn event_args(args: proc_macro2::TokenStream) -> syn::Result<EventArgs> {
    let mut max_serialized_size = None;
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("max_serialized_size") {
            if max_serialized_size.is_some() {
                return Err(meta.error("duplicate `max_serialized_size`"));
            }
            let value = meta.value()?.parse::<syn::LitInt>()?;
            max_serialized_size = Some(value.base10_parse()?);
            Ok(())
        } else {
            Err(meta.error("unsupported event attribute"))
        }
    });
    parser.parse2(args)?;
    Ok(EventArgs {
        max_serialized_size,
    })
}
