//! The `#[command]` attribute macro for tower-amp. Use it through
//! `tower_amp::command`; see that crate's documentation.

#![forbid(unsafe_code)]

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{Data, DeriveInput, LitStr, Token, Type, parse_macro_input};

/// Declares the struct it is attached to as a command's arguments.
///
/// Takes `name = "..."`, the command's name on the wire, which defaults to
/// the struct's name, and `response = Type`, the type of the command's
/// results, which defaults to `()`. Generates an implementation of
/// `tower_amp::Command` and leaves the struct itself untouched.
///
/// The generated code refers to the crate as `::tower_amp`, so tower-amp must
/// be a dependency under its own name. Renaming it with `package = "tower-amp"`
/// in Cargo.toml is not supported.
#[proc_macro_attribute]
pub fn command(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as Args);
    let input = parse_macro_input!(item as DeriveInput);

    // A command's arguments are a box on the wire, which only a struct
    // serializes to. Catch anything else here rather than at runtime.
    let keyword = match &input.data {
        Data::Struct(_) => None,
        Data::Enum(data) => Some(data.enum_token.span),
        Data::Union(data) => Some(data.union_token.span),
    };
    if let Some(span) = keyword {
        return syn::Error::new(span, "`#[command]` must be attached to a struct")
            .to_compile_error()
            .into();
    }

    let ident = &input.ident;
    let name = args
        .name
        .unwrap_or_else(|| LitStr::new(&ident.to_string(), ident.span()));
    let response = args
        .response
        .unwrap_or_else(|| syn::parse_quote_spanned!(Span::call_site() => ()));
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    quote! {
        #input

        impl #impl_generics ::tower_amp::Command for #ident #ty_generics #where_clause {
            const NAME: &'static str = #name;
            type Response = #response;
        }
    }
    .into()
}

/// The arguments of `#[command(...)]`.
#[derive(Default)]
struct Args {
    name: Option<LitStr>,
    response: Option<Type>,
}

impl Parse for Args {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut args = Args::default();
        for arg in Punctuated::<Arg, Token![,]>::parse_terminated(input)? {
            match arg {
                Arg::Name(key, name) => {
                    if args.name.replace(name).is_some() {
                        return Err(syn::Error::new(key.span(), "`name` given twice"));
                    }
                }
                Arg::Response(key, response) => {
                    if args.response.replace(*response).is_some() {
                        return Err(syn::Error::new(key.span(), "`response` given twice"));
                    }
                }
            }
        }
        Ok(args)
    }
}

enum Arg {
    Name(syn::Ident, LitStr),
    Response(syn::Ident, Box<Type>),
}

impl Parse for Arg {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let key: syn::Ident = input.parse()?;
        input.parse::<Token![=]>()?;
        match key.to_string().as_str() {
            "name" => Ok(Arg::Name(key, input.parse()?)),
            "response" => Ok(Arg::Response(key, Box::new(input.parse()?))),
            other => Err(syn::Error::new(
                key.span(),
                format!("unknown argument `{other}`; expected `name` or `response`"),
            )),
        }
    }
}
