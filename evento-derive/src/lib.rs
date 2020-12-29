#![feature(proc_macro_diagnostic)]
extern crate proc_macro;
extern crate syn;
#[macro_use]
extern crate quote;

use proc_macro::TokenStream;
use syn::spanned::Spanned;
use syn::Fields;

#[proc_macro_attribute]
pub fn workflow(_metadata: TokenStream, input: TokenStream) -> TokenStream {
    let item = syn::parse_macro_input!(input as syn::ItemStruct);
    let struct_name = &item.ident;
    let mut fields = Vec::new();
    if let Fields::Named(ref f) = item.fields {
        fields.extend(f.named.iter());
    }

    // Recreate the workflow struct
    let workflow_def = quote! {
        struct #struct_name {
            #( #fields, )*
            __id: ::uuid::Uuid,
            __operation_results: Vec<OperationResult>,
            __iteration_counter_map: ::std::collections::HashMap<String, core::sync::atomic::AtomicUsize>,
        }
    };
    // Create the WorkflowMetadata impl
    let workflow_name = format!("{}", struct_name);
    let metadata_def = quote! {
        impl ::evento_api::WorkflowMetadata for #struct_name {
            fn id(&self) -> Uuid {
                self.__id.clone()
            }
            fn name(&self) -> String {
                String::from(#workflow_name)
            }
            fn execution_results(&self) -> Vec<::evento_api::OperationResult> {
                self.__operation_results.clone()
            }
        }
    };
    // Create the workflow factory struct and impl
    let factory_name = format!("{}Factory", struct_name);
    let factory_ident = syn::Ident::new(factory_name.as_str(), item.span());
    let factory_def = quote! {
        pub struct #factory_ident;
        impl ::evento_api::WorkflowFactory for #factory_ident {
            fn create(&self, id: uuid::Uuid, execution_results: Vec<::evento_api::OperationResult>) -> Box<dyn ::evento_api::Workflow> {
                Box::new(#struct_name {
                    __id: id,
                    __operation_results: execution_results,
                    __iteration_counter_map: ::std::collections::HashMap::new(),
                })
            }
        }
    };
    // Create workflow export code
    let workflow_export = quote! {
        ::evento_api::export_workflow!(register);
        extern "C" fn register(registrar: &mut dyn ::evento_api::WorkflowFactoryRegistrar) {
            registrar.register_factory(
                String::from(#workflow_name),
                Box::new(#factory_ident),
            );
        }
    };
    let output = quote! {
        #workflow_def
        #metadata_def
        #factory_def
        #workflow_export
    };
    println!(">>>> OUTPUT: {}", output.to_string());
    output.into()
}
