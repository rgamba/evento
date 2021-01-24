#![feature(proc_macro_diagnostic)]
extern crate proc_macro;
extern crate syn;
#[macro_use]
extern crate quote;

use proc_macro::TokenStream;
use syn::spanned::Spanned;
use syn::{AttributeArgs, Fields};

#[proc_macro_attribute]
pub fn workflow(metadata: TokenStream, input: TokenStream) -> TokenStream {
    let _args = syn::parse_macro_input!(metadata as AttributeArgs);
    let item = syn::parse_macro_input!(input as syn::ItemStruct);
    let context_type = get_context_type(&item);
    let struct_name = &item.ident;
    let mut fields = Vec::new();
    if let Fields::Named(ref f) = item.fields {
        fields.extend(f.named.iter());
    }
    let fields2 = fields.clone();
    let fields_names: Vec<syn::Ident> = fields.iter().map(|f| f.ident.clone().unwrap()).collect();
    let workflow_name = format!("{}", struct_name);
    // Recreate the workflow struct and construct the `new` method
    let workflow_def = quote! {
        struct #struct_name {
            #( #fields, )*
            __id: ::evento_api::WorkflowId,
            __correlation_id: ::evento_api::CorrelationId,
            __operation_results: Vec<OperationResult>,
            __iteration_counter_map: ::std::sync::Mutex<::std::collections::HashMap<String, usize>>,
        }
        impl #struct_name {
            pub fn new(id: ::evento_api::WorkflowId, correlation_id: ::evento_api::CorrelationId, operation_results: Vec<::evento_api::OperationResult>, #( #fields2 ),*) -> Self
            where #context_type: ::serde::Serialize + Clone
            {
                Self {
                    __id: id,
                    __correlation_id: correlation_id,
                    __operation_results: operation_results,
                    #( #fields_names, )*
                    __iteration_counter_map: ::std::sync::Mutex::new(::std::collections::HashMap::new()),
                }
            }
            fn convert_context(context: &serde_json::Value) -> Result<#context_type> {
                ::serde_json::from_value(context.clone()).map_err(|e| {
                    ::anyhow::format_err!("Unable to deserialize workflow context: {:?}", e)
                })
            }
            fn context(&self) -> #context_type {
                self.context.clone()
            }
            fn increase_iteration_counter(&self, operation_name: &String) {
                let mut guard = self.__iteration_counter_map.lock().unwrap();
                let count = {
                    if let Some(c) = guard.get(operation_name.as_str()) {
                        c.clone()
                    } else {
                        0
                    }
                };
                guard.insert(operation_name.clone(), count + 1);
            }
            pub fn id(&self) -> ::uuid::Uuid {
                self.__id.clone()
            }
            pub fn name(&self) -> String {
                String::from(#workflow_name)
            }
            fn iteration_counter(&self, operation_name: &String) -> usize {
                let mut guard = self.__iteration_counter_map.lock().unwrap();
                guard
                    .get(operation_name.as_str())
                    .map_or(0, |v| v.clone())
            }
            fn find_execution_result(
                &self,
                operation_name: String,
                iteration: usize,
            ) -> Option<OperationResult> {
                self.__operation_results
                    .clone()
                    .into_iter()
                    .find(|r| r.operation_name == operation_name && r.iteration == iteration)
            }
        }
    };
    // Create the workflow factory struct and impl
    let factory_name = format!("{}Factory", struct_name);
    let factory_ident = syn::Ident::new(factory_name.as_str(), item.span());
    let factory_def = quote! {
        pub struct #factory_ident;
        impl ::evento_api::WorkflowFactory for #factory_ident {
            fn create(&self, id: ::evento_api::WorkflowId, correlation_id: ::evento_api::CorrelationId, context: ::evento_api::WorkflowContext, execution_results: Vec<::evento_api::OperationResult>) -> Box<dyn ::evento_api::Workflow> {
                Box::new(#struct_name::new(
                    id,
                    correlation_id,
                    execution_results,
                    #struct_name::convert_context(&context).unwrap(),
                ))
            }
        }
    };
    let output = quote! {
        #workflow_def
        #factory_def
    };
    println!(">>>> OUTPUT: {}", output.to_string());
    output.into()
}

fn get_context_type(struct_item: &syn::ItemStruct) -> &syn::Type {
    let mut result = Option::None;
    if let Fields::Named(ref f) = struct_item.fields {
        for field in &f.named {
            let ident = field.ident.as_ref().unwrap();
            if ident == "context" {
                result = Some(&field.ty);
            } else {
                ident
                    .span()
                    .unstable()
                    .error("This field is not allowed in workflow struct")
                    .emit();
                panic!("Workflow structure is invalid")
            }
        }
    }
    match result {
        None => {
            struct_item
                .span()
                .unstable()
                .error("Workflow must have 'context' field")
                .emit();
            panic!("Workflow structure is invalid")
        }
        Some(r) => r,
    }
}
