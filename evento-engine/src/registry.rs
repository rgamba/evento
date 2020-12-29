use anyhow::{bail, format_err, Result};
use evento_api::{
    Operation, OperationInput, OperationResult, Workflow, WorkflowDeclaration, WorkflowError,
    WorkflowFactory, WorkflowStatus,
};
use libloading::Library;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::io;
use std::rc::Rc;
use std::sync::Arc;
use uuid::Uuid;

pub struct WorkflowFactoryProxy {
    factory: Box<dyn WorkflowFactory>,
    _lib: Rc<Library>,
}

impl WorkflowFactory for WorkflowFactoryProxy {
    fn create(&self, id: Uuid, execution_results: Vec<OperationResult>) -> Box<dyn Workflow> {
        self.factory.create(id, execution_results)
    }
}

pub struct OperationProxy {
    operation: Arc<dyn Operation>,
    _lib: Rc<Library>,
}

impl Operation for OperationProxy {
    fn new() -> Self {
        unimplemented!()
    }

    fn execute(&self, input: OperationInput) -> Result<OperationResult, WorkflowError> {
        self.operation.execute(input)
    }

    fn name(&self) -> &str {
        self.operation.name()
    }

    fn validate_input(input: &OperationInput) {
        unimplemented!()
    }
}

/// This workflow registrar is only used for it to be passed as an argument to the workflow's
/// `register` method for it to register itself and its passed as a mutable ref.
/// This is to add a layer of indirection and avoid having the workflow plugin registration have
/// direct access to our `ExternalWorkflows` struct.
struct WorkflowFactoryRegistrar {
    factories: HashMap<String, WorkflowFactoryProxy>,
    operations: HashMap<String, OperationProxy>,
    lib: Rc<Library>,
}

impl WorkflowFactoryRegistrar {
    fn new(lib: Rc<Library>) -> Self {
        Self {
            lib,
            factories: HashMap::default(),
            operations: HashMap::default(),
        }
    }
}

impl evento_api::WorkflowFactoryRegistrar for WorkflowFactoryRegistrar {
    fn register_factory(&mut self, workflow_name: String, factory: Box<dyn WorkflowFactory>) {
        let proxy = WorkflowFactoryProxy {
            factory,
            _lib: Rc::clone(&self.lib),
        };
        self.factories.insert(workflow_name, proxy);
    }

    fn register_operation_factory(&mut self, operation: Arc<dyn Operation>) {
        let proxy = OperationProxy {
            operation,
            _lib: Rc::clone(&self.lib),
        };
        self.operations.insert(operation.name().to_string(), proxy);
    }
}

/// This is the main component that will load worflows from external libraries
/// and maintain a table of loaded `WorkflowFactories` for the workflow creation.
pub struct ExternalWorkflows {
    factories: HashMap<String, WorkflowFactoryProxy>,
    libraries: Vec<Rc<Library>>,
    operations: HashMap<String, OperationProxy>,
}

impl Default for ExternalWorkflows {
    fn default() -> Self {
        Self {
            factories: HashMap::new(),
            libraries: Vec::new(),
            operations: HashMap::new(),
        }
    }
}

impl ExternalWorkflows {
    pub unsafe fn load(&mut self, library_path: &str) -> Result<()> {
        // load the library into memory
        let library = Rc::new(Library::new(library_path)?);

        // get a pointer to the plugin_declaration symbol.
        let decl = library
            .get::<*mut WorkflowDeclaration>(b"workflow_declaration\0")?
            .read();

        // version checks to prevent accidental ABI incompatibilities
        if decl.rustc_version != evento_api::RUSTC_VERSION
            || decl.core_version != evento_api::CORE_VERSION
        {
            bail!("Workflow version mismatch");
        }

        let mut registrar = WorkflowFactoryRegistrar::new(Rc::clone(&library));

        // Call the plugin declaration's register function so it can register itself.
        (decl.register)(&mut registrar);

        // add all loaded plugins to the workflow and operations map
        self.factories.extend(registrar.factories);
        self.operations.extend(registrar.operations);
        // and make sure ExternalFunctions keeps a reference to the library
        self.libraries.push(library);

        Ok(())
    }

    /// This is the main method to create workflows
    pub fn create_workflow(
        &self,
        workflow_name: &str,
        workflow_id: Uuid,
        execution_results: Vec<OperationResult>,
    ) -> Result<Box<dyn Workflow>> {
        Ok(self
            .factories
            .get(workflow_name)
            .ok_or_else(|| {
                format_err!(
                    "Workflow with the name '{}' not found in registry",
                    workflow_name
                )
            })?
            .create(workflow_id, execution_results))
    }

    pub fn get_operation(&self, operation_name: &str) -> Result<Arc<dyn Operation>> {
        Ok(self
            .operations
            .get(operation_name)
            .ok_or_else(|| {
                format_err!(
                    "Operation with name '{}' not found in registry",
                    operation_name
                )
            })?
            .operation
            .clone())
    }
}

#[cfg(test)]
mod tests {
    use crate::registry::ExternalWorkflows;
    use uuid::Uuid;

    #[test]
    fn test_works() {
        let mut external = ExternalWorkflows::default();

        unsafe {
            external.load("../target/debug/libdemo.dylib").unwrap();
        }

        let workflow = external
            .create_workflow("DemoWorkflow", Uuid::new_v4(), Vec::new())
            .unwrap();
        assert_eq!(workflow.name(), "DemoWorkflow".to_string());
    }
}
