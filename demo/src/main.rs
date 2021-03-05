use actix_web::web::Json;
use actix_web::{get, middleware, web, App, HttpServer, Responder};
use anyhow::format_err;
use chrono::Utc;
use demo::{FetchUsers, StoreResult, TestContext, TestWorkflowFactory, WaitAndFilterUsers};
use evento::admin::Admin;
use evento::api::Evento;
use evento::registry::{SimpleOperationExecutor, SimpleWorkflowRegistry};
use evento::runners::AsyncWorkflowRunner;
use evento::state::{InMemoryStore, State};
use evento::{
    ExternalInputKey, Operation, WorkflowData, WorkflowError, WorkflowFactory, WorkflowStatus,
};
use serde_json::Number;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use uuid::Uuid;

pub type AppFacade = web::Data<Evento>;

//TODO:add endpoint for completing external wait

async fn index(facade: AppFacade) -> Result<Json<WorkflowData>, WorkflowError> {
    let workflow = facade.create_workflow(
        "TestWorkflow".to_string(),
        "test".to_string(),
        serde_json::to_value(TestContext {
            keyword: "test".to_string(),
            age_filter: 20,
        })
        .unwrap(),
    )?;
    Ok(Json(workflow))
}

async fn complete_external(
    web::Path((code,)): web::Path<(ExternalInputKey,)>,
    facade: AppFacade,
) -> Result<Json<()>, WorkflowError> {
    facade.complete_external(code, serde_json::Value::Number(Number::from(30)))?;
    Ok(Json(()))
}

fn create_facade() -> Evento {
    let state = State {
        store: Arc::new(InMemoryStore::default()),
    };
    let mut factories: HashMap<String, Arc<dyn WorkflowFactory>> = HashMap::new();
    factories.insert("TestWorkflow".to_string(), Arc::new(TestWorkflowFactory {}));
    let registry = Arc::new(SimpleWorkflowRegistry::new(factories));
    let mut operation_map: HashMap<String, Arc<dyn Operation>> = HashMap::new();
    operation_map.insert("FetchUsers".to_string(), Arc::new(FetchUsers {}));
    operation_map.insert("StoreResult".to_string(), Arc::new(StoreResult {}));
    operation_map.insert(
        "WaitAndFilterUsers".to_string(),
        Arc::new(WaitAndFilterUsers {}),
    );
    let executor = Arc::new(SimpleOperationExecutor::new(operation_map));
    let runner = Arc::new(AsyncWorkflowRunner::new(state.clone(), registry.clone()));
    Evento::new(state.clone(), registry.clone(), executor.clone(), runner)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::try_init();

    let facade = create_facade();

    Admin::new(facade.clone(), 8082).await.unwrap();

    let facade = AppFacade::new(facade);
    HttpServer::new(move || {
        App::new()
            .wrap(middleware::Logger::default())
            .app_data(facade.clone())
            .service(web::resource("/complete/{id}").route(web::get().to(complete_external)))
            .service(web::resource("/").route(web::get().to(index)))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
