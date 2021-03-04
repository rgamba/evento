use crate::api::WorkflowFacade;
use crate::state::WorkflowFilter;
use crate::{OperationName, OperationResult, WorkflowData, WorkflowError};
use actix_cors::Cors;
use actix_web::web::{Json, Path};
use actix_web::{middleware, web, App, HttpResponse, HttpServer};
use anyhow::format_err;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::task;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ListResult<T> {
    pub data: Vec<T>,
    pub links: Links,
    pub count: u64,
    pub offset: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Link {
    href: String,
}

impl From<String> for Link {
    fn from(href: String) -> Self {
        Self { href }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Links {
    pub next: Option<Link>,
    pub prev: Option<Link>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ReplayRequest {
    pub operation_name: OperationName,
    pub iteration: u64,
}

#[allow(dead_code)]
pub struct Admin {
    facade: web::Data<WorkflowFacade>,
}

impl Admin {
    pub async fn new(facade: WorkflowFacade, port: u32) -> Result<()> {
        let data = web::Data::new(facade);

        task::spawn(async move {
            let data_clone = data.clone();
            loop {
                let data = data_clone.clone();
                log::info!("Starting admin http server...");
                let server = HttpServer::new(move || {
                    let cors = Cors::default().allow_any_origin();
                    App::new()
                        .wrap(cors)
                        .wrap(middleware::Logger::default())
                        .app_data(data.clone())
                        .service(
                            web::scope("/workflows")
                                .route("/{workflow_id}/history", web::get().to(view_workflow_history))
                                .route("/{workflow_id}/traces", web::get().to(view_workflow_traces))
                                .route("/{workflow_id}/replay", web::post().to(replay_workflow))
                                .route("/{workflow_id}/retry", web::get().to(retry_workflow))
                                .route("/{workflow_id}", web::get().to(view_workflow))
                                .route("", web::get().to(list_workflows))
                                //.route("/{workflow_id}/cancel", web::post().to(cancel_workflow))
                                //.route("/{workflow_id}/replay", web::post().to(replay_workflow))
                                // .route(
                                //     "/complete_external/{external_key}",
                                //     web::post().to(complete_external),
                                // ),
                        )
                        .service(web::scope("/").route("", web::get().to(index)))
                })
                .bind(format!("127.0.0.1:{}", port))
                .unwrap()
                .run();
                let join = task::spawn(async move {
                    server.await.ok();
                });
                if let Err(e) = join.await {
                    log::error!("Admin thread panic unexpectedly. error={:?}", e);
                    tokio::time::delay_for(Duration::from_secs(1)).await;
                }
            }
        });

        Ok(())
    }
}

async fn index(_facade: web::Data<WorkflowFacade>) -> HttpResponse {
    let html = include_str!("./admin_files/index.html");
    HttpResponse::Ok().content_type("text/html").body(html)
}

async fn list_workflows(
    facade: web::Data<WorkflowFacade>,
) -> Result<Json<ListResult<WorkflowData>>, WorkflowError> {
    let filter = WorkflowFilter {
        name: None,
        created_from: None,
        created_to: None,
        status: None,
        offset: None,
        limit: None,
    };
    let workflows = facade.get_workflows(filter)?;
    Ok(Json(ListResult {
        data: workflows,
        links: Links {
            next: None,
            prev: None,
        },
        count: 0,
        offset: 0,
    }))
}

async fn view_workflow(
    workflow_id: Path<Uuid>,
    facade: web::Data<WorkflowFacade>,
) -> Result<Json<WorkflowData>, WorkflowError> {
    let wf = facade
        .get_workflow_by_id(workflow_id.into_inner())?
        .ok_or_else(|| format_err!("Invalid workflow if provided"))?;
    Ok(Json(wf))
}

async fn retry_workflow(
    workflow_id: Path<Uuid>,
    facade: web::Data<WorkflowFacade>,
) -> Result<Json<()>, WorkflowError> {
    facade.retry(workflow_id.into_inner())?;
    Ok(Json(()))
}

async fn view_workflow_history(
    workflow_id: Path<Uuid>,
    facade: web::Data<WorkflowFacade>,
) -> Result<Json<ListResult<OperationResult>>, WorkflowError> {
    let results = facade.get_operation_results(workflow_id.into_inner())?;
    let result = ListResult {
        data: results,
        links: Links {
            next: None,
            prev: None,
        },
        count: 0,
        offset: 0,
    };
    Ok(Json(result))
}

async fn view_workflow_traces(
    workflow_id: Path<Uuid>,
    facade: web::Data<WorkflowFacade>,
) -> Result<Json<ListResult<OperationResult>>, WorkflowError> {
    let results = facade.get_operation_execution_traces(workflow_id.into_inner())?;
    let result = ListResult {
        data: results,
        links: Links {
            next: None,
            prev: None,
        },
        count: 0,
        offset: 0,
    };
    Ok(Json(result))
}

async fn replay_workflow(
    workflow_id: Path<Uuid>,
    request: web::Json<ReplayRequest>,
    facade: web::Data<WorkflowFacade>,
) -> Result<Json<()>, WorkflowError> {
    let workflow = facade
        .get_workflow_by_id(workflow_id.into_inner())?
        .ok_or_else(|| format_err!("Invalid workflow id"))?;
    facade.replay(
        workflow.id,
        request.operation_name.clone(),
        request.iteration as usize,
    )?;
    Ok(Json(()))
}
