use anyhow::{bail, Result};
use axum::{
    extract::{Extension, Query},
    handler::HandlerWithoutStateExt,
    http::{header::CACHE_CONTROL, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use log::{error, info, warn};
use serde::Deserialize;
use std::{collections::HashMap, fs, path::Path, sync::Arc, time::Duration};
use tokio::net::TcpListener;
use tower_http::{services::ServeDir, set_header::SetResponseHeaderLayer, timeout::TimeoutLayer};
use wp::{Database, Metadata, PageId};

type Databases = Arc<HashMap<Metadata, Database>>;

async fn list_databases_handler(Extension(databases): Extension<Databases>) -> Response {
    let list = databases
        .values()
        .map(|db| &db.metadata)
        .collect::<Vec<_>>();
    Json(list).into_response()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ShortestPathsQuery {
    language_code: String,
    dump_date: String,
    source: PageId,
    target: PageId,
}

async fn shortest_paths_handler(
    Extension(databases): Extension<Databases>,
    query: Query<ShortestPathsQuery>,
) -> Response {
    let query = query.0;
    let metadata = Metadata {
        language_code: query.language_code,
        dump_date: query.dump_date,
    };
    if let Some(database) = databases.get(&metadata) {
        match database.get_shortest_paths(query.source, query.target) {
            Ok(paths) => Json(paths).into_response(),
            Err(e) => {
                let response = (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "unexpected database error",
                );
                error!("failed getting shortest paths: {e}...");
                response.into_response()
            }
        }
    } else {
        (StatusCode::NOT_FOUND, "database not found").into_response()
    }
}

pub async fn serve(databases_dir: &Path, web_dir: &Path, listening_port: u16) -> Result<()> {
    let databases: Databases = {
        let mut result = HashMap::new();
        for entry in fs::read_dir(databases_dir)? {
            let path = entry?.path();
            if let Some(ext) = path.extension() {
                if ext == "redb" {
                    info!("opening database '{}'...", path.display());
                    match Database::open(&path) {
                        Ok(database) => {
                            result.insert(database.metadata.clone(), database);
                        }
                        Err(err) => {
                            warn!("skipping database '{}': {}", path.display(), err);
                        }
                    }
                }
            }
        }
        if result.is_empty() {
            bail!("no databases found");
        }
        Arc::new(result)
    };

    let router = Router::new()
        .route(
            "/api/list_databases",
            get(list_databases_handler).layer(SetResponseHeaderLayer::overriding(
                CACHE_CONTROL,
                HeaderValue::from_str("max-age=300")?, // cached for 5 minutes
            )),
        )
        .route(
            "/api/shortest_paths",
            get(shortest_paths_handler).layer(SetResponseHeaderLayer::overriding(
                CACHE_CONTROL,
                HeaderValue::from_str("max-age=3600")?, // cached for an hour
            )),
        )
        .layer(Extension(databases))
        .nest(
            "/assets",
            Router::new()
                .fallback_service(ServeDir::new(web_dir.join("assets")))
                .layer(SetResponseHeaderLayer::overriding(
                    CACHE_CONTROL,
                    HeaderValue::from_str("max-age=31536000")?, // cached for a year
                )),
        )
        .fallback_service(
            ServeDir::new(web_dir)
                .not_found_service((StatusCode::NOT_FOUND, "asset not found").into_service()),
        )
        .layer(TimeoutLayer::new(Duration::from_secs(10)));

    let listener = TcpListener::bind(format!(":::{listening_port}")).await?;
    info!("listening on port {listening_port}...");
    axum::serve(listener, router).await?;

    Ok(())
}
