use anyhow::{bail, Result};
use axum::{
    extract::{Extension, Query},
    handler::HandlerWithoutStateExt,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use log::{error, info, warn};
use serde::Deserialize;
use std::{collections::HashMap, fs, path::Path, sync::Arc};
use tokio::net::TcpListener;
use tower_http::services::ServeDir;
use wp::{Database, PageId};

type Databases = Arc<HashMap<String, Database>>;

pub async fn serve(databases_dir: &Path, web_dir: &Path, listening_port: u16) -> Result<()> {
    async fn list_databases(Extension(databases): Extension<Databases>) -> Response {
        let list = databases
            .values()
            .map(|db| &db.metadata)
            .collect::<Vec<_>>();
        Json(list).into_response()
    }

    #[derive(Debug, Deserialize)]
    struct ShortestPathsQuery {
        language: String,
        source: PageId,
        target: PageId,
    }

    async fn shortest_paths(
        Extension(databases): Extension<Databases>,
        query: Query<ShortestPathsQuery>,
    ) -> Response {
        if let Some(database) = databases.get(&query.language) {
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
            let response = (StatusCode::NOT_FOUND, "language not supported");
            response.into_response()
        }
    }

    let databases: Databases = {
        let mut result = HashMap::new();
        for entry in fs::read_dir(databases_dir)? {
            let path = entry?.path();
            if let Some(ext) = path.extension() {
                if ext == "redb" {
                    info!("opening database '{}'...", path.display());
                    match Database::open(&path) {
                        Ok(database) => {
                            result.insert(database.metadata.language_code.to_string(), database);
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
        .route("/api/list_databases", get(list_databases))
        .route("/api/shortest_paths", get(shortest_paths))
        .layer(Extension(databases))
        .fallback_service(
            ServeDir::new(web_dir)
                .not_found_service((StatusCode::NOT_FOUND, "asset not found").into_service()),
        );

    let listener = TcpListener::bind(format!(":::{listening_port}")).await?;
    info!("listening on port {listening_port}...");
    axum::serve(listener, router).await?;

    Ok(())
}
