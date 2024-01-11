use crate::database::{Database, PageId};
use anyhow::{bail, Result};
use axum::{
    body::Body,
    extract::{Extension, Query},
    http::{header, HeaderValue, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use include_dir::{include_dir, Dir};
use log::{error, info, warn};
use serde::Deserialize;
use std::{collections::HashMap, fs, path::Path, sync::Arc};
use tokio::net::TcpListener;

static WEB: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/web/dist");

type Databases = Arc<HashMap<String, Database>>;

pub async fn serve(databases_dir: &Path, listening_port: u16) -> Result<()> {
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

    async fn web_files(uri: Uri) -> Response {
        let path = {
            let raw_path = uri.path();
            if raw_path == "/" {
                "index.html"
            } else {
                raw_path.trim_start_matches('/')
            }
        };
        let mime_type = mime_guess::from_path(path).first_or_text_plain();
        WEB.get_file(path).map_or_else(
            || StatusCode::NOT_FOUND.into_response(),
            |file| {
                Response::builder()
                    .status(StatusCode::OK)
                    .header(
                        header::CONTENT_TYPE,
                        HeaderValue::from_str(mime_type.as_ref()).unwrap(),
                    )
                    .body(Body::from(file.contents()))
                    .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
            },
        )
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
        .fallback(web_files);

    let listener = TcpListener::bind(format!(":::{listening_port}")).await?;
    info!("listening on port {listening_port}...");
    axum::serve(listener, router).await?;

    Ok(())
}
