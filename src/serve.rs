use anyhow::Result;
use axum::{
    extract::{Extension, Query},
    http::{
        header::{self, CACHE_CONTROL},
        HeaderValue, StatusCode, Uri,
    },
    response::{Html, IntoResponse, Response},
    routing::get,
    Json, Router,
};
use rust_embed::RustEmbed;
use serde::Deserialize;
use std::{
    collections::HashMap,
    fs::{self},
    path::Path,
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::{net::TcpListener, task::JoinHandle};
use tower_http::{set_header::SetResponseHeaderLayer, timeout::TimeoutLayer};

use crate::database::{Database, Metadata, Mode, PageId};

#[derive(RustEmbed)]
#[folder = "web/dist/"]
struct FrontendAssets;

type Databases = Arc<RwLock<HashMap<Metadata, Database>>>;

fn not_found() -> Response {
    (StatusCode::NOT_FOUND, "404").into_response()
}

async fn list_databases_handler(Extension(databases): Extension<Databases>) -> Response {
    let guard = databases.read().unwrap();
    let list = guard.values().map(|db| &db.metadata).collect::<Vec<_>>();
    Json(list).into_response()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ShortestPathsQuery {
    language_code: String,
    date_code: String,
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
        date_code: query.date_code,
    };
    let guard = databases.read().unwrap();
    if let Some(database) = guard.get(&metadata) {
        match database.get_shortest_paths(query.source, query.target) {
            Ok(paths) => Json(paths).into_response(),
            Err(e) => {
                let response = (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "unexpected database error",
                );
                log::error!("failed getting shortest paths: {e}");
                response.into_response()
            }
        }
    } else {
        not_found()
    }
}

async fn frontend_asset_handler(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');

    if path.is_empty() || path == "index.html" {
        return match FrontendAssets::get("index.html") {
            Some(content) => Html(content.data).into_response(),
            None => not_found(),
        };
    }

    match FrontendAssets::get(path) {
        Some(content) => {
            let mime = mime_guess::from_path(path).first_or_octet_stream();
            ([(header::CONTENT_TYPE, mime.as_ref())], content.data).into_response()
        }
        None => not_found(),
    }
}

pub async fn serve(databases_dir: &Path, listening_port: u16) -> Result<()> {
    let databases: Databases = Arc::new(RwLock::new(HashMap::new()));

    let router = Router::new()
        .route("/api/list_databases", get(list_databases_handler))
        .route(
            "/api/shortest_paths",
            get(shortest_paths_handler).layer(SetResponseHeaderLayer::overriding(
                CACHE_CONTROL,
                HeaderValue::from_str("max-age=3600")?, // cached for an hour
            )),
        )
        .layer(Extension(databases.clone()))
        .route(
            "/assets/*f",
            get(frontend_asset_handler).layer(SetResponseHeaderLayer::overriding(
                CACHE_CONTROL,
                HeaderValue::from_str("max-age=31536000")?, // cached for a year, since assets are hashed
            )),
        )
        .fallback(frontend_asset_handler)
        .layer(TimeoutLayer::new(Duration::from_secs(10)));

    log::info!("listening on http://localhost:{listening_port}");
    let listener = TcpListener::bind(format!(":::{listening_port}")).await?;
    let listener_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
        axum::serve(listener, router).await?;
        Ok(())
    });

    let databases_dir = databases_dir.to_path_buf();
    let loader_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
        for entry in fs::read_dir(databases_dir)? {
            let path = entry?.path();

            let mode = Mode::Serve;
            match Database::get_metadata(&path, &mode) {
                Ok(metadata) => match Database::open(&path, mode) {
                    Ok(database) => {
                        databases.write().unwrap().insert(metadata, database);
                        log::info!("opened database '{}'", path.display());
                    }
                    Err(e) => {
                        log::warn!("skipping database '{}': {}", path.display(), e);
                    }
                },
                Err(e) => {
                    log::debug!("silently skipping database '{}': {}", path.display(), e);
                }
            }
        }
        log::info!("loaded all valid databases");
        Ok(())
    });

    // Return the first error that may occur between the loader and listener.
    match tokio::try_join!(loader_handle, listener_handle)? {
        (Err(err), _) | (_, Err(err)) => Err(err),
        (Ok(()), Ok(())) => Ok(()),
    }
}
