use crate::database::{Database, Metadata, Mode, PageId};
use anyhow::Result;
use axum::{
    extract::{Extension, Query},
    http::{header::CACHE_CONTROL, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use std::{
    collections::HashMap,
    fs::{self},
    path::Path,
    sync::Arc,
    time::Duration,
};
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::{services::ServeDir, set_header::SetResponseHeaderLayer, timeout::TimeoutLayer};

#[derive(Debug)]
struct DatabaseSet {
    inner: HashMap<Metadata, Database>,
    json: Json<Vec<Metadata>>,
}

impl DatabaseSet {
    fn load(databases_dir: &Path) -> Result<Self> {
        let mut inner = HashMap::new();

        // Load all databases from the given directory.
        for entry in fs::read_dir(databases_dir)? {
            let path = entry?.path();

            let mode = Mode::Serve;
            match Database::get_metadata(&path, &mode) {
                Ok(metadata) => match Database::open(&path, mode) {
                    Ok(database) => {
                        inner.insert(metadata, database);
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

        log::info!("database set loading complete");
        let json = Self::to_json_internal(&inner);
        Ok(Self { inner, json })
    }

    /// Convert this set of databases to a list of their metadata as JSON response.
    fn to_json(&self) -> Json<Vec<Metadata>> {
        self.json.clone()
    }

    /// Get a database by its metadata.
    fn get_by_metadata(&self, metadata: &Metadata) -> Option<&Database> {
        self.inner.get(metadata)
    }

    /// Convert the inner hashmap to a list of metadata as JSON response sorted by language code.
    fn to_json_internal(inner: &HashMap<Metadata, Database>) -> Json<Vec<Metadata>> {
        let mut list = inner
            .values()
            .map(|db| db.metadata.clone())
            .collect::<Vec<_>>();

        // Sort alphabetically by language code.
        list.sort_by(|a, b| a.language_code.cmp(&b.language_code));

        Json(list)
    }
}

async fn list_databases_handler(Extension(databases): Extension<Arc<DatabaseSet>>) -> Response {
    databases.to_json().into_response()
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
    Extension(databases): Extension<Arc<DatabaseSet>>,
    query: Query<ShortestPathsQuery>,
) -> Response {
    let query = query.0;

    let metadata = Metadata {
        language_code: query.language_code,
        date_code: query.date_code,
    };

    let result = tokio::task::spawn_blocking(move || -> Response {
        match databases.get_by_metadata(&metadata) {
            None => StatusCode::NOT_FOUND.into_response(),
            Some(db) => match db.get_shortest_paths(query.source, query.target) {
                Ok(paths) => Json(paths).into_response(),
                Err(e) => {
                    log::error!("failed getting shortest paths: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR.into_response()
                }
            },
        }
    })
    .await;

    result.unwrap_or_else(|e| {
        log::error!("getting shortest paths task join error: {e}");
        StatusCode::INTERNAL_SERVER_ERROR.into_response()
    })
}

pub async fn serve(databases_dir: &Path, web_dir: &Path, listening_port: u16) -> Result<()> {
    let databases = Arc::new(DatabaseSet::load(databases_dir)?);

    let router = Router::new()
        .route(
            "/api/list_databases",
            get(list_databases_handler).layer(Extension(databases.clone())),
        )
        .route(
            "/api/shortest_paths",
            get(shortest_paths_handler).layer(
                ServiceBuilder::new()
                    .layer(TimeoutLayer::new(Duration::from_secs(10))) // timeout after 10 seconds to prevent long-running searches
                    .layer(Extension(databases.clone())), // give access to the databases
            ),
        )
        .nest_service(
            "/assets", // treat frontend "assets" files separately, since they have hashed filenames
            ServiceBuilder::new()
                .layer(SetResponseHeaderLayer::overriding(
                    CACHE_CONTROL,
                    HeaderValue::from_str("max-age=31536000")?, // cached for a year
                ))
                .service(ServeDir::new(Path::join(Path::new(web_dir), "assets"))),
        )
        .fallback_service(ServeDir::new(web_dir)); // serve frontend files as fallback

    log::info!("listening on http://localhost:{listening_port}");
    let listener = TcpListener::bind(format!(":::{listening_port}")).await?;
    axum::serve(listener, router).await?;
    Ok(())
}
