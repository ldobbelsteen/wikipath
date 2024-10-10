use crate::database::{Database, Metadata, Mode, PageId};
use anyhow::Result;
use axum::{
    extract::{Extension, Query},
    http::{header::CACHE_CONTROL, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use notify_debouncer_mini::{new_debouncer, notify::RecursiveMode, DebounceEventResult};
use serde::Deserialize;
use std::{
    collections::HashMap,
    fs::{self},
    path::Path,
    sync::{Arc, RwLock},
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
        let mut inner: HashMap<Metadata, Database> = HashMap::new();

        // Load all databases from the given directory.
        for entry in fs::read_dir(databases_dir)? {
            let path = entry?.path();

            match Database::get_metadata(&path) {
                Ok(md) => match Database::open(&path, Mode::Serve) {
                    Ok(db) => {
                        // If any older databases were opened, close them again.
                        while let Some(md2) = inner.keys().find(|&m| m.is_older(&md)) {
                            log::info!("closing older database '{}'", md2.to_name());
                            let md2 = md2.clone();
                            inner.remove(&md2);
                        }

                        // Check if there are no newer databases.
                        let newest = !inner.keys().any(|m| m.is_newer(&md));

                        if newest {
                            log::info!("opened database '{}'", md.to_name());
                            inner.insert(md, db);
                        } else {
                            log::info!("skipping older database '{}'", md.to_name());
                        }
                    }
                    Err(e) => {
                        log::warn!("skipping database '{}': {}", md.to_name(), e);
                    }
                },
                Err(e) => {
                    log::debug!("skipping non-database path '{}': {}", path.display(), e);
                }
            }
        }

        log::info!("finished loading databases");
        let json = Self::to_json_internal(&inner);
        Ok(Self { inner, json })
    }

    fn empty() -> Self {
        Self {
            inner: HashMap::new(),
            json: Json(Vec::new()),
        }
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

async fn list_databases_handler(
    Extension(databases): Extension<Arc<RwLock<DatabaseSet>>>,
) -> Response {
    databases.read().unwrap().to_json().into_response()
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
    Extension(databases): Extension<Arc<RwLock<DatabaseSet>>>,
    query: Query<ShortestPathsQuery>,
) -> Response {
    let query = query.0;

    let metadata = Metadata {
        language_code: query.language_code,
        date_code: query.date_code,
    };

    let result = tokio::task::spawn_blocking(move || -> Response {
        let databases = databases.read().unwrap();
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
    let databases = Arc::new(RwLock::new(DatabaseSet::load(databases_dir)?));

    let databases_clone = databases.clone();
    let databases_dir_clone = databases_dir.to_path_buf();
    let mut debouncer =
        new_debouncer(
            Duration::from_secs(5),
            move |res: DebounceEventResult| match res {
                Ok(events) => {
                    if !events.is_empty() {
                        log::info!("detected changes in databases directory, reloading");
                        let mut guard = databases_clone.write().unwrap();

                        // Replace current with empty to drop currently opened databases.
                        *guard = DatabaseSet::empty();

                        // Load new databases and replace the empty one again.
                        match DatabaseSet::load(&databases_dir_clone) {
                            Ok(new) => {
                                *guard = new;
                            }
                            Err(e) => {
                                log::error!("failed to reload databases: {e}");
                            }
                        }
                    }
                }
                Err(e) => {
                    log::error!("debouncer error: {e}");
                }
            },
        )?;

    // Watch for changes in the databases directory.
    debouncer
        .watcher()
        .watch(databases_dir, RecursiveMode::NonRecursive)?;

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
