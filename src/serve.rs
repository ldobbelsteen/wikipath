use crate::database::{Database, PageId};
use axum::{
    body::{self, Full},
    extract::{Extension, Query},
    http::{header, HeaderValue, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router, Server,
};
use error_chain::error_chain;
use futures::try_join;
use include_dir::{include_dir, Dir};
use serde::Deserialize;
use std::{collections::HashMap, fs, net::SocketAddr, sync::Arc};

static WEB: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/web/dist");

error_chain! {}

struct Databases {
    map: HashMap<String, Database>,
}

impl Databases {
    fn open(dir: &str) -> std::io::Result<Self> {
        let mut map = HashMap::new();
        for entry in fs::read_dir(dir)? {
            if let Ok(entry) = entry {
                let path = entry.path();
                if path.is_dir() {
                    match Database::open(path) {
                        Ok(database) => {
                            map.insert(database.lang_code.to_string(), database);
                        }
                        Err(err) => eprintln!("ERROR: {}", err),
                    }
                }
            }
        }
        Ok(Self { map: map })
    }

    fn select(&self, lang_code: &str) -> Option<&Database> {
        self.map.get(lang_code)
    }

    fn list(&self) -> Vec<&Database> {
        self.map.values().collect()
    }
}

pub async fn serve(database_dir: &str, listening_port: u16) {
    let databases = Databases::open(database_dir).unwrap_or_else(|e| {
        eprintln!("FATAL: {}", e);
        std::process::exit(1);
    });

    async fn list_databases(Extension(databases): Extension<Arc<Databases>>) -> Response {
        let list = databases.list();
        Json(list).into_response()
    }

    #[derive(Deserialize)]
    struct ShortestPathsQuery {
        language: String,
        source: PageId,
        target: PageId,
    }

    async fn shortest_paths(
        Extension(databases): Extension<Arc<Databases>>,
        query: Query<ShortestPathsQuery>,
    ) -> Response {
        if let Some(database) = databases.select(&query.language) {
            if let Ok(paths) = database.get_shortest_paths(query.source, query.target) {
                Json(paths).into_response()
            } else {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "unexpected database error",
                )
                    .into_response()
            }
        } else {
            return (StatusCode::NOT_FOUND, "language not supported").into_response();
        }
    }

    async fn web_files(uri: Uri) -> Response {
        let path = {
            let raw_path = uri.path();
            if raw_path == "/" {
                "index.html"
            } else {
                raw_path.trim_start_matches("/")
            }
        };
        let mime_type = mime_guess::from_path(path).first_or_text_plain();

        match WEB.get_file(path) {
            None => StatusCode::NOT_FOUND.into_response(),
            Some(file) => Response::builder()
                .status(StatusCode::OK)
                .header(
                    header::CONTENT_TYPE,
                    HeaderValue::from_str(mime_type.as_ref()).unwrap(),
                )
                .body(body::boxed(Full::from(file.contents())))
                .unwrap(),
        }
    }

    let api = Router::new()
        .route("/api/list_databases", get(list_databases))
        .route("/api/shortest_paths", get(shortest_paths))
        .layer(Extension(Arc::new(databases)))
        .fallback(web_files);

    let service = api.into_make_service();
    let ipv4 = SocketAddr::from(([0, 0, 0, 0], listening_port));
    let ipv6 = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], listening_port));
    let server_ipv4 = Server::try_bind(&ipv4).and_then(|s| Ok(s.serve(service.clone())));
    let server_ipv6 = Server::try_bind(&ipv6).and_then(|s| Ok(s.serve(service.clone())));

    match (server_ipv4, server_ipv6) {
        (Ok(ipv4), Ok(ipv6)) => {
            if let Err(e) = try_join!(ipv4, ipv6) {
                eprintln!("FATAL: {}", e);
                std::process::exit(1);
            }
        }
        (Ok(ipv4), Err(ipv6)) => {
            eprintln!("ERROR: could not bind to IPv6 address: {}", ipv6);
            if let Err(e) = ipv4.await {
                eprintln!("FATAL: {}", e);
                std::process::exit(1);
            }
        }
        (Err(ipv4), Ok(ipv6)) => {
            eprintln!("ERROR: could not bind to IPv4 address: {}", ipv4);
            if let Err(e) = ipv6.await {
                eprintln!("FATAL: {}", e);
                std::process::exit(1);
            }
        }
        (Err(ipv4), Err(ipv6)) => {
            eprintln!("FATAL: could not bind to IPv4 nor IPv6: {} {}", ipv4, ipv6);
            std::process::exit(1);
        }
    };
}
