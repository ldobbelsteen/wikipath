use crate::{
    database::{Database, PageId},
    dump::Metadata,
};
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
use hashbrown::HashMap;
use include_dir::{include_dir, Dir};
use serde::Deserialize;
use std::{fs, net::SocketAddr, path::Path, sync::Arc};

static WEB: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/web/dist");

error_chain! {
    foreign_links {
        Hyper(hyper::Error);
    }

    errors {
        SocketBindError(port: u16) {
            display("could bind to neither ipv4 nor ipv6 on port {}", port)
        }
    }
}

type Databases = Arc<HashMap<String, Database>>;

pub async fn serve(databases_dir: &Path, listening_port: u16, cache_capacity: u64) -> Result<()> {
    let databases: Databases = {
        let mut result = HashMap::new();
        if let Ok(entries) = fs::read_dir(databases_dir) {
            entries.for_each(|entry| {
                if let Ok(entry) = entry {
                    let path = entry.path();
                    if path.is_dir() {
                        println!("[INFO] opening {}...", path.display());
                        match Database::open(&path, cache_capacity) {
                            Ok(database) => {
                                result
                                    .insert(database.metadata.language_code.to_string(), database);
                            }
                            Err(err) => eprintln!("[WARNING] skipping database: {}", err),
                        }
                    }
                }
            });
        }
        Arc::new(result)
    };

    async fn list_databases(Extension(databases): Extension<Databases>) -> Response {
        let list = databases
            .values()
            .map(|db| &db.metadata)
            .collect::<Vec<&Metadata>>();
        Json(list).into_response()
    }

    #[derive(Deserialize)]
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
                    eprintln!("[ERROR] {}", e);
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
                    .body(body::boxed(Full::from(file.contents())))
                    .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
            },
        )
    }

    let api = Router::new()
        .route("/api/list_databases", get(list_databases))
        .route("/api/shortest_paths", get(shortest_paths))
        .layer(Extension(databases))
        .fallback(web_files);

    let service = api.into_make_service();
    let ipv4 = SocketAddr::from(([0, 0, 0, 0], listening_port));
    let ipv6 = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], listening_port));
    let server_ipv4 = Server::try_bind(&ipv4).map(|s| s.serve(service.clone()));
    let server_ipv6 = Server::try_bind(&ipv6).map(|s| s.serve(service.clone()));

    println!("[INFO] listening on port {}...", listening_port);
    match (server_ipv4, server_ipv6) {
        (Ok(ipv4), Ok(ipv6)) => {
            try_join!(ipv4, ipv6)?;
        }
        (Ok(ipv4), Err(ipv6)) => {
            eprintln!("[WARNING] could not bind to IPv6 address: {}", ipv6);
            ipv4.await?;
        }
        (Err(ipv4), Ok(ipv6)) => {
            eprintln!("[WARNING] could not bind to IPv4 address: {}", ipv4);
            ipv6.await?;
        }
        (Err(_), Err(_)) => {
            return Err(ErrorKind::SocketBindError(listening_port).into());
        }
    };

    Ok(())
}
