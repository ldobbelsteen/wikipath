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
use notify::{
    event::{CreateKind, ModifyKind, RemoveKind, RenameMode},
    EventKind, RecursiveMode, Watcher,
};
use serde::Deserialize;
use std::{
    collections::HashMap,
    fs,
    net::SocketAddr,
    path::Path,
    sync::{mpsc, Arc, RwLock, RwLockReadGuard},
};

static WEB: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/web/dist");

error_chain! {
    foreign_links {
        Io(std::io::Error);
        Notify(notify::Error);
        Hyper(hyper::Error);
    }

    errors {
        CouldNotBind(port: u16) {
            description("could not bind to port")
            display("could not bind to either ipv4 nor ipv6 on port {}", port)
        }
    }
}

#[derive(Clone)]
struct Databases {
    databases: Arc<RwLock<HashMap<String, Database>>>,
}

impl Databases {
    fn new(dir: &str) -> Result<Self> {
        fn open(dir: &str) -> Result<HashMap<String, Database>> {
            let mut result = HashMap::new();
            for entry in fs::read_dir(dir)? {
                if let Ok(entry) = entry {
                    let path = entry.path();
                    if path.is_dir() {
                        match Database::open(path) {
                            Ok(database) => {
                                result.insert(database.lang_code.to_string(), database);
                            }
                            Err(err) => eprintln!("ERROR: {}", err),
                        }
                    }
                }
            }
            Ok(result)
        }

        let result = Self {
            databases: Arc::new(RwLock::new(open(dir)?)),
        };

        let (tx, rx) = mpsc::channel();
        let mut watcher = notify::recommended_watcher(tx)?;
        watcher.watch(Path::new(&dir), RecursiveMode::NonRecursive)?;

        let databases = result.databases.clone();
        let dir = dir.to_string();

        std::thread::spawn(move || {
            watcher.configure(notify::Config::default()).unwrap();
            for msg in rx {
                match msg {
                    Err(e) => eprintln!("ERROR: {}", e),
                    Ok(event) => {
                        if event.kind == EventKind::Create(CreateKind::Folder)
                            || event.kind == EventKind::Remove(RemoveKind::Folder)
                            || event.kind == EventKind::Modify(ModifyKind::Name(RenameMode::From))
                        {
                            println!("INFO: database file change detected, reloading databases...");
                            let mut lock = databases.write().unwrap();
                            lock.drain().for_each(|db| drop(db));
                            *lock = open(&dir).unwrap_or_default();
                        }
                    }
                }
            }
        });

        Ok(result)
    }

    fn get(&self) -> RwLockReadGuard<HashMap<String, Database>> {
        self.databases.read().unwrap()
    }
}

pub async fn serve(database_dir: &str, listening_port: u16) -> Result<()> {
    let databases = Databases::new(database_dir)?;

    async fn list_databases(Extension(databases): Extension<Databases>) -> Response {
        let databases = databases.get();
        let list = databases.values().collect::<Vec<&Database>>();
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
        let databases = databases.get();
        if let Some(database) = databases.get(&query.language) {
            match database.get_shortest_paths(query.source, query.target) {
                Ok(paths) => Json(paths).into_response(),
                Err(e) => {
                    let response = (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "unexpected database error",
                    );
                    eprintln!("ERROR: {}", e);
                    response.into_response()
                }
            }
        } else {
            let response = (StatusCode::NOT_FOUND, "language not supported");
            return response.into_response();
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
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR.into_response()),
        }
    }

    let api = Router::new()
        .route("/api/list_databases", get(list_databases))
        .route("/api/shortest_paths", get(shortest_paths))
        .layer(Extension(databases))
        .fallback(web_files);

    let service = api.into_make_service();
    let ipv4 = SocketAddr::from(([0, 0, 0, 0], listening_port));
    let ipv6 = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], listening_port));
    let server_ipv4 = Server::try_bind(&ipv4).and_then(|s| Ok(s.serve(service.clone())));
    let server_ipv6 = Server::try_bind(&ipv6).and_then(|s| Ok(s.serve(service.clone())));

    match (server_ipv4, server_ipv6) {
        (Ok(ipv4), Ok(ipv6)) => {
            try_join!(ipv4, ipv6)?;
        }
        (Ok(ipv4), Err(ipv6)) => {
            eprintln!("ERROR: could not bind to IPv6 address: {}", ipv6);
            ipv4.await?;
        }
        (Err(ipv4), Ok(ipv6)) => {
            eprintln!("ERROR: could not bind to IPv4 address: {}", ipv4);
            ipv6.await?;
        }
        (Err(_), Err(_)) => {
            return Err(ErrorKind::CouldNotBind(listening_port).into());
        }
    };

    Ok(())
}
