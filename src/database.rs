use crate::{
    dump::{self, Dump},
    progress::{multi_progress, step_progress},
};
use bincode::{deserialize, serialize};
use error_chain::error_chain;
use hashbrown::{HashMap, HashSet};
use serde::Serialize;
use std::{
    collections::VecDeque,
    ops::Deref,
    path::{Path, PathBuf},
};

error_chain! {
    foreign_links {
        Io(std::io::Error);
        Sled(sled::Error);
        Dump(dump::Error);
        Bincode(bincode::Error);
    }

    errors {
        ShortestPathsAlgo(msg: String) {
            description("error in shortest paths algorithm")
            display("unexpected error in shortest paths algorithm: {}", msg)
        }
        InvalidDatabasePath(path: PathBuf, reason: String) {
            description("invalid database path")
            display("database path '{}' is invalid: {}", path.display(), reason)
        }
    }
}

pub type PageId = u32;
pub type Titles = HashMap<String, PageId>;
pub type Redirects = HashMap<PageId, PageId>;

#[derive(Debug, Default)]
pub struct Links {
    pub incoming: HashMap<PageId, HashSet<PageId>>,
    pub outgoing: HashMap<PageId, HashSet<PageId>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Paths {
    source: PageId,
    source_is_redir: bool,
    target: PageId,
    target_is_redir: bool,
    lang_code: String,
    links: HashMap<PageId, HashSet<PageId>>,
    path_length: u32,
    path_count: u32,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Database {
    #[serde(skip)]
    incoming: sled::Tree,
    #[serde(skip)]
    outgoing: sled::Tree,
    #[serde(skip)]
    redirects: sled::Tree,
    pub lang_code: String,
    pub dump_date: String,
}

impl Database {
    pub fn open(path: PathBuf) -> Result<Self> {
        let database = sled::Config::default()
            .path(&path)
            .cache_capacity(4 * 1024 * 1024 * 1024) // 4GB
            .mode(sled::Mode::LowSpace)
            .use_compression(true)
            .open()?;
        let name = path.as_path().file_name().and_then(|s| s.to_str()).ok_or(
            ErrorKind::InvalidDatabasePath(path.clone(), "invalid string".into()),
        )?;
        let mut parts = name.split("-");
        let lang_code = parts.next().ok_or(ErrorKind::InvalidDatabasePath(
            path.clone(),
            "no language code included".into(),
        ))?;
        let dump_date = parts.next().ok_or(ErrorKind::InvalidDatabasePath(
            path.clone(),
            "no dump date included".into(),
        ))?;

        Ok(Database {
            incoming: database.open_tree("incoming")?,
            outgoing: database.open_tree("outgoing")?,
            redirects: database.open_tree("redirects")?,
            lang_code: lang_code.to_string(),
            dump_date: dump_date.to_string(),
        })
    }

    pub fn build(dir: &str, dump: &Dump) -> Result<PathBuf> {
        let step = step_progress("Initializing database".into());
        let name = format!("{}-{}", dump.lang_code, dump.date);
        let tmp_suffix = "-tmp";
        let path = Path::new(dir).join(name.clone());
        if path.exists() {
            println!("Database already exists, skipping...");
            return Ok(path);
        }
        let tmp_path = Path::new(dir).join(name + tmp_suffix);
        if tmp_path.exists() {
            std::fs::remove_dir_all(tmp_path.clone())?;
        }

        let db = Self::open(tmp_path.clone())?;
        step.finish();

        let progress = multi_progress();
        let step = progress.add(step_progress("Parsing page dump".into()));
        let titles = dump.parse_page_dump_file(progress)?;
        step.finish();

        let progress = multi_progress();
        let step = progress.add(step_progress("Parsing redirects dump".into()));
        let redirects = dump.parse_redir_dump_file(&titles, progress)?;
        step.finish();

        let progress = multi_progress();
        let step = progress.add(step_progress("Parsing links dump".into()));
        let links = dump.parse_link_dump_file(&titles, &redirects, progress)?;
        step.finish();

        let step = step_progress("Ingesting redirects into database".into());
        for (source, target) in &redirects {
            db.redirects
                .insert(serialize(source)?, serialize(target)?)?;
        }
        step.finish();

        let step = step_progress("Ingesting incoming links into database".into());
        for (target, sources) in &links.incoming {
            db.incoming
                .insert(serialize(target)?, serialize(sources)?)?;
        }
        step.finish();

        let step = step_progress("Ingesting outgoing links into database".into());
        for (source, targets) in &links.outgoing {
            db.outgoing
                .insert(serialize(source)?, serialize(targets)?)?;
        }
        step.finish();

        drop(db);
        std::fs::rename(&tmp_path, &path)?;

        Ok(path)
    }

    fn get_redirect(&self, id: PageId) -> Result<Option<PageId>> {
        let data = self.redirects.get(serialize(&id)?)?;
        if let Some(data) = data {
            Ok(Some(deserialize(data.deref())?))
        } else {
            Ok(None)
        }
    }

    fn get_incoming(&self, id: PageId) -> Result<HashSet<PageId>> {
        let data = self.incoming.get(serialize(&id)?)?;
        if let Some(data) = data {
            Ok(deserialize(data.deref())?)
        } else {
            Ok(Default::default())
        }
    }

    fn get_outgoing(&self, id: PageId) -> Result<HashSet<PageId>> {
        let data = self.outgoing.get(serialize(&id)?)?;
        if let Some(data) = data {
            Ok(deserialize(data.deref())?)
        } else {
            Ok(Default::default())
        }
    }

    pub fn get_shortest_paths(&self, source: PageId, target: PageId) -> Result<Paths> {
        let (source, source_is_redir) = {
            if let Some(new_source) = self.get_redirect(source)? {
                (new_source, true)
            } else {
                (source, false)
            }
        };

        let (target, target_is_redir) = {
            if let Some(new_target) = self.get_redirect(target)? {
                (new_target, true)
            } else {
                (target, false)
            }
        };

        let mut forward_parents: HashMap<PageId, HashSet<PageId>> =
            HashMap::from([(source, HashSet::new())]);
        let mut backward_parents: HashMap<PageId, HashSet<PageId>> =
            HashMap::from([(target, HashSet::new())]);
        let mut forward_queue = VecDeque::from([source]);
        let mut backward_queue = VecDeque::from([target]);
        let mut overlapping: HashSet<PageId> = HashSet::new();
        let mut forward_depth = 0;
        let mut backward_depth = 0;

        if source == target {
            overlapping.insert(source);
        }

        while overlapping.len() == 0 && forward_queue.len() > 0 && backward_queue.len() > 0 {
            let mut new_parents: HashMap<PageId, HashSet<PageId>> = HashMap::new();
            if forward_queue.len() < backward_queue.len() {
                for _ in 0..forward_queue.len() {
                    let page = forward_queue
                        .pop_front()
                        .ok_or(ErrorKind::ShortestPathsAlgo(
                            "empty forward queue".to_string(),
                        ))?;
                    for out in self.get_outgoing(page)? {
                        if !forward_parents.contains_key(&out) {
                            forward_queue.push_back(out);
                            if let Some(set) = new_parents.get_mut(&out) {
                                set.insert(page);
                            } else {
                                new_parents.insert(out, HashSet::from([page]));
                            }
                            if backward_parents.contains_key(&out) {
                                overlapping.insert(out);
                            }
                        }
                    }
                }
                for (child, parents) in new_parents {
                    for parent in parents {
                        forward_parents
                            .entry(child)
                            .and_modify(|parents| {
                                parents.insert(parent);
                            })
                            .or_insert(HashSet::from([parent]));
                    }
                }
                forward_depth += 1;
            } else {
                for _ in 0..backward_queue.len() {
                    let page = backward_queue
                        .pop_front()
                        .ok_or(ErrorKind::ShortestPathsAlgo(
                            "empty backward queue".to_string(),
                        ))?;
                    for inc in self.get_incoming(page)? {
                        if !backward_parents.contains_key(&inc) {
                            backward_queue.push_back(inc);
                            if let Some(parents) = new_parents.get_mut(&inc) {
                                parents.insert(page);
                            } else {
                                new_parents.insert(inc, HashSet::from([page]));
                            }
                            if forward_parents.contains_key(&inc) {
                                overlapping.insert(inc);
                            }
                        }
                    }
                }
                for (child, parents) in new_parents {
                    for parent in parents {
                        backward_parents
                            .entry(child)
                            .and_modify(|parents| {
                                parents.insert(parent);
                            })
                            .or_insert(HashSet::from([parent]));
                    }
                }
                backward_depth += 1;
            }
        }

        fn extract_paths(
            page: PageId,
            counts: &mut HashMap<PageId, u32>,
            forward: bool,
            parents: &HashMap<PageId, HashSet<PageId>>,
            links: &mut HashMap<PageId, HashSet<PageId>>,
        ) -> Result<u32> {
            if let Some(direct_parents) = parents.get(&page) {
                if direct_parents.len() > 0 {
                    let mut occurred: HashSet<PageId> = HashSet::new();
                    for parent in direct_parents {
                        if occurred.insert(*parent) {
                            if forward {
                                links
                                    .entry(page)
                                    .and_modify(|links| {
                                        links.insert(*parent);
                                    })
                                    .or_insert(HashSet::from([*parent]));
                            } else {
                                links
                                    .entry(*parent)
                                    .and_modify(|links| {
                                        links.insert(page);
                                    })
                                    .or_insert(HashSet::from([page]));
                            }
                            let parent_count = {
                                let memoized = *counts.get(parent).unwrap_or(&0);
                                if memoized == 0 {
                                    extract_paths(*parent, counts, forward, parents, links)
                                } else {
                                    Ok(memoized)
                                }
                            }?;
                            *counts.entry(page).or_default() += parent_count;
                        }
                    }
                    return Ok(*counts.get(&page).ok_or(ErrorKind::ShortestPathsAlgo(
                        "unmemoized path count".to_string(),
                    ))?);
                }
            }
            Ok(1)
        }

        let mut total_path_count = 0;
        let mut forward_path_counts: HashMap<PageId, u32> = HashMap::new();
        let mut backward_path_counts: HashMap<PageId, u32> = HashMap::new();
        let mut links: HashMap<PageId, HashSet<PageId>> = HashMap::new();
        for overlap in overlapping {
            let forward_path_count = extract_paths(
                overlap,
                &mut forward_path_counts,
                true,
                &backward_parents,
                &mut links,
            )?;
            let backward_path_count = extract_paths(
                overlap,
                &mut backward_path_counts,
                false,
                &forward_parents,
                &mut links,
            )?;
            total_path_count += forward_path_count * backward_path_count;
        }

        Ok(Paths {
            source: source,
            source_is_redir: source_is_redir,
            target: target,
            target_is_redir: target_is_redir,
            lang_code: self.lang_code.to_string(),
            links: links,
            path_length: if total_path_count != 0 {
                forward_depth + backward_depth
            } else {
                0
            },
            path_count: total_path_count,
        })
    }
}
