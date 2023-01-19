use crate::dump::{self, Dump};
use bincode::{deserialize, serialize};
use error_chain::error_chain;
use regex::Regex;
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    ops::Deref,
    path::{Path, PathBuf},
};

error_chain! {
    foreign_links {
        Sled(sled::Error);
        Dump(dump::Error);
        Encoding(bincode::Error);
    }

    errors {
        ShortestPaths(msg: String) {
            description("error in shortest paths algorithm")
            display("unexpected error in shortest pzaths algorithm: {}", msg)
        }
        AlreadyExists(path: String) {
            description("database already exists")
            display("database to build already exists: {}", path)
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
pub struct Graph {
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
    pub fn build(output_dir: &str, dump: &Dump) -> Result<PathBuf> {
        let name = format!("{}-{}", dump.lang_code, dump.date);
        let path = Path::new(output_dir).join(name);
        if path.exists() {
            return Err(ErrorKind::AlreadyExists(path.display().to_string()).into());
        }
        let db = Self::open(&path)?;

        let titles = dump.parse_page_dump_file()?;
        let redirects = dump.parse_redir_dump_file(&titles)?; // TODO: cleanup
        let links = dump.parse_link_dump_file(&titles, &redirects)?;

        for (source, target) in &redirects {
            db.redirects
                .insert(serialize(source)?, serialize(target)?)?;
        }

        for (target, sources) in &links.incoming {
            db.incoming
                .insert(serialize(target)?, serialize(sources)?)?;
        }

        for (source, targets) in &links.outgoing {
            db.outgoing
                .insert(serialize(source)?, serialize(targets)?)?;
        }

        Ok(path)
    }

    pub fn open(dir: &Path) -> Result<Self> {
        let name_err = sled::Error::Unsupported("invalid database name".to_string());

        let name = dir.file_name().ok_or(name_err.clone())?;
        let caps = Regex::new(r"(.+)-([0-9]{8})")
            .unwrap()
            .captures(name.to_str().ok_or(name_err.clone())?)
            .ok_or(name_err.clone())?;
        let lang_code = caps.get(1).ok_or(name_err.clone())?.as_str();
        let date = caps.get(2).ok_or(name_err.clone())?.as_str();

        let db = sled::open(dir)?;

        Ok(Database {
            incoming: db.open_tree("incoming")?,
            outgoing: db.open_tree("outgoing")?,
            redirects: db.open_tree("redirects")?,
            lang_code: lang_code.to_string(),
            dump_date: date.to_string(),
        })
    }

    fn get_redirect(&self, id: PageId) -> Result<Option<PageId>> {
        let data = self.redirects.get(serialize(&id)?)?;
        if let Some(data) = data {
            Ok(Some(deserialize(data.deref())?))
        } else {
            Ok(None)
        }
    }

    fn get_incoming(&self, id: PageId) -> Result<Option<HashSet<PageId>>> {
        let data = self.incoming.get(serialize(&id)?)?;
        if let Some(data) = data {
            Ok(Some(deserialize(data.deref())?))
        } else {
            Ok(None)
        }
    }

    fn get_outgoing(&self, id: PageId) -> Result<Option<HashSet<PageId>>> {
        let data = self.outgoing.get(serialize(&id)?)?;
        if let Some(data) = data {
            Ok(Some(deserialize(data.deref())?))
        } else {
            Ok(None)
        }
    }

    pub fn get_shortest_paths(&self, source: PageId, target: PageId) -> Result<Graph> {
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
                        .ok_or(ErrorKind::ShortestPaths("empty forward queue".to_string()))?;
                    if let Some(outgoing) = self.get_outgoing(page)? {
                        for out in outgoing {
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
                        .ok_or(ErrorKind::ShortestPaths("empty backward queue".to_string()))?;
                    if let Some(incoming) = self.get_incoming(page)? {
                        for inc in incoming {
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
                    return Ok(*counts.get(&page).ok_or(ErrorKind::ShortestPaths(
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

        Ok(Graph {
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
