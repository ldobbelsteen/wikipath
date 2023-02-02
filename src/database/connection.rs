use super::builder::{
    PageId, INCOMING_FILENAME, INCOMING_INDEX_FILENAME, METADATA_FILENAME, OUTGOING_FILENAME,
    OUTGOING_INDEX_FILENAME, REDIRECTS_FILENAME,
};
use crate::dump::Metadata;
use bincode::deserialize_from;
use error_chain::error_chain;
use hashbrown::{HashMap, HashSet};
use serde::Serialize;
use std::{
    collections::VecDeque,
    fs::File,
    io::{Read, Seek, SeekFrom},
    mem::size_of,
    path::PathBuf,
    sync::{Arc, Mutex},
};

error_chain! {
    foreign_links {
        Io(std::io::Error);
        Bincode(bincode::Error);
    }

    errors {
        Algorithm(msg: String) {
            display("unexpected error in DFS algorithm: {}", msg)
        }
        MissingMetadata(path: PathBuf) {
            display("database at '{}' is missing metadata", path.display())
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Paths {
    source: PageId,
    source_is_redirect: bool,
    target: PageId,
    target_is_redirect: bool,
    links: HashMap<PageId, HashSet<PageId>>,
    language_code: String,
    dump_date: String,
    path_lengths: u32,
    path_count: u32,
}

#[derive(Debug)]
pub struct Connection {
    incoming: Arc<Mutex<File>>,
    incoming_index: HashMap<PageId, (u64, u64)>,
    outgoing: Arc<Mutex<File>>,
    outgoing_index: HashMap<PageId, (u64, u64)>,
    redirects: HashMap<PageId, PageId>,
    pub metadata: Metadata,
}

impl Connection {
    pub fn open(path: &PathBuf) -> Result<Self> {
        Ok(Self {
            incoming: Arc::new(Mutex::new(File::open(path.join(INCOMING_FILENAME))?)),
            incoming_index: deserialize_from(File::open(path.join(INCOMING_INDEX_FILENAME))?)?,
            outgoing: Arc::new(Mutex::new(File::open(path.join(OUTGOING_FILENAME))?)),
            outgoing_index: deserialize_from(File::open(path.join(OUTGOING_INDEX_FILENAME))?)?,
            redirects: deserialize_from(File::open(path.join(REDIRECTS_FILENAME))?)?,
            metadata: deserialize_from(File::open(path.join(METADATA_FILENAME))?)?,
        })
    }

    fn get_redirect(&self, id: PageId) -> Option<PageId> {
        self.redirects.get(&id).copied()
    }

    fn get_links(&self, incoming: bool, id: PageId) -> Result<Vec<PageId>> {
        let (file, index) = if incoming {
            (&self.incoming, &self.incoming_index)
        } else {
            (&self.outgoing, &self.outgoing_index)
        };

        if let Some(index) = index.get(&id) {
            let mut file = file.lock().unwrap();
            file.seek(SeekFrom::Start(index.0))?;
            let mut result = Vec::with_capacity(index.1 as usize);
            for _ in 0..index.1 {
                let mut buffer = [0; size_of::<PageId>()];
                file.read_exact(&mut buffer)?;
                result.push(PageId::from_le_bytes(buffer));
            }
            Ok(result)
        } else {
            Ok(Default::default())
        }
    }

    pub fn get_shortest_paths(&self, source: PageId, target: PageId) -> Result<Paths> {
        let (source, source_is_redirect) = {
            if let Some(new_source) = self.get_redirect(source) {
                (new_source, true)
            } else {
                (source, false)
            }
        };

        let (target, target_is_redirect) = {
            if let Some(new_target) = self.get_redirect(target) {
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
                        .ok_or(ErrorKind::Algorithm("empty forward queue".to_string()))?;
                    for out in self.get_links(false, page)? {
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
                        .ok_or(ErrorKind::Algorithm("empty backward queue".to_string()))?;
                    for inc in self.get_links(true, page)? {
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
                    return Ok(*counts
                        .get(&page)
                        .ok_or(ErrorKind::Algorithm("unmemoized path count".to_string()))?);
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
            source_is_redirect,
            target: target,
            target_is_redirect: target_is_redirect,
            links: links,
            language_code: self.metadata.language_code.clone(),
            dump_date: self.metadata.dump_date.clone(),
            path_lengths: if total_path_count != 0 {
                forward_depth + backward_depth
            } else {
                0
            },
            path_count: total_path_count,
        })
    }
}
