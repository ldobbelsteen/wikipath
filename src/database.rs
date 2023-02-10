use crate::{
    dump::{self, Metadata},
    parse, progress,
};
use error_chain::error_chain;
use hashbrown::{HashMap, HashSet};
use indicatif::MultiProgress;
use serde::Serialize;
use std::sync::Arc;
use std::{collections::VecDeque, mem, path::PathBuf};

error_chain! {
    foreign_links {
        Sled(sled::Error);
        Bincode(bincode::Error);
        Parse(parse::Error);
    }

    errors {
        ShortestPathsAlgorithm(msg: String) {
            display("unexpected error in shortest paths algorithm: {}", msg)
        }
        MissingMetadata(path: PathBuf) {
            display("database at '{}' is missing metadata", path.display())
        }
        InvalidBytes(msg: String) {
            display("invalid bytes encountered: {}", msg)
        }
    }
}

pub static INCOMING_TREE_NAME: &str = "incoming";
pub static OUTGOING_TREE_NAME: &str = "outgoing";
pub static REDIRECTS_TREE_NAME: &str = "redirects";
pub static METADATA_KEY: &str = "metadata";

pub type PageId = u32;

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
pub struct Database {
    root: sled::Db,
    incoming: sled::Tree,
    outgoing: sled::Tree,
    redirects: sled::Tree,
    pub metadata: Metadata,
}

impl Database {
    pub fn open(path: &PathBuf, cache_capacity: u64) -> Result<Self> {
        let root = sled::Config::default()
            .cache_capacity(cache_capacity)
            .path(path)
            .open()?;

        let metadata = bincode::deserialize(
            &root
                .get(METADATA_KEY)?
                .ok_or_else(|| ErrorKind::MissingMetadata(path.clone()))?,
        )?;

        Ok(Self {
            incoming: root.open_tree(INCOMING_TREE_NAME)?,
            outgoing: root.open_tree(OUTGOING_TREE_NAME)?,
            redirects: root.open_tree(REDIRECTS_TREE_NAME)?,
            metadata,
            root,
        })
    }

    pub fn create(
        path: &PathBuf,
        dump: &dump::Dump,
        cache_capacity: u64,
        thread_count: usize,
    ) -> Result<Self> {
        let root = sled::Config::default()
            .cache_capacity(cache_capacity)
            .path(path)
            .create_new(true)
            .open()?;

        let db = Self {
            incoming: root.open_tree(INCOMING_TREE_NAME)?,
            outgoing: root.open_tree(OUTGOING_TREE_NAME)?,
            redirects: root.open_tree(REDIRECTS_TREE_NAME)?,
            metadata: dump.metadata.clone(),
            root,
        };

        let progress = MultiProgress::new();

        let step = progress.add(progress::spinner("Parsing page dump"));
        let titles = Arc::new(dump.parse_page_dump_file(thread_count, progress.clone())?);
        step.finish();

        let step = progress.add(progress::spinner("Parsing redirects dump"));
        let redirects =
            Arc::new(dump.parse_redir_dump_file(thread_count, titles.clone(), progress.clone())?);
        step.finish();

        let step = progress.add(progress::spinner("Parsing links dump"));
        let links =
            dump.parse_link_dump_file(thread_count, titles, redirects.clone(), progress.clone())?;
        step.finish();

        let step = progress.add(progress::spinner("Ingesting redirects into database"));
        let bar = progress.add(progress::unit(redirects.len() as u64));
        for (source, target) in redirects.as_ref() {
            db.redirects
                .insert(source.to_le_bytes(), &target.to_le_bytes())?;
            bar.inc(1);
        }
        bar.finish();
        step.finish();

        let step = progress.add(progress::spinner("Ingesting incoming links into database"));
        let bar = progress.add(progress::unit(links.incoming.len() as u64));
        for (target, sources) in links.incoming {
            db.incoming.insert(
                target.to_le_bytes(),
                sources
                    .iter()
                    .flat_map(|id| id.to_le_bytes())
                    .collect::<Vec<u8>>(),
            )?;
            bar.inc(1);
        }
        bar.finish();
        step.finish();

        let step = progress.add(progress::spinner("Ingesting outgoing links into database"));
        let bar = progress.add(progress::unit(links.outgoing.len() as u64));
        for (source, targets) in links.outgoing {
            db.outgoing.insert(
                source.to_le_bytes(),
                targets
                    .iter()
                    .flat_map(|id| id.to_le_bytes())
                    .collect::<Vec<u8>>(),
            )?;
            bar.inc(1);
        }
        bar.finish();
        step.finish();

        db.root
            .insert(METADATA_KEY, bincode::serialize(&db.metadata)?)?;

        Ok(db)
    }

    pub fn size(&self) -> u64 {
        self.root.size_on_disk().unwrap_or(0)
    }

    pub fn get_shortest_paths(&self, source: PageId, target: PageId) -> Result<Paths> {
        let (source, source_is_redirect) = {
            (self.get_redirect(source)?).map_or((source, false), |new_source| (new_source, true))
        };

        let (target, target_is_redirect) = {
            (self.get_redirect(target)?).map_or((target, false), |new_target| (new_target, true))
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

        while overlapping.is_empty() && !forward_queue.is_empty() && !backward_queue.is_empty() {
            let mut new_parents: HashMap<PageId, HashSet<PageId>> = HashMap::new();
            if forward_queue.len() < backward_queue.len() {
                for _ in 0..forward_queue.len() {
                    let page = forward_queue.pop_front().ok_or_else(|| {
                        ErrorKind::ShortestPathsAlgorithm("empty forward queue".to_string())
                    })?;
                    for out in self.get_links(page, false)? {
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
                    let page = backward_queue.pop_front().ok_or_else(|| {
                        ErrorKind::ShortestPathsAlgorithm("empty backward queue".to_string())
                    })?;
                    for inc in self.get_links(page, true)? {
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
                if !direct_parents.is_empty() {
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
                    return Ok(*counts.get(&page).ok_or_else(|| {
                        ErrorKind::ShortestPathsAlgorithm("unmemoized path count".to_string())
                    })?);
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
            source,
            source_is_redirect,
            target,
            target_is_redirect,
            links,
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

    fn get_redirect(&self, id: PageId) -> Result<Option<PageId>> {
        if let Some(data) = self.redirects.get(PageId::to_le_bytes(id))? {
            let data: [u8; mem::size_of::<PageId>()] = data
                .as_ref()
                .try_into()
                .map_err(|_| ErrorKind::InvalidBytes("invalid redirect length".into()))?;
            Ok(Some(PageId::from_le_bytes(data)))
        } else {
            Ok(None)
        }
    }

    fn get_links(&self, id: PageId, incoming: bool) -> Result<Vec<PageId>> {
        let tree = if incoming {
            &self.incoming
        } else {
            &self.outgoing
        };

        if let Some(data) = tree.get(PageId::to_le_bytes(id))? {
            if data.len() % mem::size_of::<PageId>() == 0 {
                Ok(data
                    .chunks(mem::size_of::<PageId>())
                    .map(|bs| PageId::from_le_bytes(bs.try_into().unwrap()))
                    .collect())
            } else {
                Err(ErrorKind::InvalidBytes("invalid links length".into()).into())
            }
        } else {
            Ok(Default::default())
        }
    }
}
