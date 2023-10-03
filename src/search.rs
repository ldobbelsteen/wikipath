use crate::database::{Database, PageId};
use anyhow::{anyhow, Result};
use serde::Serialize;
use std::collections::{HashMap, HashSet, VecDeque};

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

impl Database {
    /// Get the shortest paths between two pages.
    pub fn get_shortest_paths(&self, source: PageId, target: PageId) -> Result<Paths> {
        let txn = self.begin_read()?;
        let tables = txn.open_serve()?;

        let (source, source_is_redirect) = tables
            .get_redirect(source)?
            .map_or((source, false), |new_source| (new_source, true));

        let (target, target_is_redirect) = tables
            .get_redirect(target)?
            .map_or((target, false), |new_target| (new_target, true));

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
                    let page = forward_queue
                        .pop_front()
                        .ok_or(anyhow!("empty forward queue in bfs"))?;
                    for out in tables.get_outgoing_links(page)?.0 {
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
                        .ok_or(anyhow!("empty backward queue in bfs"))?;
                    for inc in tables.get_incoming_links(page)?.0 {
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
                    return Ok(*counts
                        .get(&page)
                        .ok_or(anyhow!("unmemoized path count in path extraction"))?);
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
}
