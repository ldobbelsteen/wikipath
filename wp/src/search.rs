use crate::database::{Database, PageId};
use anyhow::Result;
use serde::Serialize;
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Paths<'a> {
    source: PageId,
    source_is_redirect: bool,
    target: PageId,
    target_is_redirect: bool,
    links: HashMap<PageId, HashSet<PageId>>,
    language_code: &'a str,
    date_code: &'a str,
    length: u32,
    count: u32,
}

impl Database {
    #[allow(clippy::too_many_lines)]
    /// Get the shortest paths between two pages.
    pub fn get_shortest_paths(&self, source: PageId, target: PageId) -> Result<Paths> {
        let txn = self.begin_read()?;
        let tables = txn.begin_serve()?;

        // Follow any redirects and report whether they were redirects.
        let (source, source_is_redirect) = tables
            .redirect(source)?
            .map_or((source, false), |new_source| (new_source, true));
        let (target, target_is_redirect) = tables
            .redirect(target)?
            .map_or((target, false), |new_target| (new_target, true));

        // We run BFS in both directions, so we store two queues.
        let mut forward_queue = VecDeque::from([source]);
        let mut backward_queue = VecDeque::from([target]);

        // We store the predecessors of each page on the shortest path to the source/target, so that
        // we can extract the paths later. Only the source and target can have an empty set.
        let mut forward_predecessors: HashMap<PageId, HashSet<PageId>> =
            HashMap::from([(source, HashSet::new())]);
        let mut backward_predecessors: HashMap<PageId, HashSet<PageId>> =
            HashMap::from([(target, HashSet::new())]);

        // The overlap between the currently visited pages in the forward and backward directions.
        let mut overlapping: HashSet<PageId> = HashSet::new();

        // Keep track of the depth of the BFS of both directions.
        let mut forward_depth = 0;
        let mut backward_depth = 0;

        // Skip BFS if the source and target are the same.
        if source == target {
            overlapping.insert(source);
        }

        // Take BFS steps until either the two directions meet, or all possible paths are depleted.
        while overlapping.is_empty() && !forward_queue.is_empty() && !backward_queue.is_empty() {
            let mut new_predecessors: HashMap<PageId, HashSet<PageId>> = HashMap::new();

            // Take the direction that has the shortest queue.
            if forward_queue.len() < backward_queue.len() {
                for _ in 0..forward_queue.len() {
                    let page = forward_queue.pop_front().unwrap(); // forward queue cannot be empty by the while-loop guard
                    for out in tables.outgoing_links(page)? {
                        // Only consider if it has not been visited yet.
                        if !forward_predecessors.contains_key(&out) {
                            forward_queue.push_back(out);
                            new_predecessors.entry(out).or_default().insert(page);
                            if backward_predecessors.contains_key(&out) {
                                overlapping.insert(out);
                            }
                        }
                    }
                }

                // Insert newly found predecessors into the predecessors map.
                for (child, predecessors) in new_predecessors {
                    for predecessor in predecessors {
                        forward_predecessors
                            .entry(child)
                            .or_default()
                            .insert(predecessor);
                    }
                }

                // Increment search depth.
                forward_depth += 1;
            } else {
                for _ in 0..backward_queue.len() {
                    let page = backward_queue.pop_front().unwrap(); // backward queue cannot be empty by the while-loop guard
                    for inc in tables.incoming_links(page)? {
                        // Only consider if it has not been visited yet.
                        if !backward_predecessors.contains_key(&inc) {
                            backward_queue.push_back(inc);
                            new_predecessors.entry(inc).or_default().insert(page);
                            if forward_predecessors.contains_key(&inc) {
                                overlapping.insert(inc);
                            }
                        }
                    }
                }

                // Insert newly found predecessors into the predecessors map.
                for (child, predecessors) in new_predecessors {
                    for predecessor in predecessors {
                        backward_predecessors
                            .entry(child)
                            .or_default()
                            .insert(predecessor);
                    }
                }

                // Increment search depth.
                backward_depth += 1;
            }
        }

        // Extract the number of paths and links from the predecessor maps.
        let mut links: HashMap<PageId, HashSet<PageId>> = HashMap::new();
        let mut count = 0;
        let mut forward_memory = HashMap::new();
        let mut backward_memory = HashMap::new();
        for page in overlapping {
            let forward_count = count_paths(
                page,
                source,
                &forward_predecessors,
                &mut forward_memory,
                &mut |target, source| {
                    links.entry(source).or_default().insert(target);
                },
            );
            let backward_count = count_paths(
                page,
                target,
                &backward_predecessors,
                &mut backward_memory,
                &mut |source, target| {
                    links.entry(source).or_default().insert(target);
                },
            );
            count += forward_count * backward_count;
        }

        Ok(Paths {
            source,
            source_is_redirect,
            target,
            target_is_redirect,
            links,
            language_code: &self.metadata.language_code,
            date_code: &self.metadata.date_code,
            length: if count != 0 {
                forward_depth + backward_depth
            } else {
                0
            },
            count,
        })
    }
}

/// Count the number of paths from a source page to a target page. To avoid duplicate work, it uses
/// (and appends to) a memory which stores results. All traversed edges are reported to a function.
/// All paths are assumed to end up in the target, else it panics or has undefined behaviour.
fn count_paths(
    source: PageId,
    target: PageId,
    edges: &HashMap<PageId, HashSet<PageId>>,
    result_memory: &mut HashMap<PageId, u32>,
    report_edge: &mut impl FnMut(PageId, PageId),
) -> u32 {
    if source == target {
        return 1;
    }

    edges
        .get(&source)
        .unwrap()
        .iter()
        .map(|&subtarget| {
            report_edge(source, subtarget);
            if let Some(memoized) = result_memory.get(&subtarget) {
                *memoized
            } else {
                let result = count_paths(subtarget, target, edges, result_memory, report_edge);
                result_memory.insert(subtarget, result);
                result
            }
        })
        .sum()
}
