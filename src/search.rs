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
    /// Get the shortest paths between two pages.
    #[allow(clippy::too_many_lines)]
    pub fn get_shortest_paths(&self, source: PageId, target: PageId) -> Result<Paths> {
        let txn = self.read_txn()?;

        // Follow any redirects and report whether they were redirects.
        let (source, source_is_redirect) = self
            .get_redirect(&txn, source)?
            .map_or((source, false), |new_source| (new_source, true));
        let (target, target_is_redirect) = self
            .get_redirect(&txn, target)?
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
        // If there is overlap, it means we have found the shortest path(s).
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

            // Take the direction that has the shortest queue (for efficiency).
            if forward_queue.len() < backward_queue.len() {
                // We pop the front of the queue as many times as the queue is long. The queue may grow
                // during the loop, but we only consider the pages that were in the queue at the start.
                for _ in 0..forward_queue.len() {
                    // Forward queue cannot be empty by the while-loop guard.
                    let source = forward_queue.pop_front().unwrap();

                    // Consider all outgoing links of the source page.
                    for target in self.get_outgoing_links(&txn, source)? {
                        // Only consider if it has not been visited yet.
                        if !forward_predecessors.contains_key(&target) {
                            forward_queue.push_back(target);

                            // Mark the target as a predecessor of the source.
                            new_predecessors.entry(target).or_default().insert(source);

                            // If the target has been visited by the backward BFS, we have found overlap.
                            if backward_predecessors.contains_key(&target) {
                                overlapping.insert(target);
                            }
                        }
                    }
                }

                // Insert newly found predecessors into the predecessors map. This is done after
                // the loop, because we only want to mark them as visited after this iteration.
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
                // We pop the front of the queue as many times as the queue is long. The queue may grow
                // during the loop, but we only consider the pages that were in the queue at the start.
                for _ in 0..backward_queue.len() {
                    // Backward queue cannot be empty by the while-loop guard.
                    let target = backward_queue.pop_front().unwrap();

                    // Consider all incoming links of the target page.
                    for source in self.get_incoming_links(&txn, target)? {
                        // Only consider if it has not been visited yet.
                        if !backward_predecessors.contains_key(&source) {
                            backward_queue.push_back(source);

                            // Mark the source as a predecessor of the target.
                            new_predecessors.entry(source).or_default().insert(target);

                            // If the source has been visited by the forward BFS, we have found overlap.
                            if forward_predecessors.contains_key(&source) {
                                overlapping.insert(source);
                            }
                        }
                    }
                }

                // Insert newly found predecessors into the predecessors map. This is done after
                // the loop, because we only want to mark them as visited after this iteration.
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

        // Release the read transaction.
        txn.commit()?;

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
