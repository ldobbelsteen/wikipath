use crate::{
    database::{LinkTargetId, PageId},
    dump::TableDumpFiles,
};
use anyhow::{anyhow, Result};
use flate2::read::GzDecoder;
use regex::bytes::Regex;
use std::{
    cmp::max,
    collections::{HashMap, HashSet},
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    str,
    sync::{Arc, Mutex},
    thread,
};

const CHUNK_SIZE_BYTES: usize = 1024 * 1024; // 1MB
const CHUNK_COUNT_MULTIPLIER: usize = 2;
const MAX_LOCAL_BATCH_SIZE: usize = 5_000;
const MAX_LINK_BATCH_SIZE: usize = 4_000_000;

/// Struct representing a chunk of bytes. The end field indicates the end of
/// the valid data in the data field.
#[derive(Debug)]
pub struct Chunk {
    data: Vec<u8>,
    end: usize,
}

/// Struct representing a batch of links stored in the incoming format.
#[derive(Debug, Default)]
pub struct LinkBatchIncoming {
    size: usize,
    incoming: HashMap<PageId, Vec<PageId>>,
}

impl LinkBatchIncoming {
    fn insert(&mut self, source: PageId, target: PageId) {
        if let Some(existing) = self.incoming.get_mut(&target) {
            existing.push(source);
        } else {
            self.incoming.insert(target, vec![source]);
        }

        self.size += 1;
    }

    pub fn drain(&mut self) -> impl Iterator<Item = (PageId, Vec<PageId>)> + '_ {
        self.size = 0;
        self.incoming.drain()
    }

    pub fn size(&self) -> usize {
        self.size
    }
}

impl TableDumpFiles {
    /// Parse the page table dump file and return a mapping from page titles to page ids.
    pub fn parse_page_table(&self, thread_count: usize) -> Result<HashMap<String, PageId>> {
        let result = Arc::new(Mutex::new(HashMap::new()));

        // Helper closure to ingest a batch of page titles into the result. Empties the batch.
        let ingest_batch = |batch: &mut Vec<(PageId, String)>| -> Result<()> {
            let mut result = result.lock().unwrap();
            for (id, title) in batch.drain(..) {
                if let Some(prev) = result.insert(title, id) {
                    if prev != id {
                        return Err(anyhow!(
                            "two page titles with same id found: {} & {}",
                            prev,
                            id,
                        ));
                    }
                }
            }
            Ok(())
        };

        let remaining_batches = threaded_sliding_regex_file(
            self.page.as_path(),
            &Regex::new(
                r"\(([0-9]{1,10}),0,'(.{0,255}?)',[01],[01],0.[0-9]{1,32}?,'[0-9]{14}',(?:'[0-9]{14}'|NULL),[0-9]{1,10},[0-9]{1,10},(?:'.{0,32}'|NULL),(?:'.{0,35}'|NULL)\)",
            )?, // https://www.mediawiki.org/wiki/Manual:Page_table
            1 + 10 + 4 + 255 + 8 + 32 + 2 + 14 + 3 + 14 + 2 + 10 + 1 + 10 + 2 + 32 + 3 + 35 + 2,
            |caps| -> Result<(PageId, String)> {
                let id = {
                    let m = caps.get(1).unwrap(); // Capture 1 always participates in the match
                    let str = str::from_utf8(m.as_bytes())?;
                    str.parse::<PageId>()?
                };

                let title = {
                    let m = caps.get(2).unwrap(); // Capture 2 always participates in the match
                    String::from_utf8(m.as_bytes().to_vec())?
                };

                Ok((id, title))
            },
            |batch: &mut Vec<(PageId, String)>, value| {
                batch.push(value);
                if batch.len() > MAX_LOCAL_BATCH_SIZE {
                    ingest_batch(batch)?;
                }
                Ok(())
            },
            thread_count,
        )?;

        log::debug!("ingesting remaining batches");
        for mut batch in remaining_batches {
            ingest_batch(&mut batch)?;
        }

        Ok(Arc::try_unwrap(result).unwrap().into_inner().unwrap())
    }

    /// Parse the redirect table dump file and return a mapping from source page ids to target page ids.
    pub fn parse_redirect_table(
        &self,
        title_to_id: &HashMap<String, PageId>,
        thread_count: usize,
    ) -> Result<HashMap<PageId, PageId>> {
        let result = Arc::new(Mutex::new(HashMap::new()));

        // Helper closure to ingest a batch of redirects into the result. Empties the batch.
        let ingest_batch = |batch: &mut Vec<(PageId, PageId)>| -> Result<()> {
            let mut result = result.lock().unwrap();
            for (source, target) in batch.drain(..) {
                if let Some(prev) = result.insert(source, target) {
                    if prev != target {
                        return Err(anyhow!(
                            "two redirect targets for same source found: {} & {}",
                            prev,
                            target
                        ));
                    }
                }
            }
            Ok(())
        };

        let remaining_batches = threaded_sliding_regex_file(
            self.redirect.as_path(),
            &Regex::new(
                r"\(([0-9]{1,10}),0,'(.{0,255}?)',(?:'.{0,32}'|NULL),(?:'.{0,255}'|NULL)\)",
            )?, // https://www.mediawiki.org/wiki/Manual:Redirect_table
            1 + 10 + 4 + 255 + 3 + 32 + 3 + 255 + 2,
            |caps| -> Result<(PageId, PageId)> {
                let source = {
                    let m = caps.get(1).unwrap(); // Capture 1 always participates in the match
                    let str = str::from_utf8(m.as_bytes())?;
                    str.parse::<PageId>()?
                };

                let target = {
                    let m = caps.get(2).unwrap(); // Capture 2 always participates in the match
                    let str = str::from_utf8(m.as_bytes())?;
                    if let Some(id) = title_to_id.get(str) {
                        *id
                    } else {
                        return Err(anyhow!("redirect target title '{}' not known", str));
                    }
                };

                if source == target {
                    return Err(anyhow!("self-redirect found for page id {}", source));
                }

                Ok((source, target))
            },
            |batch: &mut Vec<(PageId, PageId)>, value| {
                batch.push(value);
                if batch.len() > MAX_LOCAL_BATCH_SIZE {
                    ingest_batch(batch)?;
                }
                Ok(())
            },
            thread_count,
        )?;

        log::debug!("ingesting remaining batches");
        for mut batch in remaining_batches {
            ingest_batch(&mut batch)?;
        }

        Ok(Arc::try_unwrap(result).unwrap().into_inner().unwrap())
    }

    /// Parse the linktarget table dump file and return a mapping from link target ids to page ids.
    pub fn parse_linktarget_table(
        &self,
        title_to_id: &HashMap<String, PageId>,
        thread_count: usize,
    ) -> Result<HashMap<LinkTargetId, PageId>> {
        let result = Arc::new(Mutex::new(HashMap::new()));

        // Helper closure to ingest a batch of linktargets into the result. Empties the batch.
        let ingest_batch = |batch: &mut Vec<(LinkTargetId, PageId)>| -> Result<()> {
            let mut result = result.lock().unwrap();
            for (linktarget, target) in batch.drain(..) {
                if let Some(prev) = result.insert(linktarget, target) {
                    if prev != target {
                        return Err(anyhow!(
                            "two page ids with same linktarget found: {} & {}",
                            prev,
                            target
                        ));
                    }
                }
            }
            Ok(())
        };

        let remaining_batches = threaded_sliding_regex_file(
            self.linktarget.as_path(),
            &Regex::new(r"\(([0-9]{1,20}),0,'(.{0,255}?)'\)")?, // https://www.mediawiki.org/wiki/Manual:Linktarget_table
            1 + 20 + 4 + 255 + 2,
            |caps| -> Result<(LinkTargetId, PageId)> {
                let linktarget = {
                    let m = caps.get(1).unwrap(); // Capture 1 always participates in the match
                    let str = str::from_utf8(m.as_bytes())?;
                    str.parse::<LinkTargetId>()?
                };

                let target = {
                    let m = caps.get(2).unwrap(); // Capture 2 always participates in the match
                    let str = str::from_utf8(m.as_bytes())?;
                    if let Some(id) = title_to_id.get(str) {
                        *id
                    } else {
                        return Err(anyhow!("linktarget title '{}' not known", str));
                    }
                };

                Ok((linktarget, target))
            },
            |batch: &mut Vec<(LinkTargetId, PageId)>, value| {
                batch.push(value);
                if batch.len() > MAX_LOCAL_BATCH_SIZE {
                    ingest_batch(batch)?;
                }
                Ok(())
            },
            thread_count,
        )?;

        log::debug!("ingesting remaining batches");
        for mut batch in remaining_batches {
            ingest_batch(&mut batch)?;
        }

        Ok(Arc::try_unwrap(result).unwrap().into_inner().unwrap())
    }

    /// Parse the pagelinks table dump file and output the parsed links in batches.
    ///
    /// They are output in the form incoming batches, which are maps of a page id to a list of page ids
    /// that link to it. The reason that it is in the incoming form instead of outgoing (which would be
    /// more intuitive), is because the pagelinks table is (at the time of writing) sorted by target
    /// page id, so the batches that are output are not fragmented (i.e. the same target page id is
    /// generally not present in multiple batches as a key of the map).
    ///
    /// The page ids in the lists are not strictly unique, as the parsing process may output the same
    /// link multiple times occasionally.
    pub fn parse_pagelinks_table<F>(
        &self,
        output_link_batch: F,
        linktarget_to_target: &HashMap<LinkTargetId, PageId>,
        redirects: &HashMap<PageId, PageId>,
        thread_count: usize,
    ) -> Result<()>
    where
        F: Fn(&mut LinkBatchIncoming) -> Result<()> + Clone + Send + Sync,
    {
        let main_batch = Arc::new(Mutex::new(LinkBatchIncoming::default()));

        // Helper closure to ingest a batch of links into the main batch. Empties the batch.
        // If the main batch exceeds is size limit, it is outputted and cleared.
        let ingest_batch = |batch: &mut Vec<(PageId, PageId)>| -> Result<()> {
            let mut main_batch = main_batch.lock().unwrap();
            for (source, target) in batch.drain(..) {
                main_batch.insert(source, target);
            }
            if main_batch.size() > MAX_LINK_BATCH_SIZE {
                output_link_batch(&mut main_batch)?;
            }
            Ok(())
        };

        let remaining_bathces = threaded_sliding_regex_file(
            self.pagelinks.as_path(),
            &Regex::new(r"\(([0-9]{1,10}),0,([0-9]{1,20})\)")?, // https://www.mediawiki.org/wiki/Manual:Pagelinks_table
            1 + 10 + 3 + 20 + 1,
            |caps| -> Result<(PageId, PageId)> {
                let source = {
                    let m = caps.get(1).unwrap(); // Capture 1 always participates in the match
                    let str = str::from_utf8(m.as_bytes())?;
                    str.parse::<PageId>()?
                };

                let linktarget = {
                    let m = caps.get(2).unwrap(); // Capture 2 always participates in the match
                    let str = str::from_utf8(m.as_bytes())?;
                    str.parse::<LinkTargetId>()?
                };

                let target = if let Some(target) = linktarget_to_target.get(&linktarget) {
                    *target
                } else {
                    return Err(anyhow!("linktarget id {} not known", linktarget));
                };

                let source = *redirects.get(&source).unwrap_or(&source);
                let target = *redirects.get(&target).unwrap_or(&target);

                if source == target {
                    return Err(anyhow!("self-link found for page id {}", source));
                }

                Ok((source, target))
            },
            |batch: &mut Vec<(PageId, PageId)>, value| {
                batch.push(value);
                if batch.len() > MAX_LOCAL_BATCH_SIZE {
                    ingest_batch(batch)?;
                }
                Ok(())
            },
            thread_count,
        )?;

        log::debug!("ingesting remaining batches");
        for mut batch in remaining_bathces {
            ingest_batch(&mut batch)?;
        }

        log::debug!("outputting final main batch");
        let mut main_batch = main_batch.lock().unwrap();
        if main_batch.size() > 0 {
            output_link_batch(&mut main_batch)?;
        }

        Ok(())
    }
}

/// Parse a file by running a regex on its contents in a sliding window fashion. It does so concurrently
/// in a highly optimized manner, using a fixed number of threads. Regex captures are extracted using a
/// function and stored using another function. The sliding window size is specified in bytes (max match
/// size), to ensure that the regex can match across chunk boundaries when reading the file. Every thread
/// returns a result, which are collected into a vector and returned.
fn threaded_sliding_regex_file<F, G, T, U>(
    path: &Path,
    regex: &Regex,
    max_match_size: usize,
    extract_match: F,
    store_match: G,
    thread_count: usize,
) -> Result<Vec<U>>
where
    F: Fn(&regex::bytes::Captures) -> Result<T> + Clone + Send,
    G: Fn(&mut U, T) -> Result<()> + Clone + Send,
    U: Default + Send,
{
    thread::scope(|s| {
        let path = PathBuf::from(path);
        let parser_count = max(thread_count - 1, 1);

        // Create channels for sending data chunks back and forth between the reader and parsers.
        let (fresh_tx, fresh_rx) = crossbeam_channel::unbounded::<Option<Chunk>>();
        let (stale_tx, stale_rx) = crossbeam_channel::unbounded::<Result<Chunk>>();

        // Spawn the chunk parsers.
        let parser_join_handles = (0..parser_count)
            .map(|_| {
                let regex = regex.clone();
                let extract_match = extract_match.clone();
                let store_match = store_match.clone();
                let fresh_rx = fresh_rx.clone();
                let stale_tx = stale_tx.clone();
                s.spawn(move || {
                    let mut result = U::default();
                    for chunk in fresh_rx {
                        if let Some(chunk) = chunk {
                            for captures in regex.captures_iter(&chunk.data[..chunk.end]) {
                                match extract_match(&captures) {
                                    Ok(extracted) => {
                                        if let Err(e) = store_match(&mut result, extracted) {
                                            stale_tx.send(Err(e)).ok();
                                        }
                                    }
                                    Err(e) => {
                                        log::trace!("regex match extraction failed: {}", e);
                                    }
                                }
                            }
                            stale_tx.send(Ok(chunk)).ok(); // send the chunk back
                        } else {
                            break; // end-of-stream, break loop and exit
                        }
                    }
                    result
                })
            })
            .collect::<Vec<_>>();
        log::debug!("{} parser threads spawned", parser_count);

        let file = File::open(path)?;
        let mut reader = GzDecoder::new(file);
        let chunk_count = parser_count * CHUNK_COUNT_MULTIPLIER;

        // Create new chunks and send to ourselves to be populated.
        for _ in 0..(chunk_count - 1) {
            let new_chunk = Chunk {
                data: vec![0; CHUNK_SIZE_BYTES],
                end: 0,
            };
            stale_tx.send(Ok(new_chunk))?;
        }

        // Cached chunk to facilitate overlap copying.
        let mut current_chunk = Chunk {
            data: vec![0; CHUNK_SIZE_BYTES],
            end: 0,
        };

        // Populate already handled chunks with new data.
        log::debug!("reading file to chunks");
        for new_chunk in &stale_rx {
            let mut new_chunk = new_chunk?; // return error if parser failed
            let overlap_start = if current_chunk.end >= max_match_size {
                current_chunk.end - max_match_size
            } else {
                0
            };
            let overlap_end = current_chunk.end;
            let overlap = overlap_end - overlap_start;
            new_chunk.data[..overlap]
                .copy_from_slice(&current_chunk.data[overlap_start..overlap_end]);
            let old_chunk = std::mem::replace(&mut current_chunk, new_chunk);
            fresh_tx.send(Some(old_chunk))?;
            let bytes_read = reader.read(&mut current_chunk.data[overlap..])?;
            if bytes_read == 0 {
                break;
            }
            current_chunk.end = overlap + bytes_read;
        }
        fresh_tx.send(Some(current_chunk))?;

        // Send end-of-stream message to parsers
        log::debug!("sending end-of-stream signals");
        for _ in 0..parser_count {
            fresh_tx.send(None)?;
        }

        // Wait for threads to exit and collect their results.
        log::debug!("joining parser threads");
        let results = parser_join_handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();

        log::debug!("finished parsing");
        Ok(results)
    })
}

/// Remove chains of redirects from a redirect mapping by concatenating redirects to redirects into
/// single redirects. This will flatten any redirect paths larger than one.
#[must_use]
pub fn cleanup_redirects(mut redirs: HashMap<PageId, PageId>) -> HashMap<PageId, PageId> {
    let mut updates = HashMap::new();
    let mut removals = HashSet::new();

    loop {
        for (source, target) in &redirs {
            if *source == *target {
                removals.insert(*source);
            } else if let Some(new_target) = redirs.get(target) {
                updates.insert(*source, *new_target);
            }
        }

        if updates.is_empty() && removals.is_empty() {
            break;
        }

        for (source, target) in updates.drain() {
            redirs.insert(source, target);
        }

        for source in removals.drain() {
            redirs.remove(&source);
        }
    }

    redirs
}
