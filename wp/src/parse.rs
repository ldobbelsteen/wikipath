use crate::{
    database::{LinkTargetId, PageId},
    dump::TableDumpFiles,
};
use anyhow::{anyhow, Result};
use flate2::read::GzDecoder;
use hashbrown::{HashMap, HashSet};
use regex::bytes::Regex;
use std::{
    cmp::max,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    str,
    sync::{Arc, Mutex},
    thread,
};

/// Struct representing a chunk of bytes. The end field indicates the end of the valid data in the
/// data field.
#[derive(Debug)]
pub struct Chunk {
    data: Vec<u8>,
    end: usize,
}

impl TableDumpFiles {
    /// Parse the page table dump file and return a mapping from page titles to page ids.
    pub fn parse_page_table_dump(&self, thread_count: usize) -> Result<HashMap<String, PageId>> {
        let result = Arc::new(Mutex::new(HashMap::new()));
        sliding_regex_file(
            self.page.as_path(),
            &Regex::new(
                r"\(([0-9]{1,10}),0,'(.{0,255}?)',[01],[01],0.[0-9]{1,32}?,'[0-9]{14}',(?:'[0-9]{14}'|NULL),[0-9]{1,10},[0-9]{1,10},(?:'.{0,32}'|NULL),(?:'.{0,35}'|NULL)\)",
            )?, // https://www.mediawiki.org/wiki/Manual:Page_table
            1 + 10 + 4 + 255 + 8 + 32 + 2 + 14 + 3 + 14 + 2 + 10 + 1 + 10 + 2 + 32 + 3 + 35 + 2,
            |caps| {
                let id: PageId = {
                    let m = caps.get(1).unwrap(); // Capture 1 always participates in the match
                    let str = str::from_utf8(m.as_bytes())?;
                    str.parse::<PageId>()?
                };

                let title: String = {
                    let m = caps.get(2).unwrap(); // Capture 2 always participates in the match
                    String::from_utf8(m.as_bytes().to_vec())?
                };

                if let Some(prev) = result.lock().unwrap().insert(title, id) {
                    if id != prev {
                        log::debug!("same title encountered for page {} and {}", id, prev);
                    }
                }

                Ok(())
            },
            thread_count,
        )?;
        Ok(Arc::into_inner(result)
            .ok_or(anyhow!("failed to unwrap result arc"))?
            .into_inner()?)
    }

    /// Parse the redirect table dump file and return a mapping from source page ids to target page ids.
    pub fn parse_redirect_table_dump(
        &self,
        title_to_id: &HashMap<String, PageId>,
        thread_count: usize,
    ) -> Result<HashMap<PageId, PageId>> {
        let result = Arc::new(Mutex::new(HashMap::new()));
        sliding_regex_file(
            self.redirect.as_path(),
            &Regex::new(
                r"\(([0-9]{1,10}),0,'(.{0,255}?)',(?:'.{0,32}'|NULL),(?:'.{0,255}'|NULL)\)",
            )?, // https://www.mediawiki.org/wiki/Manual:Redirect_table
            1 + 10 + 4 + 255 + 3 + 32 + 3 + 255 + 2,
            |caps| {
                let source: PageId = {
                    let m = caps.get(1).unwrap(); // Capture 1 always participates in the match
                    let str = str::from_utf8(m.as_bytes())?;
                    str.parse::<PageId>()?
                };

                let target: PageId = {
                    let m = caps.get(2).unwrap(); // Capture 2 always participates in the match
                    let str = str::from_utf8(m.as_bytes())?;
                    if let Some(id) = title_to_id.get(str) {
                        *id
                    } else {
                        log::debug!("redirect target title '{}' not known", str);
                        return Ok(());
                    }
                };

                if source == target {
                    log::debug!("self-redirect found for page id {}", source);
                    return Ok(());
                }

                let mut redirs = result.lock().unwrap();
                if let Some(prev) = redirs.insert(source, target) {
                    if prev != target {
                        log::debug!(
                            "two redirects with same source page id {} encountered: {} and {}",
                            source,
                            target,
                            prev
                        );
                    }
                }

                Ok(())
            },
            thread_count,
        )?;
        Ok(Arc::into_inner(result)
            .ok_or(anyhow!("failed to unwrap result arc"))?
            .into_inner()?)
    }

    /// Parse the linktarget table dump file and return a mapping from link target ids to page ids.
    pub fn parse_linktarget_table_dump(
        &self,
        title_to_id: &HashMap<String, PageId>,
        thread_count: usize,
    ) -> Result<HashMap<LinkTargetId, PageId>> {
        let result = Arc::new(Mutex::new(HashMap::new()));
        sliding_regex_file(
            self.linktarget.as_path(),
            &Regex::new(r"\(([0-9]{1,20}),0,'(.{0,255}?)'\)")?, // https://www.mediawiki.org/wiki/Manual:Linktarget_table
            1 + 20 + 4 + 255 + 2,
            |caps| {
                let linktarget: LinkTargetId = {
                    let m = caps.get(1).unwrap(); // Capture 1 always participates in the match
                    let str = str::from_utf8(m.as_bytes())?;
                    str.parse::<LinkTargetId>()?
                };

                let target: PageId = {
                    let m = caps.get(2).unwrap(); // Capture 2 always participates in the match
                    let str = str::from_utf8(m.as_bytes())?;
                    if let Some(id) = title_to_id.get(str) {
                        *id
                    } else {
                        log::debug!("linktarget title '{}' not known", str);
                        return Ok(());
                    }
                };

                if let Some(prev) = result.lock().unwrap().insert(linktarget, target) {
                    if target != prev {
                        log::debug!(
                            "different targets {} and {} found for linktarget {}",
                            target,
                            prev,
                            linktarget
                        );
                    }
                }

                Ok(())
            },
            thread_count,
        )?;
        Ok(Arc::into_inner(result)
            .ok_or(anyhow!("failed to unwrap result arc"))?
            .into_inner()?)
    }

    /// Parse the pagelinks table dump file and output all links to a closure.
    pub fn parse_pagelinks_table_dump<F>(
        &self,
        linktarget_to_target: &HashMap<LinkTargetId, PageId>,
        redirects: &HashMap<PageId, PageId>,
        thread_count: usize,
        output: F,
    ) -> Result<()>
    where
        F: Fn(PageId, PageId) + Clone + Send + Sync,
    {
        sliding_regex_file(
            self.pagelinks.as_path(),
            &Regex::new(r"\(([0-9]{1,10}),0,([0-9]{1,20})\)")?, // https://www.mediawiki.org/wiki/Manual:Pagelinks_table
            1 + 10 + 3 + 20 + 1,
            |caps| {
                let source: PageId = {
                    let m = caps.get(1).unwrap(); // Capture 1 always participates in the match
                    let str = str::from_utf8(m.as_bytes())?;
                    str.parse::<PageId>()?
                };

                let linktarget: LinkTargetId = {
                    let m = caps.get(2).unwrap(); // Capture 2 always participates in the match
                    let str = str::from_utf8(m.as_bytes())?;
                    str.parse::<LinkTargetId>()?
                };

                let target = if let Some(target) = linktarget_to_target.get(&linktarget) {
                    *target
                } else {
                    log::debug!("linktarget id {} not known", linktarget);
                    return Ok(());
                };

                let source_clean = *redirects.get(&source).unwrap_or(&source);
                let target_clean = *redirects.get(&target).unwrap_or(&target);

                if source_clean == target_clean {
                    log::debug!("self-link found for page id {}", source_clean);
                    return Ok(());
                }

                output(source_clean, target_clean);
                Ok(())
            },
            thread_count,
        )
    }
}

/// Parse a file by running a regex on its contents in a sliding window fashion. It does so concurrently
/// in a highly optimized manner, using a fixed number of threads. Regex matches are output to a closure.
/// For the sliding window, a maximum match size in byte size should be specified. This is to ensure that
/// the regex can match across chunk boundaries when reading the file.
fn sliding_regex_file<'a, F>(
    path: &Path,
    regex: &Regex,
    max_match_size: usize,
    consume_captures: F,
    thread_count: usize,
) -> Result<()>
where
    F: 'a + FnMut(regex::bytes::Captures) -> Result<()> + Send + Sync + Clone,
{
    thread::scope(|s| -> Result<()> {
        let path = PathBuf::from(path);
        let parser_count = max(thread_count - 1, 1);

        // Create channels for sending data chunks back and forth between the reader and parsers.
        let (fresh_tx, fresh_rx) = crossbeam_channel::unbounded::<Option<Chunk>>();
        let (stale_tx, stale_rx) = crossbeam_channel::unbounded::<Option<Chunk>>();

        // Spawn the chunk parsers.
        for _ in 0..parser_count {
            let regex = regex.clone();
            let mut consume_capture = consume_captures.clone();
            let fresh_rx = fresh_rx.clone();
            let stale_tx = stale_tx.clone();
            s.spawn(move || {
                for chunk in fresh_rx {
                    if let Some(chunk) = chunk {
                        // Find all captures in chunk and consume results.
                        for captures in regex.captures_iter(&chunk.data[..chunk.end]) {
                            if let Err(e) = consume_capture(captures) {
                                log::warn!("error while consuming capture: {e}");
                            }
                        }
                        stale_tx.send(Some(chunk)).ok(); // Send back chunk.
                    } else {
                        stale_tx.send(None).ok(); // Acknowledge end-of-stream.
                        break;
                    }
                }
            });
        }

        let file = File::open(path)?;
        let mut reader = GzDecoder::new(file);
        let chunk_count = parser_count * 2;
        let chunk_size = 64 * 1024;

        // Create new chunks and send to ourselves to be populated.
        for _ in 0..(chunk_count - 1) {
            let new_chunk = Chunk {
                data: vec![0; chunk_size],
                end: 0,
            };
            stale_tx.send(Some(new_chunk))?;
        }

        // Cached chunk to facilitate overlap copying.
        let mut current_chunk = Chunk {
            data: vec![0; chunk_size],
            end: 0,
        };

        // Populate already handled chunks with new data.
        for new_chunk in &stale_rx {
            let mut new_chunk = new_chunk.unwrap(); // Unwrap, since parser only sends none when we have done so
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
        for _ in 0..parser_count {
            fresh_tx.send(None)?;
        }

        // Receive end-of-stream acknowledgements. Ignore any stale chunks.
        let mut acks = 0;
        while acks < parser_count {
            if stale_rx.iter().next().unwrap().is_none() {
                acks += 1;
            }
        }

        Ok(())
    })
}

/// Remove chains of redirects from a redirect mapping by concatenating redirects to redirects into
/// single redirects. This will flatten any redirect paths larger than one. This operation is
/// in-place.
pub fn cleanup_redirects(redirs: &mut HashMap<PageId, PageId>) {
    let mut updates = HashMap::new();
    let mut removals = HashSet::new();

    loop {
        for (source, target) in redirs.iter() {
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
}
