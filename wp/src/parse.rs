use crate::{database::PageId, dump::Dump};
use anyhow::{anyhow, Result};
use flate2::read::GzDecoder;
use hashbrown::HashMap;
use log::{debug, warn};
use regex::bytes::Regex;
use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    str,
    sync::{Arc, Mutex},
    thread,
};

#[derive(Debug)]
pub struct Chunk {
    data: Vec<u8>,
    end: usize,
}

impl Dump {
    pub fn parse_page_dump(&self, thread_count: usize) -> Result<HashMap<String, PageId>> {
        let result = Arc::new(Mutex::new(HashMap::new()));
        Self::parse_dump_file(
            self.pages.as_path(),
            &Regex::new(
                r"\(([0-9]{1,10}),0,'(.{1,255}?)',[01],[01],0.[0-9]{1,32}?,'[0-9]{14}',(?:'[0-9]{14}'|NULL),[0-9]{1,10},[0-9]{1,10},'wikitext',NULL\)",
            )?, // https://www.mediawiki.org/wiki/Manual:Page_table
            1 + 10 + 4 + 255 + 8 + 32 + 2 + 14 + 3 + 14 + 2 + 10 + 1 + 10 + 17,
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
                        debug!("same title encountered for page {} and {}", id, prev);
                    }
                }

                Ok(())
            },
            thread_count,
        )?;
        Ok(Arc::try_unwrap(result)
            .map_err(|_| anyhow!("failed to unwrap page result arc"))?
            .into_inner()?)
    }

    pub fn parse_redir_dump(
        &self,
        pages: &HashMap<String, PageId>,
        thread_count: usize,
    ) -> Result<HashMap<PageId, PageId>> {
        let result = Arc::new(Mutex::new(HashMap::new()));
        Self::parse_dump_file(
            self.redirects.as_path(),
            &Regex::new(r"\(([0-9]{1,10}),0,'(.{1,255}?)','.{0,32}?','.{0,255}?'\)")?, // https://www.mediawiki.org/wiki/Manual:Redirect_table
            1 + 10 + 4 + 255 + 3 + 32 + 3 + 255 + 2,
            |caps| {
                let source: PageId = {
                    let m = caps.get(1).unwrap(); // Capture 1 always participates in the match
                    let str = str::from_utf8(m.as_bytes())?;
                    str.parse::<PageId>()?
                };

                let mut target: PageId = {
                    let m = caps.get(2).unwrap(); // Capture 2 always participates in the match
                    let str = str::from_utf8(m.as_bytes())?;
                    if let Some(id) = pages.get(str) {
                        *id
                    } else {
                        debug!("redirect target title '{}' not known", str);
                        return Ok(());
                    }
                };

                let mut redirs = result.lock().unwrap();

                // Follow redirect chain until we reach the end or come back again.
                while let Some(next) = redirs.get(&target) {
                    if *next == source {
                        debug!(
                            "redirect chain cycle detected starting from page id {}",
                            source
                        );
                        return Ok(());
                    }
                    target = *next;
                }

                if source == target {
                    debug!("self-redirect found for page id {}", source);
                    return Ok(());
                }

                if let Some(prev) = redirs.insert(source, target) {
                    if prev != target {
                        debug!(
                            "two redirects with same source page id {} encountered: {} and {}",
                            source, target, prev
                        );
                    }
                }

                Ok(())
            },
            thread_count,
        )?;
        Ok(Arc::try_unwrap(result)
            .map_err(|_| anyhow!("failed to unwrap redir result arc"))?
            .into_inner()?)
    }

    pub fn parse_link_dump<F>(
        &self,
        pages: &HashMap<String, PageId>,
        redirs: &HashMap<PageId, PageId>,
        thread_count: usize,
        output: F,
    ) -> Result<()>
    where
        F: Fn(PageId, PageId) + Clone + Send + Sync,
    {
        Self::parse_dump_file(
            self.pagelinks.as_path(),
            &Regex::new(r"\(([0-9]{1,10}),0,'(.{1,255}?)',0,(?:[0-9]{1,20}|NULL)\)")?, // https://www.mediawiki.org/wiki/Manual:Pagelinks_table
            1 + 10 + 4 + 255 + 4,
            |caps| {
                let source: PageId = {
                    let m = caps.get(1).unwrap(); // Capture 1 always participates in the match
                    let str = str::from_utf8(m.as_bytes())?;
                    str.parse::<PageId>()?
                };

                let target: PageId = {
                    let m = caps.get(2).unwrap(); // Capture 2 always participates in the match
                    let str = str::from_utf8(m.as_bytes())?;
                    if let Some(id) = pages.get(str) {
                        *id
                    } else {
                        debug!("link target title '{}' not known", str);
                        return Ok(());
                    }
                };

                let source_clean = *redirs.get(&source).unwrap_or(&source);
                let target_clean = *redirs.get(&target).unwrap_or(&target);

                if source_clean == target_clean {
                    debug!("self-link found for page id {}", source_clean);
                    return Ok(());
                }

                output(source, target);
                Ok(())
            },
            thread_count,
        )
    }

    pub fn parse_dump_file<'a, F>(
        path: &Path,
        regex: &Regex,
        max_captures_size: usize,
        consume_captures: F,
        thread_count: usize,
    ) -> Result<()>
    where
        F: 'a + FnMut(regex::bytes::Captures) -> Result<()> + Send + Sync + Clone,
    {
        thread::scope(|s| -> Result<()> {
            let path = PathBuf::from(path);
            let parser_count = if thread_count > 1 {
                thread_count - 1
            } else {
                1
            };

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
                                    warn!("error while consuming capture: {e}");
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
                let overlap_start = if current_chunk.end >= max_captures_size {
                    current_chunk.end - max_captures_size
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
}

/// Remove chains of redirects from a redirect mapping by concatenating redirects to redirects into
/// single redirects. This will flatten any redirect paths larger than one. This operation is
/// in-place.
pub fn cleanup_redirects(redirs: &mut HashMap<PageId, PageId>) {
    let mut updates = HashMap::new();
    loop {
        for (source, target) in redirs.iter() {
            if let Some(new_target) = redirs.get(target) {
                updates.insert(*source, *new_target);
            }
        }

        if updates.is_empty() {
            break;
        }
        for (source, target) in updates.drain() {
            redirs.insert(source, target);
        }
    }
}
