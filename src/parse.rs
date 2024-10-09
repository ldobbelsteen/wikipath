use crate::{
    database::{LinkTargetId, PageId},
    dump::TableDumpFiles,
};
use anyhow::{anyhow, Result};
use flate2::read::GzDecoder;
use regex::bytes::Regex;
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::Read,
    path::Path,
};

const CHUNK_SIZE_BYTES: usize = 1024 * 1024; // 1MB
const MAX_LINK_BATCH_SIZE: usize = 4_000_000;

/// Struct representing a batch of links stored in the incoming format.
#[derive(Debug, Default)]
pub struct IncomingLinkBatch {
    size: usize,
    incoming: HashMap<PageId, Vec<PageId>>,
}

impl IncomingLinkBatch {
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
    pub fn parse_page_table(&self) -> Result<HashMap<String, PageId>> {
        sliding_regex_file(
            self.page.as_path(),
            &Regex::new(
                r"\(([0-9]{1,10}),0,'(.{0,255}?)',[01],[01],0.[0-9]{1,32}?,'[0-9]{14}',(?:'[0-9]{14}'|NULL),[0-9]{1,10},[0-9]{1,10},(?:'.{0,32}'|NULL),(?:'.{0,35}'|NULL)\)",
            )?, // https://www.mediawiki.org/wiki/Manual:Page_table
            1 + 10 + 4 + 255 + 8 + 32 + 2 + 14 + 3 + 14 + 2 + 10 + 1 + 10 + 2 + 32 + 3 + 35 + 2,
            |caps| -> Result<(PageId, String)> {
                let id = {
                    let m = caps.get(1).unwrap(); // Capture 1 always participates in the match
                    let str = std::str::from_utf8(m.as_bytes())?;
                    str.parse::<PageId>()?
                };

                let title = {
                    let m = caps.get(2).unwrap(); // Capture 2 always participates in the match
                    String::from_utf8(m.as_bytes().to_vec())?
                };

                Ok((id, title))
            },
            |result: &mut HashMap<String, PageId>, (id, title)| {
                if let Some(prev) = result.insert(title, id) {
                    if prev != id {
                        return Err(anyhow!(
                            "two page ids for same title found: {} & {}",
                            prev,
                            id
                        ));
                    }
                }
                Ok(())
            },
        )
    }

    /// Parse the redirect table dump file and return a mapping from source page ids to target page ids.
    pub fn parse_redirect_table(
        &self,
        title_to_id: &HashMap<String, PageId>,
    ) -> Result<HashMap<PageId, PageId>> {
        sliding_regex_file(
            self.redirect.as_path(),
            &Regex::new(
                r"\(([0-9]{1,10}),0,'(.{0,255}?)',(?:'.{0,32}'|NULL),(?:'.{0,255}'|NULL)\)",
            )?, // https://www.mediawiki.org/wiki/Manual:Redirect_table
            1 + 10 + 4 + 255 + 3 + 32 + 3 + 255 + 2,
            |caps| -> Result<(PageId, PageId)> {
                let source = {
                    let m = caps.get(1).unwrap(); // Capture 1 always participates in the match
                    let str = std::str::from_utf8(m.as_bytes())?;
                    str.parse::<PageId>()?
                };

                let target = {
                    let m = caps.get(2).unwrap(); // Capture 2 always participates in the match
                    let str = std::str::from_utf8(m.as_bytes())?;
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
            |result: &mut HashMap<PageId, PageId>, (source, target)| {
                if let Some(prev) = result.insert(source, target) {
                    if prev != target {
                        return Err(anyhow!(
                            "two redirect targets for same source found: {} & {}",
                            prev,
                            target
                        ));
                    }
                }
                Ok(())
            },
        )
    }

    /// Parse the linktarget table dump file and return a mapping from link target ids to page ids.
    pub fn parse_linktarget_table(
        &self,
        title_to_id: &HashMap<String, PageId>,
    ) -> Result<HashMap<LinkTargetId, PageId>> {
        sliding_regex_file(
            self.linktarget.as_path(),
            &Regex::new(r"\(([0-9]{1,20}),0,'(.{0,255}?)'\)")?, // https://www.mediawiki.org/wiki/Manual:Linktarget_table
            1 + 20 + 4 + 255 + 2,
            |caps| -> Result<(LinkTargetId, PageId)> {
                let linktarget = {
                    let m = caps.get(1).unwrap(); // Capture 1 always participates in the match
                    let str = std::str::from_utf8(m.as_bytes())?;
                    str.parse::<LinkTargetId>()?
                };

                let target = {
                    let m = caps.get(2).unwrap(); // Capture 2 always participates in the match
                    let str = std::str::from_utf8(m.as_bytes())?;
                    if let Some(id) = title_to_id.get(str) {
                        *id
                    } else {
                        return Err(anyhow!("linktarget title '{}' not known", str));
                    }
                };

                Ok((linktarget, target))
            },
            |result: &mut HashMap<LinkTargetId, PageId>, (linktarget, target)| {
                if let Some(prev) = result.insert(linktarget, target) {
                    if prev != target {
                        return Err(anyhow!(
                            "two page ids with same linktarget found: {} & {}",
                            prev,
                            target
                        ));
                    }
                }
                Ok(())
            },
        )
    }

    /// Parse the pagelinks table dump file and output the parsed links in batches.
    ///
    /// They are output in the form incoming batches, which are maps of a page id to a list of page ids
    /// that link to it. The reason that it is in the incoming form instead of outgoing (which would be
    /// more intuitive), is because the pagelinks table is (at the time of writing) sorted by target
    /// page id, so the batches that are output are not fragmented (i.e. the same target page id is
    /// generally not present in multiple batches as a key of the map). This helps with the performance
    /// of the database insertion process.
    ///
    /// The page ids in the lists are not strictly unique, as the parsing process may output the same
    /// link multiple times occasionally.
    pub fn parse_pagelinks_table<F: Fn(&mut IncomingLinkBatch) -> Result<()>>(
        &self,
        redirects: &HashMap<PageId, PageId>,
        linktarget_to_target: &HashMap<LinkTargetId, PageId>,
        output_link_batch: F,
    ) -> Result<()> {
        let mut remaining_batch = sliding_regex_file(
            self.pagelinks.as_path(),
            &Regex::new(r"\(([0-9]{1,10}),0,([0-9]{1,20})\)")?, // https://www.mediawiki.org/wiki/Manual:Pagelinks_table
            1 + 10 + 3 + 20 + 1,
            |caps| -> Result<(PageId, PageId)> {
                let source = {
                    let m = caps.get(1).unwrap(); // Capture 1 always participates in the match
                    let str = std::str::from_utf8(m.as_bytes())?;
                    str.parse::<PageId>()?
                };

                let linktarget = {
                    let m = caps.get(2).unwrap(); // Capture 2 always participates in the match
                    let str = std::str::from_utf8(m.as_bytes())?;
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
            |batch: &mut IncomingLinkBatch, (source, target)| {
                batch.insert(source, target);
                if batch.size() > MAX_LINK_BATCH_SIZE {
                    output_link_batch(batch)?;
                    if batch.size() > 0 {
                        return Err(anyhow!("link batch not properly drained"));
                    }
                }
                Ok(())
            },
        )?;

        output_link_batch(&mut remaining_batch)?;
        Ok(())
    }
}

/// Parse a file by running a regex on its contents in a sliding window fashion. Regex captures
/// are extracted using a function and stored using another function. The sliding window size is
/// specified in bytes (max match size), to ensure that the regex can match across chunk boundaries
/// when reading the file.
fn sliding_regex_file<
    F: Fn(&regex::bytes::Captures) -> Result<T>,
    G: Fn(&mut U, T) -> Result<()>,
    T,
    U: Default,
>(
    path: &Path,
    regex: &Regex,
    max_match_size: usize,
    extract_match: F,
    store_match: G,
) -> Result<U> {
    struct Chunk {
        data: Vec<u8>, // TODO: investigate if this could be a static array (on stack)
        end: usize,
    }

    impl Default for Chunk {
        fn default() -> Self {
            Self {
                data: vec![0; CHUNK_SIZE_BYTES],
                end: 0,
            }
        }
    }

    let file = File::open(path)?;
    let mut reader = GzDecoder::new(file);
    let mut result = U::default();

    let mut prev_chunk = Chunk::default();
    let mut cur_chunk = Chunk::default();

    loop {
        // Copy end of previous chunk to start of current chunk.
        let overlap_start = if prev_chunk.end >= max_match_size {
            prev_chunk.end - max_match_size
        } else {
            0
        };
        let overlap_end = prev_chunk.end;
        let overlap = overlap_end - overlap_start;
        cur_chunk.data[..overlap].copy_from_slice(&prev_chunk.data[overlap_start..overlap_end]);

        // Read new data into current chunk (starting after the overlap).
        let bytes_read = reader.read(&mut cur_chunk.data[overlap..])?;
        if bytes_read == 0 {
            break; // EOF
        }
        cur_chunk.end = overlap + bytes_read;

        // Process the current chunk by running the regex on it.
        for captures in regex.captures_iter(&cur_chunk.data[..cur_chunk.end]) {
            match extract_match(&captures) {
                Ok(m) => {
                    store_match(&mut result, m)?;
                }
                Err(e) => {
                    // NOTE: these happen often and can be ignored
                    log::trace!("regex match extraction failed: {}", e);
                }
            }
        }

        // Make the current chunk the previous chunk.
        std::mem::swap(&mut prev_chunk.data, &mut cur_chunk.data);
    }

    Ok(result)
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
