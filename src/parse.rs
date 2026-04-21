use crate::{
    database::{LinkTargetId, PageId, PageNamespaceId},
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

// Conservative upper bound for one `page` tuple in the SQL dump.
// Assumes mysqldump-style escaping (varbinary fields can approach 2x expansion).
// Computed worst-case is ~757 bytes:
// - numeric/text fixed fields + delimiters: ~107 bytes
// - `page_title` varbinary(255), SQL-escaped and quoted: up to ~512 bytes
// - `page_content_model` varbinary(32), escaped+quoted: up to ~66 bytes
// - `page_lang` varbinary(35), escaped+quoted: up to ~72 bytes
// Configured at 800 for safety margin across dump quirks.
const PAGE_MAX_MATCH_SIZE_BYTES: usize = 800;

// Conservative upper bound for one `redirect` tuple in the SQL dump.
// Assumes mysqldump-style escaping (varbinary fields can approach 2x expansion).
// Computed worst-case is ~1117 bytes:
// - fixed numeric fields + delimiters: ~27 bytes
// - `rd_title` varbinary(255), SQL-escaped and quoted: up to ~512 bytes
// - `rd_interwiki` varbinary(32), escaped+quoted (or NULL): up to ~66 bytes
// - `rd_fragment` varbinary(255), escaped+quoted (or NULL): up to ~512 bytes
// Configured at 1200 for safety margin across dump quirks.
const REDIRECT_MAX_MATCH_SIZE_BYTES: usize = 1200;

// Conservative upper bound for one `linktarget` tuple in the SQL dump.
// Assumes mysqldump-style escaping (`lt_title` can approach 2x expansion).
// Computed worst-case is ~549 bytes:
// - `lt_id` bigint unsigned max textual width: 20 bytes
// - `lt_namespace` int signed min textual width: 11 bytes
// - `lt_title` varbinary(255), SQL-escaped and quoted: up to ~512 bytes
// - tuple syntax (parens, commas, quotes): 6 bytes
// Configured at 600 for safety margin across dump quirks.
const LINKTARGET_MAX_MATCH_SIZE_BYTES: usize = 600;

// Conservative upper bound for one `pagelinks` tuple in the SQL dump.
// Computed worst-case is ~45 bytes:
// - `pl_from` int unsigned max textual width: 10 bytes
// - `pl_from_namespace` int signed min textual width: 11 bytes
// - `pl_target_id` bigint unsigned max textual width: 20 bytes
// - tuple syntax (parens + commas): 4 bytes
// Configured at 64 for safety margin across dump quirks.
const PAGELINKS_MAX_MATCH_SIZE_BYTES: usize = 64;

// Based on https://www.mediawiki.org/wiki/Manual:Page_table
const PAGE_ROW_PATTERN: &str = r"\((\d+),(-?\d+),'((?:[^'\\]|\\.)*)',[01],[01],(?:\d+(?:\.\d+)?|\.\d+)(?:[eE][+-]?\d+)?,'\d*',(?:'\d*'|NULL),\d+,\d+,(?:'(?:[^'\\]|\\.)*'|NULL),(?:'(?:[^'\\]|\\.)*'|NULL)\)";
// Based on https://www.mediawiki.org/wiki/Manual:Redirect_table
const REDIRECT_ROW_PATTERN: &str =
    r"\((\d+),(-?\d+),'((?:[^'\\]|\\.)*)',(?:'(?:[^'\\]|\\.)*'|NULL),(?:'(?:[^'\\]|\\.)*'|NULL)\)";
// Based on https://www.mediawiki.org/wiki/Manual:Linktarget_table
const LINKTARGET_ROW_PATTERN: &str = r"\((\d+),(-?\d+),'((?:[^'\\]|\\.)*)'\)";
// Based on https://www.mediawiki.org/wiki/Manual:Pagelinks_table
// NOTE: despite newer schema docs listing `(pl_from, pl_target_id, pl_from_namespace)`,
// the dumps we parse are observed as `(pl_from, pl_from_namespace, pl_target_id)`.
const PAGELINKS_ROW_PATTERN: &str = r"\((\d+),(?:-?\d+),(\d+)\)";

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
    /// Parse the page table dump file and return a mapping from page titles to page ids for each
    /// namespace.
    pub fn parse_page_table(&self) -> Result<HashMap<PageNamespaceId, HashMap<String, PageId>>> {
        sliding_regex_file(
            self.page.as_path(),
            &Regex::new(PAGE_ROW_PATTERN)?,
            PAGE_MAX_MATCH_SIZE_BYTES,
            |caps| -> Result<(PageId, PageNamespaceId, String)> {
                let id = {
                    let m = caps.get(1).unwrap(); // Capture 1 always participates in the match
                    let str = std::str::from_utf8(m.as_bytes())?;
                    str.parse::<PageId>()?
                };

                let namespace = {
                    let m = caps.get(2).unwrap(); // Capture 2 always participates in the match
                    let str = std::str::from_utf8(m.as_bytes())?;
                    str.parse::<PageNamespaceId>()?
                };

                let title = {
                    let m = caps.get(3).unwrap(); // Capture 3 always participates in the match
                    String::from_utf8(m.as_bytes().to_vec())?
                };

                Ok((id, namespace, title))
            },
            |result: &mut HashMap<PageNamespaceId, HashMap<String, PageId>>,
             (id, namespace, title)| {
                let namespace_map = result.entry(namespace).or_insert_with(HashMap::new);
                if let Some(prev) = namespace_map.insert(title.clone(), id) {
                    if prev != id {
                        return Err(anyhow!(
                            "two page ids for same title found in namespace {namespace}: {prev} & {id}"
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
        title_to_id: &HashMap<PageNamespaceId, HashMap<String, PageId>>,
    ) -> Result<HashMap<PageId, PageId>> {
        sliding_regex_file(
            self.redirect.as_path(),
            &Regex::new(REDIRECT_ROW_PATTERN)?,
            REDIRECT_MAX_MATCH_SIZE_BYTES,
            |caps| -> Result<(PageId, PageId)> {
                let source = {
                    let m = caps.get(1).unwrap(); // Capture 1 always participates in the match
                    let str = std::str::from_utf8(m.as_bytes())?;
                    str.parse::<PageId>()?
                };

                let target_namespace = {
                    let m = caps.get(2).unwrap(); // Capture 2 always participates in the match
                    let str = std::str::from_utf8(m.as_bytes())?;
                    str.parse::<PageNamespaceId>()?
                };

                let target = {
                    let m = caps.get(3).unwrap(); // Capture 3 always participates in the match
                    let str = std::str::from_utf8(m.as_bytes())?;
                    if let Some(namespace_map) = title_to_id.get(&target_namespace) {
                        if let Some(id) = namespace_map.get(str) {
                            *id
                        } else {
                            return Err(anyhow!("redirect target title '{str}' not known in namespace {target_namespace}"));
                        }
                    } else {
                        return Err(anyhow!(
                            "redirect target namespace id {target_namespace} not known"
                        ));
                    }
                };

                if source == target {
                    return Err(anyhow!("self-redirect found for page id {source}"));
                }

                Ok((source, target))
            },
            |result: &mut HashMap<PageId, PageId>, (source, target)| {
                if let Some(prev) = result.insert(source, target) {
                    if prev != target {
                        return Err(anyhow!(
                            "two redirect targets for same source found: {prev} & {target}"
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
        title_to_id: &HashMap<PageNamespaceId, HashMap<String, PageId>>,
    ) -> Result<HashMap<LinkTargetId, PageId>> {
        sliding_regex_file(
            self.linktarget.as_path(),
            &Regex::new(LINKTARGET_ROW_PATTERN)?,
            LINKTARGET_MAX_MATCH_SIZE_BYTES,
            |caps| -> Result<(LinkTargetId, PageId)> {
                let linktarget = {
                    let m = caps.get(1).unwrap(); // Capture 1 always participates in the match
                    let str = std::str::from_utf8(m.as_bytes())?;
                    str.parse::<LinkTargetId>()?
                };

                let target_namespace = {
                    let m = caps.get(2).unwrap(); // Capture 2 always participates in the match
                    let str = std::str::from_utf8(m.as_bytes())?;
                    str.parse::<PageNamespaceId>()?
                };

                let target = {
                    let m = caps.get(3).unwrap(); // Capture 3 always participates in the match
                    let str = std::str::from_utf8(m.as_bytes())?;
                    if let Some(namespace_map) = title_to_id.get(&target_namespace) {
                        if let Some(id) = namespace_map.get(str) {
                            *id
                        } else {
                            return Err(anyhow!("linktarget title '{str}' not known in namespace {target_namespace}"));
                        }
                    } else {
                        return Err(anyhow!(
                            "linktarget namespace id {target_namespace} not known"
                        ));
                    }
                };

                Ok((linktarget, target))
            },
            |result: &mut HashMap<LinkTargetId, PageId>, (linktarget, target)| {
                if let Some(prev) = result.insert(linktarget, target) {
                    if prev != target {
                        return Err(anyhow!(
                            "two page ids with same linktarget found: {prev} & {target}"
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
            &Regex::new(PAGELINKS_ROW_PATTERN)?,
            PAGELINKS_MAX_MATCH_SIZE_BYTES,
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
                    return Err(anyhow!("linktarget id {linktarget} not known"));
                };

                let source = *redirects.get(&source).unwrap_or(&source);
                let target = *redirects.get(&target).unwrap_or(&target);

                if source == target {
                    return Err(anyhow!("self-link found for page id {source}"));
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
        if remaining_batch.size() > 0 {
            return Err(anyhow!("link batch not properly drained"));
        }

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
        let overlap_start = prev_chunk.end.saturating_sub(max_match_size);
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
                    log::trace!("regex match extraction failed: {e}");
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_regex_matches_multiple_valid_number_formats() {
        let regex = Regex::new(PAGE_ROW_PATTERN).unwrap();

        let rows: &[&[u8]] = &[
            br"(123,-1,'Title',1,0,0.123,'20240102030405',NULL,456,789,'wikitext','en')",
            br"(123,-1,'Title',1,0,1,'20240102030405','20240102030405',456,789,NULL,NULL)",
            br"(123,-1,'Title',1,0,.5,'20240102030405',NULL,456,789,NULL,NULL)",
            br"(123,-1,'Title',1,0,1e-5,'20240102030405',NULL,456,789,NULL,NULL)",
            br"(123,-1,'Title',1,0,1.25E+10,'20240102030405',NULL,456,789,NULL,NULL)",
        ];

        for row in rows {
            assert!(regex.is_match(row));
        }
    }

    #[test]
    fn page_regex_matches_escaped_title_and_captures_core_fields() {
        let regex = Regex::new(PAGE_ROW_PATTERN).unwrap();

        let row = br"(123,-1,'A\'B\\C',1,0,0.123,'20240102030405',NULL,456,789,'wikitext','en')";
        let caps = regex.captures(row).unwrap();
        assert_eq!(caps.get(1).unwrap().as_bytes(), b"123");
        assert_eq!(caps.get(2).unwrap().as_bytes(), b"-1");
        assert_eq!(caps.get(3).unwrap().as_bytes(), b"A\\'B\\\\C");
    }

    #[test]
    fn page_regex_rejects_invalid_quote_escaping() {
        let regex = Regex::new(PAGE_ROW_PATTERN).unwrap();
        let row = br"(123,-1,'A''B',1,0,0.123,'20240102030405',NULL,456,789,NULL,NULL)";
        assert!(!regex.is_match(row));
    }

    #[test]
    fn page_regex_rejects_invalid_random_number() {
        let regex = Regex::new(PAGE_ROW_PATTERN).unwrap();
        let row = br"(123,-1,'Title',1,0,1e,'20240102030405',NULL,456,789,NULL,NULL)";
        assert!(!regex.is_match(row));
    }

    #[test]
    fn page_regex_matches_null_and_non_null_tail_fields() {
        let regex = Regex::new(PAGE_ROW_PATTERN).unwrap();
        let with_nulls = br"(1,0,'T',0,0,0.1,'20240102030405',NULL,1,1,NULL,NULL)";
        let with_values =
            br"(1,0,'T',0,0,0.1,'20240102030405','20240102030405',1,1,'json','zh-hans')";
        assert!(regex.is_match(with_nulls));
        assert!(regex.is_match(with_values));
    }

    #[test]
    fn redirect_regex_matches_with_null_optional_fields() {
        let regex = Regex::new(REDIRECT_ROW_PATTERN).unwrap();
        let row = br"(42,0,'Target',NULL,NULL)";
        let caps = regex.captures(row).unwrap();
        assert_eq!(caps.get(1).unwrap().as_bytes(), b"42");
        assert_eq!(caps.get(2).unwrap().as_bytes(), b"0");
        assert_eq!(caps.get(3).unwrap().as_bytes(), b"Target");
    }

    #[test]
    fn redirect_regex_matches_escaped_literals() {
        let regex = Regex::new(REDIRECT_ROW_PATTERN).unwrap();
        let row = br"(42,0,'Foo\'bar','w\:en','Section\\2')";
        let caps = regex.captures(row).unwrap();
        assert_eq!(caps.get(1).unwrap().as_bytes(), b"42");
        assert_eq!(caps.get(2).unwrap().as_bytes(), b"0");
        assert_eq!(caps.get(3).unwrap().as_bytes(), b"Foo\\'bar");
    }

    #[test]
    fn redirect_regex_rejects_unescaped_quote_in_title() {
        let regex = Regex::new(REDIRECT_ROW_PATTERN).unwrap();
        let row = br"(42,0,'Foo'bar',NULL,NULL)";
        assert!(!regex.is_match(row));
    }

    #[test]
    fn linktarget_regex_matches_bigint_id_and_escaped_title() {
        let regex = Regex::new(LINKTARGET_ROW_PATTERN).unwrap();
        let row = br"(18446744073709551615,-2,'Talk\:\\Main\'Page')";
        let caps = regex.captures(row).unwrap();
        assert_eq!(caps.get(1).unwrap().as_bytes(), b"18446744073709551615");
        assert_eq!(caps.get(2).unwrap().as_bytes(), b"-2");
        assert_eq!(caps.get(3).unwrap().as_bytes(), b"Talk\\:\\\\Main\\'Page");
    }

    #[test]
    fn linktarget_regex_rejects_missing_quotes() {
        let regex = Regex::new(LINKTARGET_ROW_PATTERN).unwrap();
        let row = br"(10,0,NoQuotes)";
        assert!(!regex.is_match(row));
    }

    #[test]
    fn pagelinks_regex_uses_observed_dump_order() {
        let regex = Regex::new(PAGELINKS_ROW_PATTERN).unwrap();
        let row = br"(11,-7,22)";
        let caps = regex.captures(row).unwrap();
        assert_eq!(caps.get(1).unwrap().as_bytes(), b"11");
        assert_eq!(caps.get(2).unwrap().as_bytes(), b"22");
    }

    #[test]
    fn pagelinks_regex_accepts_positive_namespace() {
        let regex = Regex::new(PAGELINKS_ROW_PATTERN).unwrap();
        let row = br"(11,7,22)";
        assert!(regex.is_match(row));
    }

    #[test]
    fn pagelinks_regex_rejects_wrong_column_order_shape() {
        let regex = Regex::new(PAGELINKS_ROW_PATTERN).unwrap();
        let row = br"(11,22,-7)";
        assert!(!regex.is_match(row));
    }

    #[test]
    fn cleanup_redirects_flattens_chains() {
        let mut redirs = HashMap::new();
        redirs.insert(1, 2);
        redirs.insert(2, 3);
        redirs.insert(3, 4);

        let cleaned = cleanup_redirects(redirs);
        assert_eq!(cleaned.get(&1), Some(&4));
        assert_eq!(cleaned.get(&2), Some(&4));
        assert_eq!(cleaned.get(&3), Some(&4));
    }

    #[test]
    fn cleanup_redirects_removes_self_redirects() {
        let mut redirs = HashMap::new();
        redirs.insert(1, 1);
        redirs.insert(2, 3);

        let cleaned = cleanup_redirects(redirs);
        assert!(!cleaned.contains_key(&1));
        assert_eq!(cleaned.get(&2), Some(&3));
    }

    #[test]
    fn cleanup_redirects_handles_mixed_graph() {
        let mut redirs = HashMap::new();
        redirs.insert(1, 2);
        redirs.insert(2, 2);
        redirs.insert(3, 4);
        redirs.insert(4, 5);

        let cleaned = cleanup_redirects(redirs);
        assert_eq!(cleaned.get(&1), Some(&2));
        assert!(!cleaned.contains_key(&2));
        assert_eq!(cleaned.get(&3), Some(&5));
        assert_eq!(cleaned.get(&4), Some(&5));
    }
}
