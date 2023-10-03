use crate::{
    database::{BuildTransaction, PageId},
    dump::Dump,
    progress,
};
use anyhow::Result;
use flate2::read::GzDecoder;
use indicatif::MultiProgress;
use regex::bytes::Regex;
use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    str,
    sync::Arc,
};

#[derive(Debug)]
pub struct Chunk {
    data: Vec<u8>,
    end: usize,
}

impl Dump {
    pub fn parse_dump_file<'a, F>(
        path: &Path,
        regex: Regex,
        max_captures_size: usize,
        consume_captures: F,
        progress: MultiProgress,
        thread_count: usize,
    ) -> Result<()>
    where
        F: 'a + FnMut(regex::bytes::Captures) -> Result<()> + Send + Sync + Clone,
    {
        crossbeam::thread::scope(|s| -> Result<()> {
            let path = PathBuf::from(path);
            let parser_count = if thread_count > 1 {
                thread_count - 1
            } else {
                1
            };

            // Create channels for sending data chunks back and forth between the reader and parsers.
            let (fresh_tx, fresh_rx) = crossbeam::channel::unbounded::<Option<Chunk>>();
            let (stale_tx, stale_rx) = crossbeam::channel::unbounded::<Option<Chunk>>();

            // Spawn the chunk parsers.
            for _ in 0..parser_count {
                let regex = regex.clone();
                let mut consume_capture = consume_captures.clone();
                let fresh_rx = fresh_rx.clone();
                let stale_tx = stale_tx.clone();
                let progress = progress.clone();
                s.spawn(move |_| {
                    for chunk in fresh_rx {
                        if let Some(chunk) = chunk {
                            // Find all captures in chunk and consume results.
                            for captures in regex.captures_iter(&chunk.data[..chunk.end]) {
                                if let Err(e) = consume_capture(captures) {
                                    progress
                                        .println(format!(
                                            "[WARNING] error while consuming capture: {}",
                                            e
                                        ))
                                        .unwrap(); // Assume print errors never happen.
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
            let file_size = file.metadata()?.len();
            let bar = progress.add(progress::byte("", 0, file_size));
            let mut reader = GzDecoder::new(progress::Reader::new(file, bar.clone()));
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
            for new_chunk in stale_rx.iter() {
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

            bar.finish();
            Ok(())
        })
        .unwrap()
    }

    pub fn parse_page(
        &self,
        build: Arc<BuildTransaction>,
        progress: MultiProgress,
        thread_count: usize,
    ) -> Result<()> {
        Self::parse_dump_file(
            self.pages.as_path(),
            Regex::new(
                r"\(([0-9]{1,10}),0,'(.{1,255}?)',[01],[01],0.[0-9]{1,32}?,'[0-9]{14}',(?:'[0-9]{14}'|NULL),[0-9]{1,10},[0-9]{1,10},'wikitext',NULL\)",
            )?, // https://www.mediawiki.org/wiki/Manual:Page_table
            1 + 10 + 4 + 255 + 8 + 32 + 2 + 14 + 3 + 14 + 2 + 10 + 1 + 10 + 17,
            move |caps| {
                let id: PageId = {
                    let m = caps.get(1).unwrap(); // Capture 1 always participates in the match
                    let str = str::from_utf8(m.as_bytes())?;
                    str.parse::<PageId>()?
                };
                let title: String = {
                    let m = caps.get(2).unwrap(); // Capture 2 always participates in the match
                    String::from_utf8(m.as_bytes().to_vec())?
                };
                build.store_title(title, id);
                Ok(())
            },
            progress,
            thread_count,
        )
    }

    pub fn parse_redir(
        &self,
        build: Arc<BuildTransaction>,
        progress: MultiProgress,
        thread_count: usize,
    ) -> Result<()> {
        Self::parse_dump_file(
            self.redirects.as_path(),
            Regex::new(r"\(([0-9]{1,10}),0,'(.{1,255}?)','.{0,32}?','.{0,255}?'\)")?, // https://www.mediawiki.org/wiki/Manual:Redirect_table
            1 + 10 + 4 + 255 + 3 + 32 + 3 + 255 + 2,
            move |caps| {
                let source: PageId = {
                    let m = caps.get(1).unwrap(); // Capture 1 always participates in the match
                    let str = str::from_utf8(m.as_bytes())?;
                    str.parse::<PageId>()?
                };
                let target: PageId = {
                    let m = caps.get(2).unwrap(); // Capture 2 always participates in the match
                    let str = str::from_utf8(m.as_bytes())?;
                    match build.get_id(str) {
                        Some(id) => id,
                        None => {
                            return Ok(()); // Ignore if target title is not known.
                        }
                    }
                };
                build.insert_redirect(source, target)?;
                Ok(())
            },
            progress,
            thread_count,
        )
    }

    pub fn parse_link(
        &self,
        build: Arc<BuildTransaction>,
        progress: MultiProgress,
        thread_count: usize,
    ) -> Result<()> {
        Self::parse_dump_file(
            self.pagelinks.as_path(),
            Regex::new(r"\(([0-9]{1,10}),0,'(.{1,255}?)',0,(?:([0-9]{1,20})|NULL)\)")?, // https://www.mediawiki.org/wiki/Manual:Pagelinks_table
            1 + 10 + 4 + 255 + 4,
            move |caps| {
                let source: PageId = {
                    let m = caps.get(1).unwrap(); // Capture 1 always participates in the match
                    let str = str::from_utf8(m.as_bytes())?;
                    str.parse::<PageId>()?
                };
                let target: PageId = {
                    // Capture 3 might be None. If so, try to get the target id using the target title instead
                    if let Some(m) = caps.get(3) {
                        let str = str::from_utf8(m.as_bytes())?;
                        str.parse::<PageId>()?
                    } else {
                        let m = caps.get(2).unwrap(); // Capture 2 always participates in the match
                        let str = str::from_utf8(m.as_bytes())?;
                        match build.get_id(str) {
                            Some(id) => id,
                            None => {
                                return Ok(()); // Ignore if target title is not known.
                            }
                        }
                    }
                };
                if source != target {
                    build.insert_link(source, target)?;
                }
                Ok(())
            },
            progress,
            thread_count,
        )
    }
}
