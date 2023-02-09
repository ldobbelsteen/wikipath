use crate::{database::PageId, dump::Dump, progress};
use crossbeam_channel::{Receiver, Sender};
use error_chain::error_chain;
use flate2::read::GzDecoder;
use hashbrown::{HashMap, HashSet};
use indicatif::MultiProgress;
use regex::bytes::Regex;
use std::{
    fs::File,
    io::{self, Read},
    path::PathBuf,
    str,
    sync::Arc,
    thread::{self, JoinHandle},
};

error_chain! {
    foreign_links {
        Io(io::Error);
    }
}

impl Dump {
    pub fn parse_dump_file<T, U, F, G>(
        path: PathBuf,
        regex: Regex,
        max_match_size: usize,
        extract_match: F,
        store_match: G,
        progress: MultiProgress,
        thread_count: usize,
    ) -> Result<T>
    where
        T: 'static + Default + Send + Sync,
        U: 'static + Send + Sync,
        F: 'static + Fn(regex::bytes::Captures) -> Option<U> + Send + Sync + Clone,
        G: 'static + Fn(&mut T, U) + Send + Sync,
    {
        let parser_count = if thread_count > 2 {
            thread_count - 2
        } else {
            1
        };

        struct Chunk {
            data: Vec<u8>,
            size: usize,
        }

        let spawn_reader = || -> (JoinHandle<Result<()>>, Receiver<Chunk>, Sender<Chunk>) {
            let (fresh_tx, fresh_rx) = crossbeam_channel::unbounded();
            let (stale_tx, stale_rx) = crossbeam_channel::unbounded();

            let stale_tx_clone = stale_tx.clone();
            let reader = thread::spawn(move || {
                let file = File::open(path)?;
                let file_size = file.metadata()?.len();
                let bar = progress.add(progress::byte("".into(), 0, file_size));
                let mut reader = GzDecoder::new(progress::Reader::new(file, bar.clone()));
                let chunk_count = parser_count * 2;
                let chunk_size = 8192;

                for _ in 0..(chunk_count - 1) {
                    let chunk = Chunk {
                        data: vec![0; chunk_size],
                        size: 0,
                    };
                    if let Err(e) = stale_tx.send(chunk) {
                        eprintln!("unexpected channel error: {}", e);
                    }
                }

                let mut current_chunk = Chunk {
                    data: vec![0; chunk_size],
                    size: 0,
                };

                for mut new_chunk in stale_rx {
                    let overlap_start = if current_chunk.size >= max_match_size {
                        current_chunk.size - max_match_size
                    } else {
                        0
                    };
                    let overlap_end = current_chunk.size;
                    let overlap = overlap_end - overlap_start;
                    new_chunk.data[..overlap]
                        .copy_from_slice(&current_chunk.data[overlap_start..overlap_end]);
                    let old_chunk = std::mem::replace(&mut current_chunk, new_chunk);
                    if let Err(e) = fresh_tx.send(old_chunk) {
                        eprintln!("unexpected channel error: {}", e);
                    }
                    let bytes_read = reader.read(&mut current_chunk.data[overlap..])?;
                    if bytes_read == 0 {
                        break;
                    }
                    current_chunk.size = overlap + bytes_read;
                }

                if let Err(e) = fresh_tx.send(current_chunk) {
                    eprintln!("unexpected channel error: {}", e);
                }

                bar.finish();
                Ok(())
            });

            (reader, fresh_rx, stale_tx_clone)
        };

        let spawn_parsers = |fresh: Receiver<Chunk>, stale: Sender<Chunk>| -> Result<Receiver<U>> {
            let (tx, rx) = crossbeam_channel::bounded(parser_count * 16);

            for _ in 0..parser_count {
                let regex = regex.clone();
                let extract_match = extract_match.clone();
                let fresh = fresh.clone();
                let stale = stale.clone();
                let tx = tx.clone();
                thread::spawn(move || {
                    for chunk in fresh {
                        for captures in regex.captures_iter(&chunk.data[..chunk.size]) {
                            if let Some(result) = extract_match(captures) {
                                if let Err(e) = tx.send(result) {
                                    eprintln!("unexpected channel error: {}", e);
                                }
                            }
                        }
                        stale.send(chunk).ok();
                    }
                });
            }

            Ok(rx)
        };

        let spawn_ingester = |matches: Receiver<U>| -> JoinHandle<T> {
            thread::spawn(move || {
                let mut result = Default::default();
                for m in matches {
                    store_match(&mut result, m);
                }
                result
            })
        };

        let (reader, fresh, stale) = spawn_reader();
        let matches = spawn_parsers(fresh, stale)?;
        let ingester = spawn_ingester(matches);

        reader.join().unwrap()?;
        let result = ingester.join().unwrap();
        Ok(result)
    }

    pub fn parse_page_dump_file(
        &self,
        thread_count: usize,
        progress: MultiProgress,
    ) -> Result<HashMap<String, PageId>> {
        let mc = progress.clone();
        Self::parse_dump_file(
            self.pages.clone(),
            Regex::new(r"\(([0-9]{1,10}),0,'(.{1,255}?)',[01],[01],0.[0-9]{1,32}?,'[0-9]{14}',(?:'[0-9]{14}'|NULL),[0-9]{1,10},[0-9]{1,10},'wikitext',NULL\)").unwrap(), // https://www.mediawiki.org/wiki/Manual:Page_table
            1 + 10 + 4 + 255 + 8 + 32 + 2 + 14 + 3 + 14 + 2 + 10 + 1 + 10 + 17,
            move |caps| {
                let id: PageId = {
                    if let Some(m) = caps.get(1) {
                        match str::from_utf8(m.as_bytes()) {
                            Err(e) => {
                                progress
                                    .println(format!(
                                        "Skipping match with invalid page ID bytes: {}",
                                        e
                                    ))
                                    .ok();
                                return None;
                            }
                            Ok(raw) => match raw.parse::<PageId>() {
                                Err(e) => {
                                    if matches!(e.kind(), std::num::IntErrorKind::PosOverflow) {
                                        progress
                                            .println(format!(
                                                "Skipping match with too large page ID: {}",
                                                raw
                                            ))
                                            .ok();
                                    } else {
                                        progress
                                            .println(format!(
                                                "Skipping match with invalid page ID: {}",
                                                raw
                                            ))
                                            .ok();
                                    }
                                    return None;
                                }
                                Ok(id) => id,
                            },
                        }
                    } else {
                        progress
                            .println(format!(
                                "Skpping match with missing page ID: {}",
                                caps.get(0)
                                    .and_then(
                                        |full_match| str::from_utf8(full_match.as_bytes()).ok()
                                    )
                                    .unwrap_or("?")
                            ))
                            .ok();
                        return None;
                    }
                };
                let title: String = {
                    if let Some(m) = caps.get(2) {
                        match str::from_utf8(m.as_bytes()) {
                            Err(e) => {
                                progress
                                    .println(format!(
                                        "Skipping match with invalid title bytes: {}",
                                        e
                                    ))
                                    .ok();
                                return None;
                            }
                            Ok(title) => title.to_string(),
                        }
                    } else {
                        progress
                            .println(format!(
                                "Skpping match with missing title: {}",
                                caps.get(0)
                                    .and_then(
                                        |full_match| str::from_utf8(full_match.as_bytes()).ok()
                                    )
                                    .unwrap_or("?")
                            ))
                            .ok();
                        return None;
                    }
                };
                Some((id, title))
            },
            |titles: &mut HashMap<String, PageId>, page: (PageId, String)| {
                titles.insert(page.1, page.0);
            },
            mc,
            thread_count,
        )
    }

    pub fn parse_redir_dump_file(
        &self,
        thread_count: usize,
        titles: Arc<HashMap<String, PageId>>,
        progress: MultiProgress,
    ) -> Result<HashMap<PageId, PageId>> {
        let mc = progress.clone();
        let redirects = Self::parse_dump_file(
            self.redirects.clone(),
            Regex::new(r"\(([0-9]{1,10}),0,'(.{1,255}?)','.{0,32}?','.{0,255}?'\)").unwrap(), // https://www.mediawiki.org/wiki/Manual:Redirect_table
            1 + 10 + 4 + 255 + 3 + 32 + 3 + 255 + 2,
            move |caps| {
                let source: PageId = {
                    if let Some(m) = caps.get(1) {
                        match str::from_utf8(m.as_bytes()) {
                            Err(e) => {
                                progress
                                    .println(format!(
                                        "Skipping match with invalid source ID bytes: {}",
                                        e
                                    ))
                                    .ok();
                                return None;
                            }
                            Ok(raw) => match raw.parse::<PageId>() {
                                Err(e) => {
                                    if matches!(e.kind(), std::num::IntErrorKind::PosOverflow) {
                                        progress
                                            .println(format!(
                                                "Skipping match with too large source ID: {}",
                                                raw
                                            ))
                                            .ok();
                                    } else {
                                        progress
                                            .println(format!(
                                                "Skipping match with invalid source ID: {}",
                                                raw
                                            ))
                                            .ok();
                                    }
                                    return None;
                                }
                                Ok(id) => id,
                            },
                        }
                    } else {
                        progress
                            .println(format!(
                                "Skpping match with missing source ID: {}",
                                caps.get(0)
                                    .and_then(
                                        |full_match| str::from_utf8(full_match.as_bytes()).ok()
                                    )
                                    .unwrap_or("?")
                            ))
                            .ok();
                        return None;
                    }
                };
                let target: PageId = {
                    if let Some(m) = caps.get(2) {
                        match str::from_utf8(m.as_bytes()) {
                            Err(e) => {
                                progress
                                    .println(format!(
                                        "Skipping match with invalid target title bytes: {}",
                                        e
                                    ))
                                    .ok();
                                return None;
                            }
                            Ok(title) => {
                                if let Some(id) = titles.get(title) {
                                    *id
                                } else {
                                    return None;
                                }
                            }
                        }
                    } else {
                        progress
                            .println(format!(
                                "Skpping match with missing target title: {}",
                                caps.get(0)
                                    .and_then(
                                        |full_match| str::from_utf8(full_match.as_bytes()).ok()
                                    )
                                    .unwrap_or("?")
                            ))
                            .ok();
                        return None;
                    }
                };
                Some((source, target))
            },
            |redirects: &mut HashMap<PageId, PageId>, redirect: (PageId, PageId)| {
                redirects.insert(redirect.0, redirect.1);
            },
            mc,
            thread_count,
        )?;

        let mut clean_redirects: HashMap<PageId, PageId> = HashMap::new();
        for (source, target) in &redirects {
            let source = *source;
            let mut target = *target;
            if redirects.contains_key(&target) {
                let mut encountered: HashSet<PageId> = HashSet::from([target]);
                while let Some(new_target) = redirects.get(&target) {
                    if encountered.contains(new_target) {
                        break;
                    }
                    target = *new_target;
                    encountered.insert(target);
                }
            }
            clean_redirects.insert(source, target);
        }

        Ok(redirects)
    }

    pub fn parse_link_dump_file(
        &self,
        thread_count: usize,
        titles: Arc<HashMap<String, PageId>>,
        redirects: Arc<HashMap<PageId, PageId>>,
        progress: MultiProgress,
    ) -> Result<(
        HashMap<PageId, HashSet<PageId>>,
        HashMap<PageId, HashSet<PageId>>,
    )> {
        let mc = progress.clone();
        Self::parse_dump_file(
            self.pagelinks.clone(),
            Regex::new(r"\(([0-9]{1,10}),0,'(.{1,255}?)',0\)").unwrap(), // https://www.mediawiki.org/wiki/Manual:Pagelinks_table
            1 + 10 + 4 + 255 + 4,
            move |caps| {
                let source: PageId = {
                    if let Some(m) = caps.get(1) {
                        match str::from_utf8(m.as_bytes()) {
                            Err(e) => {
                                progress
                                    .println(format!(
                                        "Skipping match with invalid source ID bytes: {}",
                                        e
                                    ))
                                    .ok();
                                return None;
                            }
                            Ok(raw) => match raw.parse::<PageId>() {
                                Err(e) => {
                                    if matches!(e.kind(), std::num::IntErrorKind::PosOverflow) {
                                        progress
                                            .println(format!(
                                                "Skipping match with too large source ID: {}",
                                                raw
                                            ))
                                            .ok();
                                    } else {
                                        progress
                                            .println(format!(
                                                "Skipping match with invalid source ID: {}",
                                                raw
                                            ))
                                            .ok();
                                    }
                                    return None;
                                }
                                Ok(id) => *redirects.get(&id).unwrap_or(&id),
                            },
                        }
                    } else {
                        progress
                            .println(format!(
                                "Skpping match with missing source ID: {}",
                                caps.get(0)
                                    .and_then(
                                        |full_match| str::from_utf8(full_match.as_bytes()).ok()
                                    )
                                    .unwrap_or("?")
                            ))
                            .ok();
                        return None;
                    }
                };
                let target: PageId = {
                    if let Some(m) = caps.get(2) {
                        match str::from_utf8(m.as_bytes()) {
                            Err(e) => {
                                progress
                                    .println(format!(
                                        "Skipping match with invalid target title bytes: {}",
                                        e
                                    ))
                                    .ok();
                                return None;
                            }
                            Ok(title) => {
                                if let Some(id) = titles.get(title) {
                                    *redirects.get(id).unwrap_or(id)
                                } else {
                                    return None;
                                }
                            }
                        }
                    } else {
                        progress
                            .println(format!(
                                "Skpping match with missing target title: {}",
                                caps.get(0)
                                    .and_then(
                                        |full_match| str::from_utf8(full_match.as_bytes()).ok()
                                    )
                                    .unwrap_or("?")
                            ))
                            .ok();
                        return None;
                    }
                };
                if source != target {
                    Some((source, target))
                } else {
                    None
                }
            },
            |links: &mut (
                HashMap<PageId, HashSet<PageId>>,
                HashMap<PageId, HashSet<PageId>>,
            ),
             link: (PageId, PageId)| {
                links.0.entry(link.1).or_default().insert(link.0);
                links.1.entry(link.0).or_default().insert(link.1);
            },
            mc,
            thread_count,
        )
    }
}
