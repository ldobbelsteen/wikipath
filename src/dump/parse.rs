use super::Dump;
use crate::{
    database::PageId,
    progress::{byte_progress, ProgressReader},
};
use error_chain::error_chain;
use flate2::read::GzDecoder;
use hashbrown::{HashMap, HashSet};
use indicatif::MultiProgress;
use std::{fs::File, io::Read, path::PathBuf, str::from_utf8};

error_chain! {
    foreign_links {
        Io(std::io::Error);
        Regex(regex::Error);
        Utf8(std::str::Utf8Error);
    }

    errors {
        InvalidPageId(id: String) {
            display("invalid page id '{}'", id)
        }
        PageIdTooLarge(id: String) {
            display("page id is too large '{}'", id)
        }
    }
}

#[derive(Debug, Default)]
pub struct Links {
    pub incoming: HashMap<PageId, HashSet<PageId>>,
    pub outgoing: HashMap<PageId, HashSet<PageId>>,
}

impl Dump {
    pub fn regex_dump<T: Default, U>(
        path: &PathBuf,
        raw_regex: &str,
        max_regex_size: usize,
        extract_and_store: U,
        progress: MultiProgress,
    ) -> Result<T>
    where
        U: Fn(&mut T, regex::bytes::Captures) -> Result<()>,
    {
        let file = File::open(path)?;
        let file_size = file.metadata()?.len();
        let progress = progress.add(byte_progress("".into(), 0, file_size));
        let mut reader = GzDecoder::new(ProgressReader::new(file, progress.clone()));
        let mut buffer = [0; 65536];

        let regex = regex::bytes::Regex::new(raw_regex)?;
        let mut result = Default::default();

        loop {
            let bytes_read = reader.read(&mut buffer[max_regex_size..])?;
            if bytes_read == 0 {
                break;
            }
            for caps in regex.captures_iter(&buffer[..max_regex_size + bytes_read]) {
                match extract_and_store(&mut result, caps) {
                    Err(Error(ErrorKind::PageIdTooLarge(_), _)) => {}
                    Err(e) => return Err(e),
                    Ok(_) => {}
                }
            }
            if bytes_read >= max_regex_size {
                let (dst, src) = buffer.split_at_mut(bytes_read);
                dst[..max_regex_size].copy_from_slice(&src[..max_regex_size]);
            }
        }

        progress.finish();
        Ok(result)
    }

    pub fn parse_page_dump_file(&self, progress: MultiProgress) -> Result<HashMap<String, PageId>> {
        Self::regex_dump(
            &self.pages,
            r"\(([0-9]{1,10}),0,'(.{1,255}?)',[01],[01],0.[0-9]{1,32}?,'[0-9]{14}',(?:'[0-9]{14}'|NULL),[0-9]{1,10},[0-9]{1,10},'wikitext',NULL\)", // https://www.mediawiki.org/wiki/Manual:Page_table
            1 + 10 + 4 + 255 + 8 + 32 + 2 + 14 + 3 + 14 + 2 + 10 + 1 + 10 + 17,
            |titles: &mut HashMap<String, PageId>, caps: regex::bytes::Captures| {
                let id: PageId = {
                    if let Some(m) = caps.get(1) {
                        let raw = from_utf8(m.as_bytes())?;
                        raw.parse().map_err(|e: std::num::ParseIntError| {
                            if matches!(e.kind(), std::num::IntErrorKind::PosOverflow) {
                                ErrorKind::PageIdTooLarge(raw.to_string())
                            } else {
                                ErrorKind::InvalidPageId(raw.to_string())
                            }
                        })?
                    } else {
                        progress.println(format!(
                            "Skipping page match with missing ID: {}",
                            if let Some(full_match) = caps.get(0) {
                                from_utf8(full_match.as_bytes())?
                            } else {
                                "?"
                            }
                        ))?;
                        return Ok(());
                    }
                };
                let title = {
                    if let Some(m) = caps.get(2) {
                        from_utf8(m.as_bytes())?
                    } else {
                        progress.println(format!(
                            "Skipping page match with missing title: {}",
                            if let Some(full_match) = caps.get(0) {
                                from_utf8(full_match.as_bytes())?
                            } else {
                                "?"
                            }
                        ))?;
                        return Ok(());
                    }
                };
                titles.insert(title.to_string(), id);
                Ok(())
            },
            progress.clone(),
        )
    }

    pub fn parse_redir_dump_file(
        &self,
        titles: &HashMap<String, PageId>,
        progress: MultiProgress,
    ) -> Result<HashMap<PageId, PageId>> {
        let redirects = Self::regex_dump(
            &self.redirects,
            r"\(([0-9]{1,10}),0,'(.{1,255}?)','.{0,32}?','.{0,255}?'\)", // https://www.mediawiki.org/wiki/Manual:Redirect_table
            1 + 10 + 4 + 255 + 3 + 32 + 3 + 255 + 2,
            |redirs: &mut HashMap<PageId, PageId>, caps: regex::bytes::Captures| {
                let source: PageId = {
                    if let Some(m) = caps.get(1) {
                        let raw = from_utf8(m.as_bytes())?;
                        raw.parse().map_err(|e: std::num::ParseIntError| {
                            if matches!(e.kind(), std::num::IntErrorKind::PosOverflow) {
                                ErrorKind::PageIdTooLarge(raw.to_string())
                            } else {
                                ErrorKind::InvalidPageId(raw.to_string())
                            }
                        })?
                    } else {
                        progress.println(format!(
                            "Skipping redirect match with missing source: {}",
                            if let Some(full_match) = caps.get(0) {
                                from_utf8(full_match.as_bytes())?
                            } else {
                                "?"
                            }
                        ))?;
                        return Ok(());
                    }
                };
                let target: PageId = {
                    if let Some(m) = caps.get(2) {
                        let title = from_utf8(m.as_bytes())?;
                        if let Some(page) = titles.get(title) {
                            *page
                        } else {
                            // progress.println(format!(
                            //     "Skipping redirect match with unknown target title: {}",
                            //     if let Some(full_match) = caps.get(0) {
                            //         from_utf8(full_match.as_bytes())?
                            //     } else {
                            //         "?"
                            //     }
                            // ))?;
                            return Ok(());
                        }
                    } else {
                        progress.println(format!(
                            "Skipping redirect match with missing target: {}",
                            if let Some(full_match) = caps.get(0) {
                                from_utf8(full_match.as_bytes())?
                            } else {
                                "?"
                            }
                        ))?;
                        return Ok(());
                    }
                };
                redirs.insert(source, target);
                Ok(())
            },
            progress.clone(),
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
        titles: &HashMap<String, PageId>,
        redirs: &HashMap<PageId, PageId>,
        progress: MultiProgress,
    ) -> Result<Links> {
        Self::regex_dump(
            &self.pagelinks,
            r"\(([0-9]{1,10}),0,'(.{1,255}?)',0\)", // https://www.mediawiki.org/wiki/Manual:Pagelinks_table
            1 + 10 + 4 + 255 + 4,
            |links: &mut Links, caps: regex::bytes::Captures| {
                let source: PageId = {
                    if let Some(m) = caps.get(1) {
                        let raw = from_utf8(m.as_bytes())?;
                        let source = raw.parse().map_err(|e: std::num::ParseIntError| {
                            if matches!(e.kind(), std::num::IntErrorKind::PosOverflow) {
                                ErrorKind::PageIdTooLarge(raw.to_string())
                            } else {
                                ErrorKind::InvalidPageId(raw.to_string())
                            }
                        })?;
                        *redirs.get(&source).unwrap_or(&source)
                    } else {
                        progress.println(format!(
                            "Skipping link match with missing source: {:?}",
                            if let Some(full_match) = caps.get(0) {
                                from_utf8(full_match.as_bytes())?
                            } else {
                                "?"
                            }
                        ))?;
                        return Ok(());
                    }
                };
                let target: PageId = {
                    if let Some(m) = caps.get(2) {
                        let title = from_utf8(m.as_bytes())?;
                        if let Some(page) = titles.get(title) {
                            *redirs.get(page).unwrap_or(page)
                        } else {
                            // progress.println(format!(
                            //     "Skipping link match with unknown target title: {}",
                            //     if let Some(full_match) = caps.get(0) {
                            //         from_utf8(full_match.as_bytes())?
                            //     } else {
                            //         "?"
                            //     }
                            // ))?;
                            return Ok(());
                        }
                    } else {
                        progress.println(format!(
                            "Skipping link match with missing target: {:?}",
                            if let Some(full_match) = caps.get(0) {
                                from_utf8(full_match.as_bytes())?
                            } else {
                                "?"
                            }
                        ))?;
                        return Ok(());
                    }
                };
                if source != target {
                    links.incoming.entry(target).or_default().insert(source);
                    links.outgoing.entry(source).or_default().insert(target);
                }
                Ok(())
            },
            progress.clone(),
        )
    }
}
