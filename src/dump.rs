use crate::{
    database::{Links, PageId, Redirects, Titles},
    progress::{file_progress, multi_progress, step_progress, ProgressReader},
};
use data_encoding::HEXLOWER;
use error_chain::error_chain;
use flate2::read::GzDecoder;
use futures::try_join;
use futures_util::StreamExt;
use hashbrown::{HashMap, HashSet};
use indicatif::MultiProgress;
use regex::Regex;
use ring::digest::{Context, SHA1_FOR_LEGACY_USE_ONLY};
use std::{
    fs::File,
    io::{BufReader, Read, Write},
    path::{Path, PathBuf},
    str::from_utf8,
};

error_chain! {
    foreign_links {
        Io(std::io::Error);
        Http(reqwest::Error);
        Regex(regex::Error);
        Utf8(std::str::Utf8Error);
    }

    errors {
        HashMismatch(file: PathBuf, digest: String, target: String) {
            description("hash mismatch")
            display("expected hash of '{}' to be '{}', but was '{}'", file.display(), target, digest)
        }
        InvalidPageId(id: String) {
            description("invalid page id")
            display("invalid page id '{}'", id)
        }
        PageIdTooLarge(id: String) {
            description("page id too large")
            display("page id is too large '{}'", id)
        }
        MissingDumpType(dump: String) {
            description("missing dump type")
            display("missing dump type '{}'", dump)
        }
        MissingContentLength(url: String) {
            description("mising content length")
            display("missing content length header for '{}'", url)
        }
    }
}

#[derive(Debug)]
struct ExternalDumpFile {
    full_name: String,
    lang_code: String,
    date: String,
    hash: String,
}

#[derive(Debug)]
struct ExternalDump {
    page: ExternalDumpFile,
    redir: ExternalDumpFile,
    link: ExternalDumpFile,
}

#[derive(Debug)]
pub struct Dump {
    page_file: String,
    redir_file: String,
    link_file: String,
    pub lang_code: String,
    pub date: String,
}

impl Dump {
    pub async fn download(dumps_dir: &str, lang_code: &str) -> Result<Self> {
        let step = step_progress("Getting latest dump metadata".into());
        let metadata = Self::latest_metadata(lang_code).await?;
        step.finish();

        let progress = multi_progress();
        let step = progress.add(step_progress("Downloading latest dump".into()));
        let (page, redir, link) = try_join!(
            Self::download_file(dumps_dir, &metadata.page, progress.clone()),
            Self::download_file(dumps_dir, &metadata.redir, progress.clone()),
            Self::download_file(dumps_dir, &metadata.link, progress.clone())
        )?;
        step.finish();

        let progress = multi_progress();
        let step = progress.add(step_progress("Hashing latest dump".into()));
        let (page, redir, link) = try_join!(
            Self::confirm_hash(page, metadata.page.hash, progress.clone()),
            Self::confirm_hash(redir, metadata.redir.hash, progress.clone()),
            Self::confirm_hash(link, metadata.link.hash, progress.clone())
        )?;
        step.finish();

        Ok(Self {
            page_file: page.display().to_string(),
            redir_file: redir.display().to_string(),
            link_file: link.display().to_string(),
            lang_code: metadata.page.lang_code,
            date: metadata.page.date,
        })
    }

    async fn latest_metadata(lang_code: &str) -> Result<ExternalDump> {
        fn find_hash(hashes: &str, re: Regex) -> Option<ExternalDumpFile> {
            hashes
                .lines()
                .find(|line| re.is_match(line))
                .and_then(|line| {
                    let caps = re.captures(line)?;
                    Some(ExternalDumpFile {
                        full_name: caps.get(2)?.as_str().to_string(),
                        lang_code: caps.get(3)?.as_str().to_string(),
                        date: caps.get(4)?.as_str().to_string(),
                        hash: caps.get(1)?.as_str().to_string(),
                    })
                })
        }

        let url = format!(
            "https://dumps.wikimedia.org/{}wiki/latest/{}wiki-latest-sha1sums.txt",
            lang_code, lang_code
        );
        let resp = reqwest::get(url).await?;
        let hashes = resp.text().await?;

        let base = |n: &str| format!(r"([0-9a-f]{{40}})  ((.+)wiki-([0-9]{{8}})-{}.sql.gz)", n);
        let page = find_hash(&hashes, Regex::new(&base("page"))?)
            .ok_or(ErrorKind::MissingDumpType("page".to_string()))?;
        let redir = find_hash(&hashes, Regex::new(&base("redirect"))?)
            .ok_or(ErrorKind::MissingDumpType("redirect".to_string()))?;
        let link = find_hash(&hashes, Regex::new(&base("pagelinks"))?)
            .ok_or(ErrorKind::MissingDumpType("pagelinks".to_string()))?;

        Ok(ExternalDump {
            page: page,
            redir: redir,
            link: link,
        })
    }

    async fn download_file(
        dumps_dir: &str,
        external_file: &ExternalDumpFile,
        progress: MultiProgress,
    ) -> Result<PathBuf> {
        let target = Path::new(dumps_dir).join(&external_file.full_name);
        let mut file = {
            if target.exists() {
                File::options().append(true).open(&target)
            } else {
                File::create(&target)
            }
        }?;

        let client = reqwest::Client::new();
        let url = format!(
            "https://dumps.wikimedia.org/{}wiki/{}/{}",
            external_file.lang_code, external_file.date, external_file.full_name,
        );

        let head_resp = client.head(&url).send().await?;
        let existing_bytes = file.metadata()?.len();
        let total_bytes = head_resp
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|h| h.to_str().ok().and_then(|s| s.parse().ok()))
            .ok_or(ErrorKind::MissingContentLength(url.clone()))?;

        let progress = progress.add(file_progress(
            external_file.full_name.clone(),
            existing_bytes,
            total_bytes,
        ));

        if existing_bytes < total_bytes {
            let resp = client
                .get(&url)
                .header(reqwest::header::RANGE, format!("bytes={}-", existing_bytes))
                .send()
                .await?;

            let mut stream = resp.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                file.write_all(&chunk)?;
                progress.inc(chunk.len() as u64);
            }
            file.flush()?;
        }

        progress.finish();

        Ok(target)
    }

    async fn confirm_hash(path: PathBuf, hash: String, progress: MultiProgress) -> Result<PathBuf> {
        let file = File::open(&path)?;
        let mut reader = BufReader::new(&file);
        let mut context = Context::new(&SHA1_FOR_LEGACY_USE_ONLY);
        let mut buffer = [0; 2048];

        let progress = progress.add(file_progress(
            format!("{}", path.display()),
            0,
            file.metadata()?.len(),
        ));

        loop {
            let count = reader.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            progress.inc(count as u64);
            context.update(&buffer[..count]);
        }

        progress.finish();
        let digest = HEXLOWER.encode(context.finish().as_ref());
        if digest != hash {
            Err(ErrorKind::HashMismatch(path.clone(), digest, hash).into())
        } else {
            Ok(path)
        }
    }

    pub fn parse_page_dump_file(&self, progress: MultiProgress) -> Result<Titles> {
        Self::regex_dump(
            &self.page_file,
            r"\(([0-9]{1,10}),0,'(.{1,255}?)',[01],[01],0.[0-9]{1,32}?,'[0-9]{14}',(?:'[0-9]{14}'|NULL),[0-9]{1,10},[0-9]{1,10},'wikitext',NULL\)", // https://www.mediawiki.org/wiki/Manual:Page_table
            1 + 10 + 4 + 255 + 8 + 32 + 2 + 14 + 3 + 14 + 2 + 10 + 1 + 10 + 17,
            |titles: &mut Titles, caps: regex::bytes::Captures| {
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
        titles: &Titles,
        progress: MultiProgress,
    ) -> Result<Redirects> {
        let redirects = Self::regex_dump(
            &self.redir_file,
            r"\(([0-9]{1,10}),0,'(.{1,255}?)','.{0,32}?','.{0,255}?'\)", // https://www.mediawiki.org/wiki/Manual:Redirect_table
            1 + 10 + 4 + 255 + 3 + 32 + 3 + 255 + 2,
            |redirs: &mut Redirects, caps: regex::bytes::Captures| {
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
                let mut encountered: HashSet<u32> = HashSet::from([target]);
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
        titles: &Titles,
        redirs: &Redirects,
        progress: MultiProgress,
    ) -> Result<Links> {
        Self::regex_dump(
            &self.link_file,
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

    fn regex_dump<T: Default, U>(
        path: &str,
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
        let progress = progress.add(file_progress(path.into(), 0, file_size));
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
}
