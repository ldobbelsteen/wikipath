use crate::database::{Links, PageId, Redirects, Titles};
use data_encoding::HEXLOWER;
use error_chain::error_chain;
use flate2::read::GzDecoder;
use futures::try_join;
use futures_util::StreamExt;
use regex::Regex;
use ring::digest::{Context, SHA1_FOR_LEGACY_USE_ONLY};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{self, BufReader, Read, Write},
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
        HashMismatch(digest: String, target: String) {
            description("hash mismatch")
            display("mismatch between '{}' and '{}'", digest, target)
        }
        InvalidPageId(page_id: String) {
            description("invalid page id")
            display("invalid page id '{}'", page_id)
        }
        PageIdTooLarge(page_id: String) {
            description("page id too large")
            display("page id is too large '{}'", page_id)
        }
        DumpIncomplete(dump_type: String) {
            description("dump incomplete")
            display("dump does not have type (yet) '{}'", dump_type)
        }
        InvalidPath(path: PathBuf) {
            description("could not parse path")
            display("could not parse path into string: '{}'", path.display())
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
pub struct Dump {
    page_file: String,
    redir_file: String,
    link_file: String,
    pub lang_code: String,
    pub date: String,
}

impl Dump {
    pub async fn download(dumps_dir: &str, lang_code: &str) -> Result<Self> {
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

        let base_re = |n: &str| format!(r"([0-9a-f]{{40}})  ((.+)wiki-([0-9]{{8}})-{}.sql.gz)", n);
        let page = find_hash(&hashes, Regex::new(&base_re("page"))?)
            .ok_or(ErrorKind::DumpIncomplete("page".to_string()))?;
        let redir = find_hash(&hashes, Regex::new(&base_re("redirect"))?)
            .ok_or(ErrorKind::DumpIncomplete("redirect".to_string()))?;
        let link = find_hash(&hashes, Regex::new(&base_re("pagelinks"))?)
            .ok_or(ErrorKind::DumpIncomplete("pagelinks".to_string()))?;

        let (page_file, redir_file, link_file) = try_join!(
            Self::download_file(&page, dumps_dir),
            Self::download_file(&redir, dumps_dir),
            Self::download_file(&link, dumps_dir)
        )?;

        Ok(Self {
            page_file: page_file
                .to_str()
                .ok_or(io::Error::from(io::ErrorKind::NotFound))?
                .to_string(),
            redir_file: redir_file
                .to_str()
                .ok_or(io::Error::from(io::ErrorKind::NotFound))?
                .to_string(),
            link_file: link_file
                .to_str()
                .ok_or(io::Error::from(io::ErrorKind::NotFound))?
                .to_string(),
            lang_code: page.lang_code,
            date: page.date,
        })
    }

    async fn download_file(file: &ExternalDumpFile, dumps_dir: &str) -> Result<PathBuf> {
        let target = Path::new(dumps_dir).join(&file.full_name);
        if !target.exists() {
            let url = format!(
                "https://dumps.wikimedia.org/{}wiki/{}/{}",
                file.lang_code, file.date, file.full_name,
            );
            println!("Downloading {}...", url);
            let resp = reqwest::get(url).await?;

            let mut file = File::create(target.clone())?;
            let mut stream = resp.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                file.write_all(&chunk)?;
            }
            file.flush()?;
        }

        println!("Hashing {}...", target.display());
        let local_hash = Self::sha1_file(&target)?;
        if local_hash != file.hash {
            return Err(ErrorKind::HashMismatch(local_hash, file.hash.to_string()).into());
        }

        Ok(target)
    }

    fn sha1_file(file: &PathBuf) -> Result<String> {
        let file = File::open(file)?;
        let mut reader = BufReader::new(file);
        let mut context = Context::new(&SHA1_FOR_LEGACY_USE_ONLY);
        let mut buffer = [0; 2048];

        loop {
            let count = reader.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            context.update(&buffer[..count]);
        }

        let digest = context.finish();
        Ok(HEXLOWER.encode(digest.as_ref()))
    }

    pub fn parse_page_dump_file(&self) -> Result<Titles> {
        Self::parse_dump_file(
            &self.page_file,
            r"\(([0-9]{1,10}),0,'(.{1,255}?)',[01],[01],[0-9.]+?,'[0-9]+?',(?:'[0-9]+?'|NULL),[0-9]{1,10},[0-9]{1,10},'wikitext',NULL\)",
            2048,
            &mut [0; 65536],
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
                        return Ok(println!(
                            "Skipping page match with missing ID: {}",
                            if let Some(full_match) = caps.get(0) {
                                from_utf8(full_match.as_bytes())?
                            } else {
                                "?"
                            }
                        ));
                    }
                };
                let title = {
                    if let Some(m) = caps.get(2) {
                        from_utf8(m.as_bytes())?
                    } else {
                        return Ok(println!(
                            "Skipping page match with missing title: {}",
                            if let Some(full_match) = caps.get(0) {
                                from_utf8(full_match.as_bytes())?
                            } else {
                                "?"
                            }
                        ));
                    }
                };
                titles.insert(title.to_string(), id);
                Ok(())
            },
        )
    }

    pub fn parse_redir_dump_file(&self, titles: &Titles) -> Result<Redirects> {
        let redirects = Self::parse_dump_file(
            &self.redir_file,
            r"\(([0-9]{1,10}),0,'(.{1,255}?)','.{0,32}?','.{0,255}?'\)",
            1536,
            &mut [0; 49152],
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
                        return Ok(println!(
                            "Skipping redirect match with missing source: {}",
                            if let Some(full_match) = caps.get(0) {
                                from_utf8(full_match.as_bytes())?
                            } else {
                                "?"
                            }
                        ));
                    }
                };
                let target: PageId = {
                    if let Some(m) = caps.get(2) {
                        let title = from_utf8(m.as_bytes())?;
                        if let Some(page) = titles.get(title) {
                            *page
                        } else {
                            return Ok(());
                            // return Ok(println!(
                            //     "Skipping redirect match with unknown target title: {}",
                            //     if let Some(full_match) = caps.get(0) {
                            //         from_utf8(full_match.as_bytes())?
                            //     } else {
                            //         "?"
                            //     }
                            // ));
                        }
                    } else {
                        return Ok(println!(
                            "Skipping redirect match with missing target: {}",
                            if let Some(full_match) = caps.get(0) {
                                from_utf8(full_match.as_bytes())?
                            } else {
                                "?"
                            }
                        ));
                    }
                };
                redirs.insert(source, target);
                Ok(())
            },
        )?;

        let mut clean_redirects: HashMap<u32, u32> = HashMap::new();
        for (source, target) in &redirects {
            let source = *source;
            let mut target = *target;
            if redirects.contains_key(&target) {
                let mut encountered = HashSet::from([target]);
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

    pub fn parse_link_dump_file(&self, titles: &Titles, redirs: &Redirects) -> Result<Links> {
        Self::parse_dump_file(
            &self.link_file,
            r"\(([0-9]{1,10}),0,'(.{1,255}?)',0\)",
            1024,
            &mut [0; 32768],
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
                        return Ok(println!(
                            "Skipping link match with missing source: {:?}",
                            if let Some(full_match) = caps.get(0) {
                                from_utf8(full_match.as_bytes())?
                            } else {
                                "?"
                            }
                        ));
                    }
                };
                let target: PageId = {
                    if let Some(m) = caps.get(2) {
                        let title = from_utf8(m.as_bytes())?;
                        if let Some(page) = titles.get(title) {
                            *redirs.get(page).unwrap_or(page)
                        } else {
                            return Ok(());
                            // return Ok(println!(
                            //     "Skipping link match with unknown target title: {}",
                            //     if let Some(full_match) = caps.get(0) {
                            //         from_utf8(full_match.as_bytes())?
                            //     } else {
                            //         "?"
                            //     }
                            // ));
                        }
                    } else {
                        return Ok(println!(
                            "Skipping link match with missing target: {:?}",
                            if let Some(full_match) = caps.get(0) {
                                from_utf8(full_match.as_bytes())?
                            } else {
                                "?"
                            }
                        ));
                    }
                };
                if source != target {
                    links.incoming.entry(target).or_default().insert(source);
                    links.outgoing.entry(source).or_default().insert(target);
                }
                Ok(())
            },
        )
    }

    fn parse_dump_file<T: Default, U>(
        path: &str,
        regex: &str,
        max_regex_size: usize,
        buffer: &mut [u8],
        output: U,
    ) -> Result<T>
    where
        U: Fn(&mut T, regex::bytes::Captures) -> Result<()>,
    {
        let file = File::open(path)?;
        let decoder = GzDecoder::new(file);
        let mut reader = BufReader::new(decoder);
        let regex = regex::bytes::Regex::new(regex)?;
        let mut result = Default::default();

        loop {
            let bytes_read = reader.read(&mut buffer[max_regex_size..])?;
            if bytes_read == 0 {
                break;
            }
            for caps in regex.captures_iter(&buffer[..max_regex_size + bytes_read]) {
                match output(&mut result, caps) {
                    Err(Error(ErrorKind::PageIdTooLarge(_), _)) => {}
                    Err(e) => return Err(e),
                    Ok(_) => {}
                }
            }
            if bytes_read > max_regex_size {
                let (dst, src) = buffer.split_at_mut(bytes_read);
                dst[..max_regex_size].copy_from_slice(&src[..max_regex_size]);
            }
        }

        Ok(result)
    }
}
