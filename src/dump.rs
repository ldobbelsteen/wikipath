use crate::progress;
use anyhow::{anyhow, bail, Result};
use data_encoding::HEXLOWER;
use futures::try_join;
use futures_util::StreamExt;
use indicatif::MultiProgress;
use regex::Regex;
use ring::digest;
use std::{
    fs::File,
    io::{BufReader, Read, Write},
    path::{Path, PathBuf},
    str,
};

#[derive(Debug)]
pub struct Dump {
    pub pages: PathBuf,
    pub redirects: PathBuf,
    pub pagelinks: PathBuf,
    pub external: ExternalDumpFiles,
}

#[derive(Debug)]
pub struct ExternalDumpFiles {
    pages: ExternalFile,
    redirects: ExternalFile,
    pagelinks: ExternalFile,
}

#[derive(Debug)]
struct ExternalFile {
    full_name: String,
    language_code: String,
    dump_date: String,
    hash: String,
}

impl ExternalDumpFiles {
    pub fn get_language_code(&self) -> String {
        self.pages.language_code.clone()
    }

    pub fn get_dump_date(&self) -> String {
        self.pages.dump_date.clone()
    }
}

impl Dump {
    /// Get information on the newest available dump from Wikimedia.
    pub async fn get_latest_external(language_code: &str) -> Result<ExternalDumpFiles> {
        fn find_hash(hashes: &str, re: &Regex) -> Option<ExternalFile> {
            hashes
                .lines()
                .find(|line| re.is_match(line))
                .and_then(|line| {
                    let caps = re.captures(line)?;
                    Some(ExternalFile {
                        full_name: caps.get(2)?.as_str().to_string(),
                        language_code: caps.get(3)?.as_str().to_string(),
                        dump_date: caps.get(4)?.as_str().to_string(),
                        hash: caps.get(1)?.as_str().to_string(),
                    })
                })
        }

        let url = format!(
            "https://dumps.wikimedia.org/{language_code}wiki/latest/{language_code}wiki-latest-sha1sums.txt",
            
        );
        let resp = reqwest::get(url).await?;
        let hashes = resp.text().await?;

        let page = find_hash(
            &hashes,
            &Regex::new(r"([0-9a-f]{40})  ((.+)wiki-([0-9]{8})-page.sql.gz)")?,
        )
        .ok_or(anyhow!("missing page dump in sums file"))?;
        let redir = find_hash(
            &hashes,
            &Regex::new(r"([0-9a-f]{40})  ((.+)wiki-([0-9]{8})-redirect.sql.gz)")?,
        )
        .ok_or(anyhow!("missing redirect dump in sums file"))?;
        let link = find_hash(
            &hashes,
            &Regex::new(r"([0-9a-f]{40})  ((.+)wiki-([0-9]{8})-pagelinks.sql.gz)")?,
        )
        .ok_or(anyhow!("missing pagelinks dump in sums file"))?;

        Ok(ExternalDumpFiles {
            pages: page,
            redirects: redir,
            pagelinks: link,
        })
    }

    /// Download all relevant dump files from Wikimedia into a directory.
    pub async fn download_external(
        dumps_dir: &Path,
        files: ExternalDumpFiles,
        progress: MultiProgress,
    ) -> Result<Self> {
        std::fs::create_dir_all(dumps_dir)?;
        let step = progress.add(progress::spinner("Downloading latest dump"));
        let (pages, redirects, pagelinks) = try_join!(
            Self::download_external_file(dumps_dir, &files.pages, progress.clone()),
            Self::download_external_file(dumps_dir, &files.redirects, progress.clone()),
            Self::download_external_file(dumps_dir, &files.pagelinks, progress.clone())
        )?;
        step.finish();

        let step = progress.add(progress::spinner("Hashing latest dump"));
        Self::check_file_hash(&pages, &files.pages.hash, &progress)?;
        Self::check_file_hash(&redirects, &files.redirects.hash, &progress)?;
        Self::check_file_hash(&pagelinks, &files.pagelinks.hash, &progress)?;
        step.finish();

        Ok(Self {
            pages,
            redirects,
            pagelinks,
            external: files,
        })
    }

    /// Download a single file from Wikimedia into a directory.
    async fn download_external_file(
        dumps_dir: &Path,
        external_file: &ExternalFile,
        progress: MultiProgress,
    ) -> Result<PathBuf> {
        let target = dumps_dir.join(&external_file.full_name);
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
            external_file.language_code, external_file.dump_date, external_file.full_name,
        );

        let head_resp = client.head(&url).send().await?;
        let existing_bytes = file.metadata()?.len();
        let total_bytes = head_resp
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|h| h.to_str().ok().and_then(|s| s.parse().ok()))
            .ok_or(anyhow!("missing Content-Length header at '{}'", url))?;

        let bar = progress.add(progress::byte(
            &external_file.full_name,
            existing_bytes,
            total_bytes,
        ));

        if existing_bytes < total_bytes {
            let resp = client
                .get(&url)
                .header(reqwest::header::RANGE, format!("bytes={existing_bytes}-"))
                .send()
                .await?;

            let mut stream = resp.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                file.write_all(&chunk)?;
                bar.inc(chunk.len() as u64);
            }
            file.flush()?;
        }

        bar.finish();

        Ok(target)
    }

    /// Check whether the hash of a file matches with a given hash.
    fn check_file_hash(path: &Path, hash: &str, progress: &MultiProgress) -> Result<()> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(&file);
        let mut context = digest::Context::new(&digest::SHA1_FOR_LEGACY_USE_ONLY);
        let mut buffer = [0; 8192];

        let bar = progress.add(progress::byte(
            &format!("{}", path.display()),
            0,
            file.metadata()?.len(),
        ));

        loop {
            let count = reader.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            bar.inc(count as u64);
            context.update(&buffer[..count]);
        }

        bar.finish();
        let digest = HEXLOWER.encode(context.finish().as_ref());
        if digest != hash {
            bail!(
                "file '{}' hash mismatch between digest {} and target {}",
                path.display(),
                digest,
                hash
            );
        }

        Ok(())
    }
}
