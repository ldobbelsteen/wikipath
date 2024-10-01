use anyhow::{anyhow, bail, Context, Result};
use data_encoding::HEXLOWER;
use regex::Regex;
use ring::digest;
use std::{
    fs::File,
    io::{BufReader, Read, Write},
    path::{Path, PathBuf},
};

use crate::database::Metadata;

/// Struct to hold paths to the local dump files.
#[derive(Debug)]
pub struct TableDumpFiles {
    pub page: PathBuf,
    pub redirect: PathBuf,
    pub pagelinks: PathBuf,
    pub linktarget: PathBuf,
}

/// Metadata of a single external dump file.
#[derive(Debug)]
struct ExternalFile {
    full_name: String,
    language_code: String,
    date_code: String,
    hash: String,
}

/// Struct to hold the metadatas of the external dump files.
#[derive(Debug)]
pub struct ExternalTableDumpFiles {
    page: ExternalFile,
    redirect: ExternalFile,
    pagelinks: ExternalFile,
    linktarget: ExternalFile,
}

impl ExternalTableDumpFiles {
    pub fn get_metadata(&self) -> Metadata {
        Metadata {
            language_code: self.page.language_code.clone(),
            date_code: self.page.date_code.clone(),
        }
    }
}

impl TableDumpFiles {
    /// Get metadatas of the dump files from Wikimedia.
    pub async fn get_external(
        language_code: &str,
        date_code: &str,
    ) -> Result<ExternalTableDumpFiles> {
        fn find_hash(hashes: &str, re: &Regex) -> Option<ExternalFile> {
            hashes
                .lines()
                .find(|line| re.is_match(line))
                .and_then(|line| {
                    let caps = re.captures(line)?;
                    Some(ExternalFile {
                        full_name: caps.get(2)?.as_str().to_string(),
                        language_code: caps.get(3)?.as_str().to_string(),
                        date_code: caps.get(4)?.as_str().to_string(),
                        hash: caps.get(1)?.as_str().to_string(),
                    })
                })
        }

        let url = format!(
            "https://dumps.wikimedia.org/{language_code}wiki/{date_code}/{language_code}wiki-{date_code}-sha1sums.txt"
        );
        let resp = reqwest::get(url).await?;
        let hashes = resp.text().await?;

        let page = find_hash(
            &hashes,
            &Regex::new(r"([0-9a-f]{40})  ((.+)wiki-([0-9]{8})-page.sql.gz)")?,
        )
        .context("missing page dump in sums file")?;

        let redirect = find_hash(
            &hashes,
            &Regex::new(r"([0-9a-f]{40})  ((.+)wiki-([0-9]{8})-redirect.sql.gz)")?,
        )
        .context("missing redirect dump in sums file")?;

        let pagelinks = find_hash(
            &hashes,
            &Regex::new(r"([0-9a-f]{40})  ((.+)wiki-([0-9]{8})-pagelinks.sql.gz)")?,
        )
        .context("missing pagelinks dump in sums file")?;

        let linktarget = find_hash(
            &hashes,
            &Regex::new(r"([0-9a-f]{40})  ((.+)wiki-([0-9]{8})-linktarget.sql.gz)")?,
        )
        .context("missing linktarget dump in sums file")?;

        Ok(ExternalTableDumpFiles {
            page,
            redirect,
            pagelinks,
            linktarget,
        })
    }

    /// Download all relevant dump files from Wikimedia into a directory.
    pub async fn download_external(
        dumps_dir: &Path,
        files: ExternalTableDumpFiles,
    ) -> Result<Self> {
        log::info!("downloading dump files");
        let page = Self::download_external_file(dumps_dir, &files.page).await?;
        let redirect = Self::download_external_file(dumps_dir, &files.redirect).await?;
        let pagelinks = Self::download_external_file(dumps_dir, &files.pagelinks).await?;
        let linktarget = Self::download_external_file(dumps_dir, &files.linktarget).await?;

        log::info!("checking dump file hashes");
        check_file_hash(&page, &files.page.hash)?;
        check_file_hash(&redirect, &files.redirect.hash)?;
        check_file_hash(&pagelinks, &files.pagelinks.hash)?;
        check_file_hash(&linktarget, &files.linktarget.hash)?;

        Ok(Self {
            page,
            redirect,
            pagelinks,
            linktarget,
        })
    }

    /// Download a single file from Wikimedia into a directory.
    async fn download_external_file(
        dumps_dir: &Path,
        external_file: &ExternalFile,
    ) -> Result<PathBuf> {
        let target = dumps_dir.join(&external_file.full_name);
        let mut file = {
            if target.exists() {
                File::options().append(true).open(&target)
            } else {
                std::fs::create_dir_all(dumps_dir)?;
                File::create(&target)
            }
        }?;

        let client = reqwest::Client::new();
        let url = format!(
            "https://dumps.wikimedia.org/{}wiki/{}/{}",
            external_file.language_code, external_file.date_code, external_file.full_name,
        );

        let head_resp = client.head(&url).send().await?;
        let existing_bytes = file.metadata()?.len();
        let total_bytes = head_resp
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|h| h.to_str().ok().and_then(|s| s.parse().ok()))
            .context(format!("missing Content-Length header at '{url}'"))?;

        if existing_bytes < total_bytes {
            let mut resp = client
                .get(&url)
                .header(reqwest::header::RANGE, format!("bytes={existing_bytes}-"))
                .send()
                .await?;

            if !resp.status().is_success() {
                return Err(anyhow!(
                    "failed to download '{}' with status '{}'",
                    url,
                    resp.status()
                ));
            }

            while let Some(chunk) = resp.chunk().await? {
                file.write_all(&chunk)?;
            }

            file.flush()?;
        }

        Ok(target)
    }
}

/// Check whether the hash of a file matches with a given hash.
fn check_file_hash(path: &Path, hash: &str) -> Result<()> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(&file);
    let mut context = digest::Context::new(&digest::SHA1_FOR_LEGACY_USE_ONLY);
    let mut buffer = [0; 8192];

    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        context.update(&buffer[..count]);
    }

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
