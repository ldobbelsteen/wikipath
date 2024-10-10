use crate::database::Metadata;
use anyhow::{anyhow, bail, Context, Result};
use data_encoding::HEXLOWER;
use regex::Regex;
use ring::digest;
use std::{
    fs::{self, File},
    io::{BufReader, Read, Write},
    path::{Path, PathBuf},
};

/// Struct to hold paths to local dump files.
#[derive(Debug)]
pub struct TableDumpFiles {
    pub page: PathBuf,
    pub redirect: PathBuf,
    pub pagelinks: PathBuf,
    pub linktarget: PathBuf,
}

/// Metadata of a single dump file.
#[derive(Debug, Clone)]
struct TableDumpFileMetadata {
    r#type: String,
    language_code: String,
    date_code: String,
    hash: String,
}

impl TableDumpFileMetadata {
    /// Convert the metadata to a normal metadata struct.
    fn to_normal(&self) -> Metadata {
        Metadata {
            language_code: self.language_code.clone(),
            date_code: self.date_code.clone(),
        }
    }

    /// Create a metadata struct from a full filename and a hash.
    pub fn from_full_name_and_hash(full_name: &str, hash: String) -> Result<Self> {
        let re = Regex::new(r"^([a-zA-Z]+)wiki-([0-9]+)-(.+).sql.gz$")?;
        if let Some(caps) = re.captures(full_name) {
            if let Some(language_code) = caps.get(1) {
                if let Some(date_code) = caps.get(2) {
                    if let Some(typ) = caps.get(3) {
                        return Ok(Self {
                            r#type: typ.as_str().to_string(),
                            language_code: language_code.as_str().to_string(),
                            date_code: date_code.as_str().to_string(),
                            hash,
                        });
                    }
                }
            }
        }
        Err(anyhow!("full name '{}' is not valid", full_name))
    }

    /// Convert the metadata to a full filename.
    pub fn to_full_name(&self) -> String {
        format!(
            "{}wiki-{}-{}.sql.gz",
            self.language_code, self.date_code, self.r#type
        )
    }
}

/// Struct to hold the metadatas of dump files.
#[derive(Debug)]
pub struct TableDumpFileMetadatas {
    page: TableDumpFileMetadata,
    redirect: TableDumpFileMetadata,
    pagelinks: TableDumpFileMetadata,
    linktarget: TableDumpFileMetadata,
}

impl TableDumpFileMetadatas {
    pub fn to_normal(&self) -> Metadata {
        self.page.to_normal() // just pick one, they should all be the same
    }
}

impl TableDumpFiles {
    /// Get metadatas of the dump files from Wikimedia. The date code may be "latest".
    pub async fn get_metadatas(
        language_code: &str,
        date_code: &str,
    ) -> Result<TableDumpFileMetadatas> {
        let url = format!(
            "https://dumps.wikimedia.org/{language_code}wiki/{date_code}/{language_code}wiki-{date_code}-sha1sums.txt"
        );

        let resp = reqwest::get(url).await?;
        let lines = resp.text().await?;

        let lines_split = lines
            .lines()
            .map(|line| {
                line.split_once("  ")
                    .context(format!("invalid line in sums file: '{line}'"))
            })
            .collect::<Result<Vec<_>>>()?;

        let files = lines_split
            .into_iter()
            .filter_map(|(hash, full_name)| {
                match TableDumpFileMetadata::from_full_name_and_hash(full_name, hash.into()) {
                    Ok(md) => Some(md),
                    Err(e) => {
                        log::debug!("skipping invalid dump file metadata: {}", e);
                        None
                    }
                }
            })
            .collect::<Vec<_>>();

        let page = files
            .iter()
            .find(|f| f.r#type == "page")
            .context("missing page dump in sums file")?;

        let redirect = files
            .iter()
            .find(|f| f.r#type == "redirect")
            .context("missing redirect dump in sums file")?;

        let pagelinks = files
            .iter()
            .find(|f| f.r#type == "pagelinks")
            .context("missing pagelinks dump in sums file")?;

        let linktarget = files
            .iter()
            .find(|f| f.r#type == "linktarget")
            .context("missing linktarget dump in sums file")?;

        Ok(TableDumpFileMetadatas {
            page: page.clone(),
            redirect: redirect.clone(),
            pagelinks: pagelinks.clone(),
            linktarget: linktarget.clone(),
        })
    }

    /// Download all relevant dump files from Wikimedia into a directory.
    pub async fn download(dumps_dir: &Path, metadatas: TableDumpFileMetadatas) -> Result<Self> {
        log::info!("downloading dump files");
        let page = Self::download_single(dumps_dir, &metadatas.page).await?;
        let redirect = Self::download_single(dumps_dir, &metadatas.redirect).await?;
        let pagelinks = Self::download_single(dumps_dir, &metadatas.pagelinks).await?;
        let linktarget = Self::download_single(dumps_dir, &metadatas.linktarget).await?;

        log::info!("checking dump file hashes");
        check_file_hash(&page, &metadatas.page.hash)?;
        check_file_hash(&redirect, &metadatas.redirect.hash)?;
        check_file_hash(&pagelinks, &metadatas.pagelinks.hash)?;
        check_file_hash(&linktarget, &metadatas.linktarget.hash)?;

        Ok(Self {
            page,
            redirect,
            pagelinks,
            linktarget,
        })
    }

    /// Download a single file from Wikimedia into a directory.
    async fn download_single(
        dumps_dir: &Path,
        metadata: &TableDumpFileMetadata,
    ) -> Result<PathBuf> {
        let target = dumps_dir.join(metadata.to_full_name());
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
            metadata.language_code,
            metadata.date_code,
            metadata.to_full_name(),
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

    /// Remove dump files with different date in the given directory.
    pub fn remove_different_date_dump_files(anchor: &Metadata, dir: &Path) -> Result<()> {
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            let filename = path.file_name().and_then(|s| s.to_str()).context(format!(
                "dump file filename in path '{}' is not valid",
                path.display()
            ))?;

            match TableDumpFileMetadata::from_full_name_and_hash(filename, String::new()) {
                Ok(md) => {
                    if md.language_code == anchor.language_code && md.date_code != anchor.date_code
                    {
                        fs::remove_file(&path)?;
                        log::info!("removed dump file with different date '{}'", path.display());
                    }
                }
                Err(_) => {
                    log::debug!("skipping non-dump file path '{}'", path.display());
                }
            }
        }

        Ok(())
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
