use crate::progress;
use data_encoding::HEXLOWER;
use error_chain::error_chain;
use futures::try_join;
use futures_util::StreamExt;
use indicatif::MultiProgress;
use regex::Regex;
use ring::digest;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::{self, BufReader, Read, Write},
    path::PathBuf,
    str,
};

error_chain! {
    foreign_links {
        Io(io::Error);
        Http(reqwest::Error);
    }

    errors {
        HashMismatch(file: PathBuf, digest: String, target: String) {
            display("expected hash of '{}' to be '{}', but was '{}'", file.display(), target, digest)
        }
        MissingDumpType(dump_type: String) {
            display("missing dump type '{}'", dump_type)
        }
        MissingContentLength(url: String) {
            display("missing content length header for '{}'", url)
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    pub dump_date: String,
    pub language_code: String,
    pages: ExternalFile,
    redirects: ExternalFile,
    pagelinks: ExternalFile,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExternalFile {
    full_name: String,
    language_code: String,
    dump_date: String,
    hash: String,
}

#[derive(Debug)]
pub struct Dump {
    pub pages: PathBuf,
    pub redirects: PathBuf,
    pub pagelinks: PathBuf,
    pub metadata: Metadata,
}

impl Dump {
    pub async fn latest_metadata(language_code: &str) -> Result<Metadata> {
        fn find_hash(hashes: &str, re: Regex) -> Option<ExternalFile> {
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
            "https://dumps.wikimedia.org/{}wiki/latest/{}wiki-latest-sha1sums.txt",
            language_code, language_code
        );
        let resp = reqwest::get(url).await?;
        let hashes = resp.text().await?;

        let page = find_hash(
            &hashes,
            Regex::new(r"([0-9a-f]{40})  ((.+)wiki-([0-9]{8})-page.sql.gz)").unwrap(),
        )
        .ok_or(ErrorKind::MissingDumpType("page".to_string()))?;
        let redir = find_hash(
            &hashes,
            Regex::new(r"([0-9a-f]{40})  ((.+)wiki-([0-9]{8})-redirect.sql.gz)").unwrap(),
        )
        .ok_or(ErrorKind::MissingDumpType("redirect".to_string()))?;
        let link = find_hash(
            &hashes,
            Regex::new(r"([0-9a-f]{40})  ((.+)wiki-([0-9]{8})-pagelinks.sql.gz)").unwrap(),
        )
        .ok_or(ErrorKind::MissingDumpType("pagelinks".to_string()))?;

        Ok(Metadata {
            dump_date: page.dump_date.to_string(),
            language_code: page.language_code.to_string(),
            pages: page,
            redirects: redir,
            pagelinks: link,
        })
    }

    pub async fn download(dumps_dir: &PathBuf, metadata: Metadata) -> Result<Self> {
        fs::create_dir_all(&dumps_dir)?;

        let progress = MultiProgress::new();
        let step = progress.add(progress::spinner("Downloading latest dump".into()));
        let (pages, redirects, pagelinks) = try_join!(
            Self::download_file(&dumps_dir, &metadata.pages, progress.clone()),
            Self::download_file(&dumps_dir, &metadata.redirects, progress.clone()),
            Self::download_file(&dumps_dir, &metadata.pagelinks, progress.clone())
        )?;
        step.finish();

        let step = progress.add(progress::spinner("Hashing latest dump".into()));
        try_join!(
            Self::confirm_hash(&pages, &metadata.pages.hash, progress.clone()),
            Self::confirm_hash(&redirects, &metadata.redirects.hash, progress.clone()),
            Self::confirm_hash(&pagelinks, &metadata.pagelinks.hash, progress.clone())
        )?;
        step.finish();

        Ok(Self {
            pages,
            redirects,
            pagelinks,
            metadata,
        })
    }

    async fn download_file(
        dumps_dir: &PathBuf,
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
            .ok_or(ErrorKind::MissingContentLength(url.clone()))?;

        let bar = progress.add(progress::byte(
            &external_file.full_name,
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
                bar.inc(chunk.len() as u64);
            }
            file.flush()?;
        }

        bar.finish();

        Ok(target)
    }

    async fn confirm_hash(path: &PathBuf, hash: &str, progress: MultiProgress) -> Result<()> {
        let file = File::open(&path)?;
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
            return Err(ErrorKind::HashMismatch(path.clone(), digest, hash.into()).into());
        }

        Ok(())
    }
}
