use anyhow::{anyhow, Context, Result};
use heed::types::SerdeBincode;
use heed::{EnvFlags, EnvOpenOptions, PutFlags, RoTxn};
use regex::Regex;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

/// Representation of a page id. The database schema uses 10-digit unsigned integers (<https://www.mediawiki.org/wiki/Manual:Pagelinks_table>).
/// A u32 cannot represent all values a 10-digit integer can, but since not that many Wikipedia articles exist for any language, this should
/// be sufficient and saves memory and disk space.
pub type PageId = u32;

/// Representation of a linktarget table id. The database schema uses 20-digit unsigned integers (<https://www.mediawiki.org/wiki/Manual:Linktarget_table>).
/// A u64 cannot represent all values a 20-digit integer can, but since not that many Wikipedia articles exist for any language, this should
/// be sufficient and saves memory and disk space.
pub type LinkTargetId = u64;

/// A struct containing metadata about a database. The language code represents
/// the Wikipedia language, and the date code represents the dump date.
#[derive(Debug, Serialize, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    pub language_code: String,
    pub date_code: String,
}

impl Metadata {
    /// Extract metadata from the name of a database.
    pub fn from_name(s: &str) -> Result<Self> {
        let re = Regex::new(r"^wp-([a-zA-Z]+)-([0-9]+)$")?;
        if let Some(caps) = re.captures(s) {
            if let Some(language_code) = caps.get(1) {
                if let Some(date_code) = caps.get(2) {
                    return Ok(Metadata {
                        language_code: language_code.as_str().into(),
                        date_code: date_code.as_str().into(),
                    });
                }
            }
        }
        Err(anyhow!("database name '{}' is not valid", s))
    }

    /// Create name containing all database metadata.
    #[must_use]
    pub fn to_name(&self) -> String {
        format!("wp-{}-{}", self.language_code, self.date_code)
    }

    pub fn is_newer(&self, other: &Self) -> bool {
        self.language_code == other.language_code && self.date_code > other.date_code
    }

    pub fn is_older(&self, other: &Self) -> bool {
        self.language_code == other.language_code && self.date_code < other.date_code
    }
}

/// The modes in which a database can be opened.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Mode {
    Serve, // read-only mode for serving shorest path queries
    Build, // read-write mode for building the database
}

#[derive(Debug)]
pub struct Database {
    pub metadata: Metadata,
    mode: Mode,
    env: heed::Env<heed::WithTls>,
    tables: Tables,
}

#[derive(Debug)]
struct Tables {
    redirects: heed::Database<SerdeBincode<PageId>, SerdeBincode<PageId>>,
    incoming: heed::Database<SerdeBincode<PageId>, SerdeBincode<Vec<PageId>>>,
    outgoing: heed::Database<SerdeBincode<PageId>, SerdeBincode<Vec<PageId>>>,
}

impl Database {
    /// Open a database at a path. Returns an error if the database name in the path is not correctly formatted.
    pub fn open(path: &Path, mode: Mode) -> Result<Self> {
        match mode {
            Mode::Serve => {
                if !path.is_file() {
                    return Err(anyhow!(
                        "serve database path '{}' is not a file",
                        path.display()
                    ));
                }
            }
            Mode::Build => {
                if !path.is_dir() {
                    return Err(anyhow!(
                        "build database path '{}' is not a directory",
                        path.display()
                    ));
                }
            }
        }

        let metadata = Self::get_metadata(path)?;

        let env = unsafe {
            EnvOpenOptions::new()
                .max_dbs(3) // redirects, incoming, outgoing
                .map_size(32 * 1024 * 1024 * 1024) // max total database size
                .flags(match mode {
                    Mode::Serve => EnvFlags::NO_SUB_DIR | EnvFlags::READ_ONLY,
                    Mode::Build => EnvFlags::empty(),
                })
                .open(path)?
        };

        let tables = match mode {
            Mode::Build => {
                let mut txn = env.write_txn()?;
                let redirects = env.create_database(&mut txn, Some("redirects"))?;
                let incoming = env.create_database(&mut txn, Some("incoming"))?;
                let outgoing = env.create_database(&mut txn, Some("outgoing"))?;
                txn.commit()?;
                Tables {
                    redirects,
                    incoming,
                    outgoing,
                }
            }
            Mode::Serve => {
                let txn = env.read_txn()?;
                let redirects = env
                    .open_database(&txn, Some("redirects"))?
                    .context("serve database is missing redirects table")?;
                let incoming = env
                    .open_database(&txn, Some("incoming"))?
                    .context("serve database is missing incoming table")?;
                let outgoing = env
                    .open_database(&txn, Some("outgoing"))?
                    .context("serve database is missing outgoing table")?;
                txn.commit()?;
                Tables {
                    redirects,
                    incoming,
                    outgoing,
                }
            }
        };

        Ok(Self {
            metadata,
            mode,
            env,
            tables,
        })
    }

    /// Extract metadata from the filename of a database path.
    pub fn get_metadata(path: &Path) -> Result<Metadata> {
        let filename = path.file_name().and_then(|s| s.to_str()).context(format!(
            "database filename in path '{}' is not valid",
            path.display()
        ))?;

        let metadata = Metadata::from_name(filename)?;
        Ok(metadata)
    }

    /// Create a read transaction on the database. Do not forget to commit the transaction.
    pub fn read_txn(&self) -> Result<heed::RoTxn<'_, heed::WithTls>> {
        Ok(self.env.read_txn()?)
    }

    /// Create a write transaction on the database. Do not forget to commit the transaction.
    /// Only allowed in build mode.
    pub fn write_txn(&self) -> Result<heed::RwTxn<'_>> {
        if self.mode != Mode::Build {
            return Err(anyhow!("write transactions are only allowed in build mode"));
        }

        Ok(self.env.write_txn()?)
    }

    /// Get the redirect of a page.
    pub fn get_redirect(&self, txn: &RoTxn<'_>, page: PageId) -> Result<Option<PageId>> {
        Ok(self.tables.redirects.get(txn, &page)?)
    }

    /// Get the incoming links of a page.
    pub fn get_incoming_links(&self, txn: &RoTxn<'_>, target: PageId) -> Result<Vec<PageId>> {
        Ok(self
            .tables
            .incoming
            .get(txn, &target)?
            .unwrap_or(Vec::new()))
    }

    /// Get the outgoing links of a page.
    pub fn get_outgoing_links(&self, txn: &RoTxn<'_>, source: PageId) -> Result<Vec<PageId>> {
        Ok(self
            .tables
            .outgoing
            .get(txn, &source)?
            .unwrap_or(Vec::new()))
    }

    /// Insert a redirect into the database. Returns an error if the source page already has a redirect.
    pub fn insert_redirect(
        &self,
        txn: &mut heed::RwTxn<'_>,
        source: PageId,
        target: PageId,
    ) -> Result<()> {
        self.tables
            .redirects
            .put_with_flags(txn, PutFlags::NO_OVERWRITE, &source, &target)?;
        Ok(())
    }

    /// Insert links into the database in the form of incoming links. If the target page already has
    /// incoming links, the new links are added to its entry. Returns whether any previous links were
    /// present already (which makes this operation relatively expensive and is thus really not desirable).
    pub fn insert_links_incoming(
        &self,
        txn: &mut heed::RwTxn<'_>,
        target: PageId,
        mut sources: Vec<PageId>,
    ) -> Result<bool> {
        sources.sort_unstable();
        sources.dedup();
        match self.tables.incoming.get_or_put(txn, &target, &sources)? {
            Some(mut existing) => {
                existing.extend(sources);
                existing.sort_unstable();
                existing.dedup();
                self.tables.incoming.put(txn, &target, &existing)?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Generate the outgoing links table. Since it is only possible to insert links in the incoming
    /// form, this function must be called after all links have been inserted to ensure the outgoing
    /// table is also populated. Any previous values in the outgoing table are cleared beforehand.
    pub fn generate_outgoing_table(&self, txn: &mut heed::RwTxn<'_>) -> Result<()> {
        self.tables.outgoing.clear(txn)?;

        log::debug!("building outgoing table entries");
        let mut outgoing: BTreeMap<PageId, Vec<PageId>> = BTreeMap::new(); // here, BTreemap is more memory-dense than HashMap since our page ids are also dense
        for entry in self.tables.incoming.iter(txn)? {
            let (target, sources) = entry?;
            for source in sources {
                outgoing.entry(source).or_default().push(target);
            }
        }

        log::debug!("inserting outgoing table entries");
        for (source, mut targets) in outgoing {
            targets.sort_unstable();
            targets.dedup();
            self.tables.outgoing.put(txn, &source, &targets)?;
        }

        Ok(())
    }

    /// Finish the database by copying it to a file, converting it to a serve database. The database
    /// is compacted in the process. Only works if the current database is a build database. The build
    /// database directory is removed at the end.
    pub fn copy_to_serve(self, path: &Path) -> Result<()> {
        if self.mode != Mode::Build {
            return Err(anyhow!("copying to serve is only allowed in build mode"));
        }

        if path.exists() {
            return Err(anyhow!(
                "serve database path '{}' already exists",
                path.display()
            ));
        }

        log::debug!("copying database to file");
        let mut file = fs::File::create(path)?;
        self.env
            .copy_to_file(&mut file, heed::CompactionOption::Enabled)?;

        log::debug!("removing build database directory");
        let build_path = self.env.path().to_path_buf();
        drop(self);
        std::fs::remove_dir_all(build_path)?;

        Ok(())
    }
}
