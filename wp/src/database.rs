#![allow(clippy::ptr_arg)]

use anyhow::{anyhow, Result};
use heed::types::SerdeBincode;
use heed::EnvOpenOptions;
use regex::Regex;
use serde::Serialize;
use std::path::Path;
use std::vec;

/// A struct containing metadata about a database. The language code represents
/// the Wikipedia language, and the date code represents the dump date.
#[derive(Debug, Serialize, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    pub language_code: String,
    pub date_code: String,
}

impl Metadata {
    /// Extract metadata from the name of a database. Also returns whether the name is temporary.
    fn from_name(s: &str) -> Result<(Self, bool)> {
        let re = Regex::new(r"wp-([a-zA-Z]+)-([0-9]+)(-tmp)?")?;
        if let Some(caps) = re.captures(s) {
            if let Some(language_code) = caps.get(1) {
                if let Some(date_code) = caps.get(2) {
                    let is_tmp = caps.get(3).is_some();
                    return Ok((
                        Metadata {
                            language_code: language_code.as_str().into(),
                            date_code: date_code.as_str().into(),
                        },
                        is_tmp,
                    ));
                }
            }
        }
        Err(anyhow!("database name '{}' is not valid", s))
    }

    /// Create name containing all database metadata.
    pub fn to_name(&self) -> String {
        format!("wp-{}-{}", self.language_code, self.date_code)
    }

    /// Create temp name containing all database metadata.
    pub fn to_tmp_name(&self) -> String {
        format!("wp-{}-{}-tmp", self.language_code, self.date_code)
    }
}

/// Instance of a database environment.
#[derive(Debug)]
pub struct Database {
    env: heed::Env,
    pub metadata: Metadata,
    pub is_tmp: bool,
}

impl Database {
    /// Open a database environment in a directory. If the database directory does not yet exist,
    /// it will be created. Returns an error if the database name in the path is not correctly formatted.
    pub fn open(path: &Path) -> Result<Self> {
        let filename = path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or(anyhow!("database path '{}' is not valid", path.display()))?;
        let (metadata, is_tmp) = Metadata::from_name(filename)?;

        std::fs::create_dir_all(path)?;
        let env = unsafe {
            EnvOpenOptions::new()
                .max_dbs(3) // incoming, outgoing, redirects
                .map_size(64 * 1024 * 1024 * 1024) // 64GB as max database size
                .open(path)?
        };

        Ok(Self {
            env,
            metadata,
            is_tmp,
        })
    }
}

/// Representation of a page id. The database schema uses 10-digit unsigned integers (<https://www.mediawiki.org/wiki/Manual:Pagelinks_table>).
/// A u32 cannot represent all values a 10-digit integer can, but since not that many Wikipedia articles exist for any language, this should
/// be sufficient and saves memory and disk space.
pub type PageId = u32;

/// Representation of a linktarget table id. The database schema uses 20-digit unsigned integers (<https://www.mediawiki.org/wiki/Manual:Linktarget_table>).
/// A u64 cannot represent all values a 20-digit integer can, but since not that many Wikipedia articles exist for any language, this should
/// be sufficient and saves memory and disk space.
pub type LinkTargetId = u64;

/// Schemas for the incoming, outgoing, and redirects databases/tables. The key is the page id, and the value is a list of page ids.
/// The incoming table represents the list of pages that link to a given page, the outgoing table represents the list of pages that a given page links to,
/// and the redirects table represents the mapping of redirecting pages to their target pages.
type HeedIncoming = heed::Database<SerdeBincode<PageId>, SerdeBincode<Vec<PageId>>>;
type HeedOutgoing = heed::Database<SerdeBincode<PageId>, SerdeBincode<Vec<PageId>>>;
type HeedRedirects = heed::Database<SerdeBincode<PageId>, SerdeBincode<PageId>>;

/// A read-only transaction on a database that wraps around an underlying Heed transaction.
/// It provides methods to query incoming and outgoing links and redirects.
pub struct ReadTransaction<'db> {
    inner: heed::RoTxn<'db>,
    incoming: HeedIncoming,
    outgoing: HeedOutgoing,
    redirects: HeedRedirects,
}

impl<'db> ReadTransaction<'db> {
    pub fn begin(db: &'db Database) -> Result<Self> {
        let inner = db.env.read_txn()?;
        let incoming = db
            .env
            .open_database(&inner, Some("incoming"))?
            .ok_or(anyhow!(
                "database '{}' missing incoming data",
                db.metadata.to_name()
            ))?;
        let outgoing = db
            .env
            .open_database(&inner, Some("outgoing"))?
            .ok_or(anyhow!(
                "database '{}' missing outgoing data",
                db.metadata.to_name()
            ))?;
        let redirects = db
            .env
            .open_database(&inner, Some("redirects"))?
            .ok_or(anyhow!(
                "database '{}' missing redirects data",
                db.metadata.to_name()
            ))?;

        Ok(Self {
            inner,
            incoming,
            outgoing,
            redirects,
        })
    }

    pub fn incoming_links(&self, target: PageId) -> Result<Vec<PageId>> {
        Ok(self.incoming.get(&self.inner, &target)?.unwrap_or(vec![]))
    }

    pub fn outgoing_links(&self, source: PageId) -> Result<Vec<PageId>> {
        Ok(self.outgoing.get(&self.inner, &source)?.unwrap_or(vec![]))
    }

    pub fn redirect(&self, page: PageId) -> Result<Option<PageId>> {
        Ok(self.redirects.get(&self.inner, &page)?)
    }
}

/// A write transaction on a database that wraps around an underlying Heed transaction.
/// It provides methods to insert incoming and outgoing links and redirects.
/// The transaction is not committed until the `commit` method is called.
pub struct WriteTransaction<'db> {
    inner: heed::RwTxn<'db>,
    incoming: HeedIncoming,
    outgoing: HeedOutgoing,
    redirects: HeedRedirects,
}

impl<'db> WriteTransaction<'db> {
    pub fn begin(db: &'db Database) -> Result<Self> {
        let mut inner = db.env.write_txn()?;
        let incoming = db.env.create_database(&mut inner, Some("incoming"))?;
        let outgoing = db.env.create_database(&mut inner, Some("outgoing"))?;
        let redirects = db.env.create_database(&mut inner, Some("redirects"))?;

        Ok(Self {
            inner,
            incoming,
            outgoing,
            redirects,
        })
    }

    /// Insert a redirect into the database. Returns an error if the source page already has a redirect.
    pub fn insert_redirect(&mut self, source: &PageId, target: &PageId) -> Result<()> {
        if self
            .redirects
            .get_or_put(&mut self.inner, source, target)?
            .is_some()
        {
            return Err(anyhow!("redirect source already in the database"));
        }
        Ok(())
    }

    /// Insert incoming links into the database. Returns an error if the target page already has incoming links.
    pub fn insert_incoming(&mut self, target: &PageId, sources: &Vec<PageId>) -> Result<()> {
        if self
            .incoming
            .get_or_put(&mut self.inner, target, sources)?
            .is_some()
        {
            return Err(anyhow!("incoming target already in the database"));
        }
        Ok(())
    }

    /// Insert outgoing links into the database. Returns an error if the source page already has outgoing links.
    pub fn insert_outgoing(&mut self, source: &PageId, targets: &Vec<PageId>) -> Result<usize> {
        if self
            .outgoing
            .get_or_put(&mut self.inner, source, targets)?
            .is_some()
        {
            return Err(anyhow!("outgoing source already in the database"));
        }
        Ok(targets.len())
    }

    pub fn commit(self) -> Result<()> {
        self.inner.commit()?;
        Ok(())
    }
}
