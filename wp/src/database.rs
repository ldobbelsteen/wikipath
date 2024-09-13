use crate::memory::ProcessMemoryUsageChecker;
use anyhow::{anyhow, Result};
use hashbrown::HashMap;
use heed::types::SerdeBincode;
use heed::EnvOpenOptions;
use regex::Regex;
use serde::Serialize;
use std::path::Path;
use std::sync::mpsc::Sender;
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{Scope, ScopedJoinHandle};
use std::time::Duration;
use std::{mem, vec};

#[derive(Debug, Serialize, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    pub language_code: String,
    pub date_code: String,
}

impl Metadata {
    /// Extract metadata from the name of a database. Returns whether the name is temporary.
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

#[derive(Debug)]
pub struct Database {
    env: heed::Env,
    pub metadata: Metadata,
    pub is_tmp: bool,
}

impl Database {
    /// Open a database environment by creating a directory at a path.
    /// Returns an error if the database name in the path is not in the correct format.
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
                .map_size(64 * 1024 * 1024 * 1024) // max database size
                .open(path)?
        };

        Ok(Self {
            env,
            metadata,
            is_tmp,
        })
    }
}

/// Representation of a page id.
pub type PageId = u32;

/// Representation of a linktarget table id.
pub type LinkTargetId = u64;

type HeedIncoming = heed::Database<SerdeBincode<PageId>, SerdeBincode<Vec<PageId>>>;
type HeedOutgoing = heed::Database<SerdeBincode<PageId>, SerdeBincode<Vec<PageId>>>;
type HeedRedirects = heed::Database<SerdeBincode<PageId>, SerdeBincode<PageId>>;

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

    /// Insert incoming links into the database. Returns the number of links inserted.
    /// TODO: detect duplicates
    pub fn insert_incoming(&mut self, target: &PageId, sources: &Vec<PageId>) -> Result<usize> {
        if let Some(mut existing) = self.incoming.get_or_put(&mut self.inner, target, sources)? {
            existing.extend(sources);
            self.incoming.put(&mut self.inner, target, &existing)?;
        }
        Ok(sources.len())
    }

    /// Insert outgoing links into the database. Returns the number of links inserted.
    /// TODO: detect duplicates
    pub fn insert_outgoing(&mut self, source: &PageId, targets: &Vec<PageId>) -> Result<usize> {
        if let Some(mut existing) = self.outgoing.get_or_put(&mut self.inner, source, targets)? {
            existing.extend(targets);
            self.outgoing.put(&mut self.inner, source, &existing)?;
        }
        Ok(targets.len())
    }

    pub fn commit(self) -> Result<()> {
        self.inner.commit()?;
        Ok(())
    }
}

struct LinkBuffer {
    incoming: HashMap<PageId, Vec<PageId>>,
    outgoing: HashMap<PageId, Vec<PageId>>,
}

impl LinkBuffer {
    fn insert(&mut self, source: PageId, target: PageId) {
        self.incoming.entry(target).or_default().push(source);
        self.outgoing.entry(source).or_default().push(target);
    }

    fn take_incoming(&mut self) -> HashMap<PageId, Vec<PageId>> {
        mem::replace(&mut self.incoming, HashMap::new())
    }

    fn take_outgoing(&mut self) -> HashMap<PageId, Vec<PageId>> {
        mem::replace(&mut self.outgoing, HashMap::new())
    }

    fn len(&self) -> usize {
        self.incoming.len() + self.outgoing.len()
    }
}

impl Default for LinkBuffer {
    fn default() -> Self {
        Self {
            incoming: HashMap::new(),
            outgoing: HashMap::new(),
        }
    }
}

pub struct BufferedLinkWriteTransaction<'scope> {
    buffer: Arc<Mutex<LinkBuffer>>,
    inserter: ScopedJoinHandle<'scope, Result<usize>>,
    flush_tx: Sender<()>,
}

impl<'scope> BufferedLinkWriteTransaction<'scope> {
    pub fn begin(
        db: Database,
        process_memory_limit: u64,
        thread_scope: &'scope Scope<'scope, '_>,
    ) -> Result<Self> {
        let (flush_tx, flush_rx) = mpsc::channel::<()>();

        let buffer: Arc<Mutex<LinkBuffer>> = Arc::default();
        let buffer_clone = buffer.clone();

        let mut memory_checker = ProcessMemoryUsageChecker::new()?;
        if memory_checker.get() > process_memory_limit {
            return Err(anyhow!(
                "memory limit exceeded already before buffering links"
            ));
        }

        let inserter = thread_scope.spawn(move || {
            let mut txn = WriteTransaction::begin(&db)?;

            let mut incoming_count = 0;
            let mut outgoing_count = 0;

            loop {
                // Wait a bit for the buffer to grow or flush and terminate if we get a signal.
                let flush = flush_rx.recv_timeout(Duration::from_secs(1)).is_ok();

                if flush {
                    // Flush the incoming buffer.
                    let incoming_buffer_taken = buffer_clone.lock().unwrap().take_incoming();
                    for (target, sources) in &incoming_buffer_taken {
                        incoming_count += txn.insert_incoming(target, sources)?;
                    }

                    // Flush the outgoing buffer.
                    let outgoing_buffer_taken = buffer_clone.lock().unwrap().take_outgoing();
                    for (source, targets) in &outgoing_buffer_taken {
                        outgoing_count += txn.insert_outgoing(source, targets)?;
                    }

                    break;
                }

                // If we exceed the limit, flush the buffered incoming links first, since the links
                // often seem to be sorted by target title in the dumps and thus we are less likely to
                // incur the cost of updating a value in the database as opposed to just inserting.
                if memory_checker.get() > process_memory_limit {
                    log::info!("flushing buffered incoming links due to reaching memory limit...");
                    let incoming_buffer_taken = buffer_clone.lock().unwrap().take_incoming();
                    for (target, sources) in &incoming_buffer_taken {
                        incoming_count += txn.insert_incoming(target, sources)?;
                    }

                    // If we still exceed the limit, flush the buffered outgoing links second.
                    if memory_checker.get() > process_memory_limit {
                        log::info!(
                            "flushing buffered outgoing links due to reaching memory limit..."
                        );
                        let outgoing_buffer_taken = buffer_clone.lock().unwrap().take_outgoing();
                        for (source, targets) in &outgoing_buffer_taken {
                            outgoing_count += txn.insert_outgoing(source, targets)?;
                        }
                    }
                }

                if flush {
                    break;
                }
            }

            if incoming_count != outgoing_count {
                return Err(anyhow!(
                    "unexpected discrepancy between incoming and outgoing links"
                ));
            }

            txn.commit()?;
            Ok(incoming_count)
        });

        Ok(Self {
            buffer,
            inserter,
            flush_tx,
        })
    }

    /// Insert a link into the buffer.
    pub fn insert_link(&self, source: PageId, target: PageId) {
        self.buffer.lock().unwrap().insert(source, target);
    }

    /// Flush the entire buffer to disk and return the total number of unique inserted links.
    pub fn flush_and_commit(self) -> Result<usize> {
        self.flush_tx.send(())?;

        let link_count = self.inserter.join().unwrap()?;
        if self.buffer.lock().unwrap().len() > 0 {
            return Err(anyhow!("link buffer incoming unexpectedly not empty"));
        }

        Ok(link_count)
    }
}
