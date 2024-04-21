use crate::memory::ProcessMemoryUsageChecker;
use anyhow::{anyhow, Result};
use hashbrown::HashMap;
use log::info;
use redb::{ReadOnlyTable, ReadableTable, Table, TableDefinition};
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
    pub dump_date: String,
}

impl Metadata {
    /// Extract metadata from the name of a database.
    fn from_name(s: &str) -> Result<Self> {
        let re = Regex::new(r"(.+)-(.+).redb(?:\.tmp)?")?;
        if let Some(caps) = re.captures(s) {
            if let Some(language_code) = caps.get(1) {
                if let Some(dump_date) = caps.get(2) {
                    return Ok(Metadata {
                        language_code: language_code.as_str().into(),
                        dump_date: dump_date.as_str().into(),
                    });
                }
            }
        }
        Err(anyhow!("database path '{}' is not valid", s))
    }

    /// Create name containing all database metadata.
    pub fn to_name(&self) -> String {
        format!("{}-{}.redb", self.language_code, self.dump_date)
    }

    /// Create temp name containing all database metadata.
    pub fn to_tmp_name(&self) -> String {
        format!("{}-{}.redb.tmp", self.language_code, self.dump_date)
    }
}

/// Representation of a Wikimedia page id.
pub type PageId = u32;

const INCOMING: TableDefinition<PageId, Vec<PageId>> = TableDefinition::new("incoming");
const OUTGOING: TableDefinition<PageId, Vec<PageId>> = TableDefinition::new("outgoing");
const REDIRECTS: TableDefinition<PageId, PageId> = TableDefinition::new("redirects");

#[derive(Debug)]
pub struct Database {
    inner: redb::Database,
    pub metadata: Metadata,
}

impl Database {
    /// Open a database file. If the file does not exist yet, a new one is created. Will return an
    /// error if the file name in the path is not in the correct format.
    pub fn open(path: &Path) -> Result<Self> {
        let filename = path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or(anyhow!("database path '{}' is not valid", path.display()))?;
        let metadata = Metadata::from_name(filename)?;
        let inner = redb::Database::create(path)?;
        Ok(Self { inner, metadata })
    }

    /// Begin a read transaction on the database.
    pub fn begin_read(&self) -> Result<ReadTransaction> {
        Ok(ReadTransaction {
            inner: self.inner.begin_read()?,
        })
    }

    /// Begin a write transaction on the database.
    pub fn begin_write(&self) -> Result<WriteTransaction> {
        Ok(WriteTransaction {
            inner: self.inner.begin_write()?,
        })
    }

    /// Compact the database file.
    pub fn compact(&mut self) -> Result<()> {
        self.inner.compact()?;
        Ok(())
    }
}

pub struct ReadTransaction {
    inner: redb::ReadTransaction,
}

impl ReadTransaction {
    pub fn begin_serve(&self) -> Result<ServeTransaction> {
        Ok(ServeTransaction {
            incoming: self.inner.open_table(INCOMING)?,
            outgoing: self.inner.open_table(OUTGOING)?,
            redirects: self.inner.open_table(REDIRECTS)?,
        })
    }
}

#[derive(Debug)]
pub struct ServeTransaction {
    incoming: ReadOnlyTable<PageId, Vec<PageId>>,
    outgoing: ReadOnlyTable<PageId, Vec<PageId>>,
    redirects: ReadOnlyTable<PageId, PageId>,
}

impl ServeTransaction {
    pub fn incoming_links(&self, target: PageId) -> Result<Vec<PageId>> {
        match self.incoming.get(target)? {
            Some(res) => Ok(res.value()),
            None => Ok(vec![]),
        }
    }

    pub fn outgoing_links(&self, source: PageId) -> Result<Vec<PageId>> {
        match self.outgoing.get(source)? {
            Some(res) => Ok(res.value()),
            None => Ok(vec![]),
        }
    }

    pub fn redirect(&self, page: PageId) -> Result<Option<PageId>> {
        Ok(self.redirects.get(page)?.map(|r| r.value()))
    }
}

pub struct WriteTransaction {
    inner: redb::WriteTransaction,
}

impl WriteTransaction {
    pub fn begin_build(&self) -> Result<BuildTransaction> {
        Ok(BuildTransaction {
            redirects: self.inner.open_table(REDIRECTS)?,
            incoming: self.inner.open_table(INCOMING)?,
            outgoing: self.inner.open_table(OUTGOING)?,
        })
    }

    pub fn commit(self) -> Result<()> {
        self.inner.commit()?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct BuildTransaction<'txn> {
    redirects: Table<'txn, PageId, PageId>,
    incoming: Table<'txn, PageId, Vec<PageId>>,
    outgoing: Table<'txn, PageId, Vec<PageId>>,
}

impl<'txn> BuildTransaction<'txn> {
    pub fn insert_redirects(&mut self, redirs: &HashMap<PageId, PageId>) -> Result<()> {
        for (source, target) in redirs {
            if self.redirects.insert(source, target)?.is_some() {
                return Err(anyhow!("redirect source already in the database"));
            }
        }
        Ok(())
    }

    pub fn insert_incoming(&mut self, incoming: HashMap<PageId, Vec<PageId>>) -> Result<usize> {
        let mut removed = 0;
        let mut added = 0;

        for (target, mut sources) in incoming {
            if let Some(existing) = self.incoming.get(target)? {
                let mut existing = existing.value();
                removed += existing.len();
                if existing.len() > sources.len() {
                    existing.extend(sources);
                    sources = existing;
                } else {
                    sources.extend(existing);
                }
            }
            added += sources.len();
            self.incoming.insert(target, sources)?;
        }

        Ok(added - removed)
    }

    pub fn insert_outgoing(&mut self, outgoing: HashMap<PageId, Vec<PageId>>) -> Result<usize> {
        let mut removed = 0;
        let mut added = 0;

        for (source, mut targets) in outgoing {
            if let Some(existing) = self.outgoing.get(source)? {
                let mut existing = existing.value();
                removed += existing.len();
                if existing.len() > targets.len() {
                    existing.extend(targets);
                    targets = existing;
                } else {
                    targets.extend(existing);
                }
            }
            added += targets.len();
            self.outgoing.insert(source, targets)?;
        }

        Ok(added - removed)
    }
}

#[derive(Debug)]
pub struct BufferedLinkInserter<'scope> {
    incoming_buffer: Arc<Mutex<HashMap<PageId, Vec<PageId>>>>,
    outgoing_buffer: Arc<Mutex<HashMap<PageId, Vec<PageId>>>>,
    inserter: ScopedJoinHandle<'scope, Result<usize>>,
    flush_tx: Sender<()>,
}

impl<'scope> BufferedLinkInserter<'scope> {
    /// Create a buffered link inserter from a build transaction. This caches link inserts in a
    /// buffer and periodically flushes the buffer to disk if the specified number of bytes of
    /// memory is exceeded for the entire process.
    pub fn for_txn<'env, 'txn>(
        txn: &'env mut BuildTransaction<'txn>,
        memory_limit: u64,
        scope: &'scope Scope<'scope, 'env>,
    ) -> Result<Self> {
        let (flush_tx, flush_rx) = mpsc::channel::<()>();

        let incoming_buffer = Arc::new(Mutex::default());
        let incoming_buffer_c = incoming_buffer.clone();

        let outgoing_buffer = Arc::new(Mutex::default());
        let outgoing_buffer_c = outgoing_buffer.clone();

        let mut memory_checker = ProcessMemoryUsageChecker::new()?;
        if memory_checker.get() > memory_limit {
            return Err(anyhow!(
                "memory limit exceeded already before buffering links"
            ));
        }

        let inserter = scope.spawn(move || {
            let mut incoming_count = 0;
            let mut outgoing_count = 0;

            loop {
                // Wait a bit for the buffer to grow or flush and terminate if we get a signal.
                let flush = flush_rx.recv_timeout(Duration::from_secs(1)).is_ok();

                if flush {
                    // Flush the incoming buffer.
                    let incoming_buffer_taken =
                        mem::replace(&mut *incoming_buffer_c.lock().unwrap(), HashMap::new());
                    incoming_count += txn.insert_incoming(incoming_buffer_taken)?;

                    // Flush the outgoing buffer.
                    let outgoing_buffer_taken =
                        mem::replace(&mut *outgoing_buffer_c.lock().unwrap(), HashMap::new());
                    outgoing_count += txn.insert_outgoing(outgoing_buffer_taken)?;

                    break;
                }

                // If we exceed the limit, flush the buffered incoming links first, since the links
                // often seem to be sorted by target title in the dumps and thus we are less likely to
                // incur the cost of updating a value in the database as opposed to just inserting.
                if memory_checker.get() > memory_limit {
                    info!("flushing buffered incoming links due to reaching memory limit...");
                    let incoming_buffer_taken =
                        mem::replace(&mut *incoming_buffer_c.lock().unwrap(), HashMap::new());
                    incoming_count += txn.insert_incoming(incoming_buffer_taken)?;

                    // If we still exceed the limit, flush the buffered outgoing links second.
                    if memory_checker.get() > memory_limit {
                        info!("flushing buffered outgoing links due to reaching memory limit...");
                        let outgoing_buffer_taken =
                            mem::replace(&mut *outgoing_buffer_c.lock().unwrap(), HashMap::new());
                        outgoing_count += txn.insert_outgoing(outgoing_buffer_taken)?;
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

            Ok(incoming_count)
        });

        Ok(Self {
            incoming_buffer,
            outgoing_buffer,
            inserter,
            flush_tx,
        })
    }

    /// Insert a link into the buffer.
    pub fn insert(&self, source: PageId, target: PageId) {
        self.incoming_buffer
            .lock()
            .unwrap()
            .entry(target)
            .or_default()
            .push(source);
        self.outgoing_buffer
            .lock()
            .unwrap()
            .entry(source)
            .or_default()
            .push(target);
    }

    /// Flush the entire buffer to disk and return the total number of unique inserted links.
    pub fn flush(self) -> Result<usize> {
        self.flush_tx.send(())?;
        let link_count = self.inserter.join().unwrap()?;
        if self.incoming_buffer.lock().unwrap().len() > 0 {
            return Err(anyhow!("incoming buffer unexpectedly not empty"));
        }
        if self.outgoing_buffer.lock().unwrap().len() > 0 {
            return Err(anyhow!("outgoing buffer unexpectedly not empty"));
        }
        Ok(link_count)
    }
}
