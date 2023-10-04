use anyhow::{anyhow, Result};
use hashbrown::{HashMap, HashSet};
use itertools::Itertools;
use redb::{ReadOnlyTable, ReadableTable, RedbValue, Table, TableDefinition, TypeName};
use regex::Regex;
use serde::Serialize;
use std::path::Path;
use std::sync::{Mutex, RwLock};
use std::{mem, vec};

use crate::memory::MemoryUsage;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    pub language_code: String,
    pub dump_date: String,
}

impl Metadata {
    /// Extract metadata from the name of a database.
    pub fn from_name(s: &str) -> Result<Self> {
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

pub type PageId = u32;

#[derive(Debug, Default)]
pub struct PageIds(pub Vec<PageId>);

const INCOMING: TableDefinition<PageId, PageIds> = TableDefinition::new("incoming");
const OUTGOING: TableDefinition<PageId, PageIds> = TableDefinition::new("outgoing");
const REDIRECTS: TableDefinition<PageId, PageId> = TableDefinition::new("redirects");

#[derive(Debug)]
pub struct Database {
    inner: redb::Database,
    pub metadata: Metadata,
}

pub struct ReadTransaction<'db> {
    inner: redb::ReadTransaction<'db>,
}

impl<'db> ReadTransaction<'db> {
    pub fn open_serve(&'db self) -> Result<ServeTransaction<'db>> {
        Ok(ServeTransaction {
            incoming: self.inner.open_table(INCOMING)?,
            outgoing: self.inner.open_table(OUTGOING)?,
            redirects: self.inner.open_table(REDIRECTS)?,
        })
    }
}

pub struct WriteTransaction<'db> {
    inner: redb::WriteTransaction<'db>,
}

impl<'db> WriteTransaction<'db> {
    pub fn open_build<'txn>(
        &'txn self,
        max_memory_usage: u64,
    ) -> Result<BuildTransaction<'db, 'txn>> {
        Ok(BuildTransaction {
            incoming_table: Mutex::new(self.inner.open_table(INCOMING)?),
            outgoing_table: Mutex::new(self.inner.open_table(OUTGOING)?),
            redirects_table: Mutex::new(self.inner.open_table(REDIRECTS)?),
            incoming_cache: Default::default(),
            outgoing_cache: Default::default(),
            redirects_cache: Default::default(),
            ids: Default::default(),
            memory_usage: MemoryUsage::new(5)?,
            max_memory_usage,
        })
    }

    pub fn commit(self) -> Result<()> {
        self.inner.commit()?;
        Ok(())
    }
}

pub struct ServeTransaction<'txn> {
    incoming: ReadOnlyTable<'txn, PageId, PageIds>,
    outgoing: ReadOnlyTable<'txn, PageId, PageIds>,
    redirects: ReadOnlyTable<'txn, PageId, PageId>,
}

impl<'txn> ServeTransaction<'txn> {
    pub fn get_incoming_links(&self, target: PageId) -> Result<PageIds> {
        match self.incoming.get(target)? {
            Some(res) => Ok(res.value()),
            None => Ok(PageIds(vec![])),
        }
    }

    pub fn get_outgoing_links(&self, source: PageId) -> Result<PageIds> {
        match self.outgoing.get(source)? {
            Some(res) => Ok(res.value()),
            None => Ok(PageIds(vec![])),
        }
    }

    pub fn get_redirect(&self, page: PageId) -> Result<Option<PageId>> {
        Ok(self.redirects.get(page)?.map(|r| r.value()))
    }
}

pub struct BuildTransaction<'db, 'txn> {
    incoming_table: Mutex<Table<'db, 'txn, PageId, PageIds>>,
    outgoing_table: Mutex<Table<'db, 'txn, PageId, PageIds>>,
    redirects_table: Mutex<Table<'db, 'txn, PageId, PageId>>,
    incoming_cache: Mutex<HashMap<PageId, PageIds>>,
    outgoing_cache: Mutex<HashMap<PageId, PageIds>>,
    redirects_cache: Mutex<HashMap<PageId, PageId>>,
    ids: RwLock<HashMap<String, PageId>>,
    memory_usage: MemoryUsage,
    max_memory_usage: u64,
}

impl<'db, 'txn> BuildTransaction<'db, 'txn> {
    /// Get the page id associated with a page title.
    pub fn get_id(&self, title: &str) -> Option<PageId> {
        self.ids.read().unwrap().get(title).copied()
    }

    /// Store a page title and the corresponding page id.
    pub fn store_title(&self, title: String, id: PageId) {
        self.ids.write().unwrap().insert(title, id);
    }

    /// Store a link from one page's id to another.
    pub fn insert_link(&self, source: PageId, target: PageId) -> Result<()> {
        self.incoming_cache
            .lock()
            .unwrap()
            .entry(target)
            .or_default()
            .0
            .push(source);
        self.outgoing_cache
            .lock()
            .unwrap()
            .entry(source)
            .or_default()
            .0
            .push(target);
        self.shrink_cache()?;
        Ok(())
    }

    /// Store a redirect from one page's id to another.
    pub fn insert_redirect(&self, source: PageId, target: PageId) -> Result<()> {
        self.redirects_cache.lock().unwrap().insert(source, target);
        self.shrink_cache()?;
        Ok(())
    }

    /// Shrink cache until below maximum memory usage.
    fn shrink_cache(&self) -> Result<()> {
        if self.memory_usage.get() > self.max_memory_usage {
            self.flush_redirects()?;
            if self.memory_usage.get() > self.max_memory_usage {
                self.flush_outgoing()?;
                if self.memory_usage.get() > self.max_memory_usage {
                    self.flush_incoming()?;
                }
            }
        }
        Ok(())
    }

    /// Flush currently cached incoming links to disk.
    fn flush_incoming(&self) -> Result<()> {
        let mut incoming_table = self.incoming_table.lock().unwrap();
        for (target, mut sources) in self.incoming_cache.lock().unwrap().drain() {
            if let Some(mut old_sources) = incoming_table.get(target)?.map(|r| r.value()) {
                sources.0.append(&mut old_sources.0);
            }
            incoming_table.insert(target, sources)?;
        }
        Ok(())
    }

    /// Flush currently cached incoming links to disk.
    fn flush_outgoing(&self) -> Result<()> {
        let mut outgoing_table = self.outgoing_table.lock().unwrap();
        for (source, mut targets) in self.outgoing_cache.lock().unwrap().drain() {
            if let Some(mut old_targets) = outgoing_table.get(source)?.map(|r| r.value()) {
                targets.0.append(&mut old_targets.0);
            }
            outgoing_table.insert(source, targets)?;
        }
        Ok(())
    }

    /// Flush currently cached redirects to disk.
    fn flush_redirects(&self) -> Result<()> {
        let mut redirects_cache = self.redirects_cache.lock().unwrap();
        let mut redirects_table = self.redirects_table.lock().unwrap();
        for (source, target) in redirects_cache.iter() {
            let source = *source;
            let mut target = *target;
            let mut sources: HashSet<PageId> = HashSet::from([source]);
            loop {
                if let Some(new_target) = redirects_cache.get(&target) {
                    if sources.contains(new_target) {
                        break;
                    }
                    sources.insert(target);
                    target = *new_target;
                    continue;
                }
                if let Some(new_target) = redirects_table.get(target)?.map(|r| r.value()) {
                    if sources.contains(&new_target) {
                        break;
                    }
                    sources.insert(target);
                    target = new_target;
                    continue;
                }
                break;
            }
            for source in sources {
                redirects_table.insert(source, target)?;
            }
        }
        redirects_cache.clear();
        Ok(())
    }

    /// Flush all cached items to disk and close.
    pub fn flush(self) -> Result<()> {
        self.flush_redirects()?;
        self.flush_outgoing()?;
        self.flush_incoming()?;
        Ok(())
    }
}

impl Database {
    pub fn open(path: &Path) -> Result<Self> {
        let filename = path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or(anyhow!("database path '{}' is not valid", path.display()))?;
        let inner = redb::Database::create(path)?;
        let metadata = Metadata::from_name(filename)?;
        Ok(Self { inner, metadata })
    }

    pub fn begin_read(&self) -> Result<ReadTransaction<'_>> {
        Ok(ReadTransaction {
            inner: self.inner.begin_read()?,
        })
    }

    pub fn begin_write(&self) -> Result<WriteTransaction<'_>> {
        Ok(WriteTransaction {
            inner: self.inner.begin_write()?,
        })
    }

    pub fn compact(&mut self) -> Result<()> {
        self.inner.compact()?;
        Ok(())
    }
}

impl RedbValue for PageIds {
    type SelfType<'a> = Self;
    type AsBytes<'a> = Vec<u8>;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn type_name() -> TypeName {
        TypeName::new("PageIds")
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        value
            .0
            .iter()
            .dedup()
            .flat_map(|id| id.to_le_bytes())
            .collect()
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        PageIds(
            data.chunks(mem::size_of::<PageId>())
                .map(|bs| PageId::from_le_bytes(bs.try_into().unwrap()))
                .collect(),
        )
    }
}
