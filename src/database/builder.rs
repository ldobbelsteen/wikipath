use crate::{
    dump,
    progress::{multi_progress, spinner, subspinner, unit_progress},
};
use bincode::serialize_into;
use error_chain::error_chain;
use hashbrown::{HashMap, HashSet};
use indicatif::MultiProgress;
use std::{
    fs::{self, File},
    io::{BufWriter, Write},
    path::PathBuf,
};

error_chain! {
    foreign_links {
        Io(std::io::Error);
        Bincode(bincode::Error);
        Parse(dump::ParseError);
    }

    errors {
        DatabaseAlreadyExists(path: PathBuf) {
            display("database already exists at '{}'", path.display())
        }
    }
}

pub static INCOMING_FILENAME: &str = "incoming";
pub static INCOMING_INDEX_FILENAME: &str = "incoming_index";
pub static OUTGOING_FILENAME: &str = "outgoing";
pub static OUTGOING_INDEX_FILENAME: &str = "outgoing_index";
pub static REDIRECTS_FILENAME: &str = "redirects";
pub static METADATA_FILENAME: &str = "metadata";

pub type PageId = u32;

#[derive(Debug)]
pub struct Builder {
    incoming: File,
    incoming_index: File,
    outgoing: File,
    outgoing_index: File,
    redirects: File,
    metadata: File,
}

impl Builder {
    pub fn new(path: &PathBuf) -> Result<Self> {
        if path.exists() {
            return Err(ErrorKind::DatabaseAlreadyExists(path.clone()).into());
        } else {
            fs::create_dir_all(&path)?;
        }

        Ok(Self {
            incoming: File::create(path.join(INCOMING_FILENAME))?,
            incoming_index: File::create(path.join(INCOMING_INDEX_FILENAME))?,
            outgoing: File::create(path.join(OUTGOING_FILENAME))?,
            outgoing_index: File::create(path.join(OUTGOING_INDEX_FILENAME))?,
            redirects: File::create(path.join(REDIRECTS_FILENAME))?,
            metadata: File::create(path.join(METADATA_FILENAME))?,
        })
    }

    pub fn build(&mut self, dump: &dump::Dump) -> Result<()> {
        let progress = multi_progress();

        let step = progress.add(spinner("Parsing page dump".into()));
        let titles = dump.parse_page_dump_file(progress.clone())?;
        step.finish();

        let step = progress.add(spinner("Parsing redirects dump".into()));
        let redirects = dump.parse_redir_dump_file(&titles, progress.clone())?;
        step.finish();

        let step = progress.add(spinner("Parsing links dump".into()));
        let links = dump.parse_link_dump_file(&titles, &redirects, progress.clone())?;
        step.finish();

        let step = progress.add(spinner("Ingesting redirects into database".into()));
        self.store_redirects(redirects)?;
        step.finish();

        let step = progress.add(spinner("Ingesting incoming links into database".into()));
        self.store_links(true, links.incoming, progress.clone())?;
        step.finish();

        let step = progress.add(spinner("Ingesting outgoing links into database".into()));
        self.store_links(false, links.outgoing, progress.clone())?;
        step.finish();

        serialize_into(&mut self.metadata, &dump.metadata)?;
        Ok(())
    }

    fn store_links(
        &mut self,
        incoming: bool,
        links: HashMap<PageId, HashSet<PageId>>,
        progress: MultiProgress,
    ) -> Result<()> {
        let bar = progress.add(unit_progress(links.len() as u64));
        let (file, index) = if incoming {
            (&mut self.incoming, &mut self.incoming_index)
        } else {
            (&mut self.outgoing, &mut self.outgoing_index)
        };

        let mut file_index = 0;
        let mut writer = BufWriter::new(file);
        let mut indices: HashMap<PageId, (u64, u64)> = HashMap::new();
        for (key, set) in links {
            indices.insert(key, (file_index, set.len() as u64));
            for value in set {
                file_index += writer.write(&value.to_le_bytes())? as u64;
            }
            bar.inc(1);
        }
        writer.flush()?;
        bar.finish();

        let spinner = progress.add(subspinner("Storing index, this may take a while".into()));
        serialize_into(index, &indices)?;
        spinner.finish();

        Ok(())
    }

    fn store_redirects(&mut self, redirects: HashMap<PageId, PageId>) -> Result<()> {
        serialize_into(&mut self.redirects, &redirects)?;
        Ok(())
    }
}
