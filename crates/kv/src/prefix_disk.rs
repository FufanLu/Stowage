//! A [`Disk`] view that namespaces file names with a prefix, so one node's data
//! directory can hold the storage engine (`e.*`) and one Raft log per range
//! (`r<id>.*`) side by side.

use std::io;
use std::sync::Arc;

use stowage_common::{Disk, DiskFile};

#[derive(Debug, Clone)]
pub struct PrefixDisk {
    inner: Arc<dyn Disk>,
    prefix: String,
}

impl PrefixDisk {
    pub fn new(inner: Arc<dyn Disk>, prefix: impl Into<String>) -> Self {
        Self {
            inner,
            prefix: prefix.into(),
        }
    }

    fn name(&self, n: &str) -> String {
        format!("{}{n}", self.prefix)
    }

    /// Delete every file in this namespace.
    pub fn destroy(&self) -> io::Result<()> {
        for n in self.list()? {
            self.inner.remove(&self.name(&n))?;
        }
        self.inner.sync_dir()
    }
}

impl Disk for PrefixDisk {
    fn open(&self, name: &str) -> io::Result<Arc<dyn DiskFile>> {
        self.inner.open(&self.name(name))
    }
    fn exists(&self, name: &str) -> io::Result<bool> {
        self.inner.exists(&self.name(name))
    }
    fn remove(&self, name: &str) -> io::Result<()> {
        self.inner.remove(&self.name(name))
    }
    fn rename(&self, from: &str, to: &str) -> io::Result<()> {
        self.inner.rename(&self.name(from), &self.name(to))
    }
    fn list(&self) -> io::Result<Vec<String>> {
        Ok(self
            .inner
            .list()?
            .into_iter()
            .filter_map(|n| n.strip_prefix(&self.prefix).map(str::to_owned))
            .collect())
    }
    fn sync_dir(&self) -> io::Result<()> {
        self.inner.sync_dir()
    }
}
