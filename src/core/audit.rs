use std::{
    collections::HashMap,
    hash::Hasher,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};

use serde::Serialize;
use twox_hash::XxHash64;

#[derive(Debug, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EntryType {
    File,
    Directory,
}

#[derive(Debug, Serialize, Clone)]
pub struct AuditEntry {
    pub path: PathBuf,
    pub entry_type: EntryType,
    pub size: u64,
    pub hash: Option<String>,
    pub permissions: Option<u32>,
    pub owner: Option<String>,
    pub is_duplicate: bool,
}

#[derive(Debug)]
pub struct AuditTrail {
    entries: Mutex<Vec<AuditEntry>>,
}

impl AuditTrail {
    #[allow(clippy::missing_const_for_fn)]
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
        }
    }

    pub fn add_file(
        &self,
        path: PathBuf,
        size: u64,
        hash: Option<u64>,
        is_duplicate: bool,
        permission: Option<u32>,
    ) {
        let mut entries = self.entries.lock().unwrap();
        entries.push(AuditEntry {
            path,
            entry_type: EntryType::File,
            size,
            hash: hash.map(|h| format!("{h:016x}")),
            permissions: Some(permission.unwrap_or(0o644)),
            owner: None,
            is_duplicate,
        });
    }

    pub fn add_directory(&self, path: PathBuf, permission: Option<u32>) {
        let mut entries = self.entries.lock().unwrap();
        entries.push(AuditEntry {
            path,
            entry_type: EntryType::Directory,
            size: 0, // Will be calculated later
            hash: None,
            permissions: Some(permission.unwrap_or(0o755)),
            owner: None,
            is_duplicate: false,
        });
    }

    pub fn calculate_directory_sizes(&self) {
        let mut entries = self.entries.lock().unwrap();

        // Map to store directory sizes
        let mut dir_sizes: HashMap<PathBuf, u64> = HashMap::new();

        // First, collect all file sizes and add them to their parent directories
        for entry in entries.iter() {
            if entry.entry_type == EntryType::File {
                let mut current = entry.path.parent();
                while let Some(parent) = current {
                    *dir_sizes.entry(parent.to_path_buf()).or_insert(0) += entry.size;
                    current = parent.parent();
                }
            }
        }

        // Update directory entries with calculated sizes
        for entry in entries.iter_mut() {
            if let (EntryType::Directory, Some(&size)) =
                (entry.entry_type, dir_sizes.get(&entry.path))
            {
                entry.size = size;
            }
        }
    }

    pub fn write_csv(&self, path: &Path) -> io::Result<()> {
        let entries = self.entries.lock().unwrap();
        let mut wtr = csv::Writer::from_path(path)?;

        // Write header
        wtr.write_record([
            "path",
            "type",
            "size",
            "hash",
            "permissions",
            "owner",
            "is_duplicate",
        ])?;

        for entry in entries.iter() {
            wtr.write_record([
                entry.path.to_string_lossy().as_ref(),
                match entry.entry_type {
                    EntryType::File => "file",
                    EntryType::Directory => "directory",
                },
                entry.size.to_string().as_str(),
                entry.hash.as_deref().unwrap_or(""),
                entry
                    .permissions
                    .map(|p| format!("{p:o}"))
                    .unwrap_or_default()
                    .as_str(),
                entry.owner.as_deref().unwrap_or(""),
                if entry.is_duplicate { "true" } else { "false" },
            ])?;
        }
        drop(entries);
        wtr.flush()?;
        Ok(())
    }

    pub fn write_sqlite(&self, path: &Path) -> rusqlite::Result<()> {
        let entries = self.entries.lock().unwrap();
        let mut conn = rusqlite::Connection::open(path)?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS audit_entries (
                path TEXT NOT NULL,
                type TEXT NOT NULL,
                size INTEGER NOT NULL,
                hash TEXT,
                permissions TEXT,
                owner TEXT,
                is_duplicate BOOLEAN NOT NULL DEFAULT 0
            )",
            [],
        )?;

        let tx = conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO audit_entries (path, type, size, hash, permissions, owner, \
                 is_duplicate)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )?;

            for entry in entries.iter() {
                stmt.execute(rusqlite::params![
                    entry.path.to_string_lossy(),
                    match entry.entry_type {
                        EntryType::File => "file",
                        EntryType::Directory => "directory",
                    },
                    entry.size,
                    entry.hash,
                    entry.permissions.map(|p| format!("{p:o}")),
                    entry.owner,
                    entry.is_duplicate,
                ])?;
            }
        }
        tx.commit()?;

        Ok(())
    }
}

pub struct HashingWriter<W: Write> {
    inner: W,
    hasher: XxHash64,
}

impl<W: Write> HashingWriter<W> {
    #[allow(clippy::missing_const_for_fn)]
    pub fn new(inner: W, seed: u64) -> Self {
        Self {
            inner,
            hasher: XxHash64::with_seed(seed),
        }
    }

    pub fn finalize(self) -> u64 {
        self.hasher.finish()
    }
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.write(&buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
