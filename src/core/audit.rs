use std::collections::HashMap;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::Serialize;
use twox_hash::XxHash64;
use std::hash::Hasher;

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
}

pub struct AuditTrail {
    entries: Mutex<Vec<AuditEntry>>,
}

impl AuditTrail {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
        }
    }

    pub fn add_file(&self, path: PathBuf, size: u64, hash: Option<u64>) {
        let mut entries = self.entries.lock().unwrap();
        entries.push(AuditEntry {
            path,
            entry_type: EntryType::File,
            size,
            hash: hash.map(|h| format!("{:016x}", h)),
            permissions: None,
            owner: None,
        });
    }

    pub fn add_directory(&self, path: PathBuf) {
        let mut entries = self.entries.lock().unwrap();
        entries.push(AuditEntry {
            path,
            entry_type: EntryType::Directory,
            size: 0, // Will be calculated later
            hash: None,
            permissions: None,
            owner: None,
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
            if entry.entry_type == EntryType::Directory {
                if let Some(&size) = dir_sizes.get(&entry.path) {
                    entry.size = size;
                }
            }
        }
    }

    pub fn write_csv(&self, path: &Path) -> io::Result<()> {
        let entries = self.entries.lock().unwrap();
        let mut wtr = csv::Writer::from_path(path)?;
        
        // Write header
        wtr.write_record(&["path", "type", "size", "hash", "permissions", "owner"])?;
        
        for entry in entries.iter() {
            wtr.write_record(&[
                entry.path.to_string_lossy().as_ref(),
                match entry.entry_type {
                    EntryType::File => "file",
                    EntryType::Directory => "directory",
                },
                &entry.size.to_string(),
                entry.hash.as_deref().unwrap_or(""),
                &entry.permissions.map(|p| format!("{:o}", p)).unwrap_or_default(),
                entry.owner.as_deref().unwrap_or(""),
            ])?;
        }
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
                permissions INTEGER,
                owner TEXT
            )",
            [],
        )?;

        let tx = conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO audit_entries (path, type, size, hash, permissions, owner)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
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
                    entry.permissions,
                    entry.owner,
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
