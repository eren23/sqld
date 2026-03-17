use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;

use crate::storage::page::{Page, PageId, INVALID_PAGE_ID, PAGE_SIZE};
use crate::utils::error::{Error, StorageError};

const DB_FILE_NAME: &str = "sqld.db";
const LOCK_FILE_NAME: &str = "sqld.lock";

/// Manages page-level I/O against a single database file.
///
/// Pages are identified by [`PageId`] (u32). Page 0 is reserved and never
/// allocated. Each page occupies `PAGE_SIZE` (8 KiB) bytes at file offset
/// `page_id * PAGE_SIZE`.
///
/// Thread safety is provided by an internal [`Mutex`] around the file handle.
/// A lock file (`sqld.lock`) is created in the data directory on open to
/// signal to other processes that the database is in use.
pub struct DiskManager {
    data_dir: PathBuf,
    file: Mutex<File>,
    next_page_id: AtomicU32,
    free_list: Mutex<Vec<PageId>>,
}

impl DiskManager {
    /// Open (or create) a database in `data_dir`.
    ///
    /// Creates the directory tree and database file if they do not exist.
    pub fn new(data_dir: impl AsRef<Path>) -> Result<Self, Error> {
        let data_dir = data_dir.as_ref().to_path_buf();

        // Ensure the data directory exists.
        fs::create_dir_all(&data_dir)?;

        // Create / touch the advisory lock file.
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(data_dir.join(LOCK_FILE_NAME))?;

        // Open or create the database file.
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(data_dir.join(DB_FILE_NAME))?;

        // Derive the next page id from the current file size.
        let file_size = file.metadata()?.len();
        let next_page_id = if file_size == 0 {
            1 // page 0 is reserved
        } else {
            (file_size / PAGE_SIZE as u64) as u32
        };

        Ok(DiskManager {
            data_dir,
            file: Mutex::new(file),
            next_page_id: AtomicU32::new(next_page_id),
            free_list: Mutex::new(Vec::new()),
        })
    }

    /// Allocate a new page id. Reuses deallocated ids when available.
    pub fn allocate_page(&self) -> Result<PageId, Error> {
        // Try the free list first.
        if let Some(id) = self.free_list.lock().unwrap().pop() {
            return Ok(id);
        }

        let page_id = self.next_page_id.fetch_add(1, Ordering::SeqCst);

        // Extend the file to cover the new page.
        let file = self.file.lock().unwrap();
        let needed = (page_id as u64 + 1) * PAGE_SIZE as u64;
        if file.metadata()?.len() < needed {
            file.set_len(needed)?;
        }

        Ok(page_id)
    }

    /// Read a page from disk.
    pub fn read_page(&self, page_id: PageId) -> Result<Page, Error> {
        if page_id == INVALID_PAGE_ID {
            return Err(StorageError::InvalidPageId(page_id as u64).into());
        }

        let mut file = self.file.lock().unwrap();
        let offset = page_id as u64 * PAGE_SIZE as u64;

        if offset + PAGE_SIZE as u64 > file.metadata()?.len() {
            return Err(StorageError::InvalidPageId(page_id as u64).into());
        }

        file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; PAGE_SIZE];
        file.read_exact(&mut buf)?;

        Page::from_bytes(&buf)
    }

    /// Write a page to disk.
    pub fn write_page(&self, page_id: PageId, page: &Page) -> Result<(), Error> {
        if page_id == INVALID_PAGE_ID {
            return Err(StorageError::InvalidPageId(page_id as u64).into());
        }

        let mut file = self.file.lock().unwrap();
        let offset = page_id as u64 * PAGE_SIZE as u64;

        // Ensure the file is large enough.
        let needed = offset + PAGE_SIZE as u64;
        if file.metadata()?.len() < needed {
            file.set_len(needed)?;
        }

        file.seek(SeekFrom::Start(offset))?;
        file.write_all(page.as_bytes())?;
        file.flush()?;

        Ok(())
    }

    /// Deallocate a page, zeroing its on-disk contents and returning the id to
    /// the internal free list for reuse.
    pub fn deallocate_page(&self, page_id: PageId) -> Result<(), Error> {
        if page_id == INVALID_PAGE_ID {
            return Err(StorageError::InvalidPageId(page_id as u64).into());
        }

        {
            let mut file = self.file.lock().unwrap();
            let offset = page_id as u64 * PAGE_SIZE as u64;

            if offset + PAGE_SIZE as u64 > file.metadata()?.len() {
                return Err(StorageError::InvalidPageId(page_id as u64).into());
            }

            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&[0u8; PAGE_SIZE])?;
            file.flush()?;
        }

        self.free_list.lock().unwrap().push(page_id);
        Ok(())
    }

    /// Path to the data directory.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Number of pages currently on the free list.
    pub fn free_list_len(&self) -> usize {
        self.free_list.lock().unwrap().len()
    }
}
