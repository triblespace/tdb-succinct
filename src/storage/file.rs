use std::io::SeekFrom;
use std::path::PathBuf;

use async_trait::async_trait;
use minibytes::Bytes;
use memmap2::Mmap;
use tokio::fs::File;
use tokio::io::{self, AsyncSeekExt, BufWriter};

use super::{FileLoad, FileStore, SyncableFile};

#[derive(Clone, Debug)]
pub struct FileBackedStore {
    path: PathBuf,
}

#[async_trait]
impl SyncableFile for File {
    async fn sync_all(self) -> io::Result<()> {
        File::sync_all(&self).await
    }
}

#[async_trait]
impl SyncableFile for BufWriter<File> {
    async fn sync_all(self) -> io::Result<()> {
        let inner = self.into_inner();

        File::sync_all(&inner).await
    }
}

impl FileBackedStore {
    pub fn new<P: Into<PathBuf>>(path: P) -> FileBackedStore {
        FileBackedStore { path: path.into() }
    }
}

#[async_trait]
impl FileLoad for FileBackedStore {
    type Read = File;

    async fn exists(&self) -> io::Result<bool> {
        let metadata = tokio::fs::metadata(&self.path).await;
        Ok(!(metadata.is_err() && metadata.err().unwrap().kind() == io::ErrorKind::NotFound))
    }

    async fn size(&self) -> io::Result<usize> {
        let m = tokio::fs::metadata(&self.path).await?;
        Ok(m.len() as usize)
    }

    async fn open_read_from(&self, offset: usize) -> io::Result<File> {
        let mut options = tokio::fs::OpenOptions::new();
        options.read(true);
        let mut file = options.open(&self.path).await?;

        file.seek(SeekFrom::Start(offset as u64)).await?;

        Ok(file)
    }

    async fn map(&self) -> io::Result<Bytes> {
        let size = self.size().await?;
        if size == 0 {
            Ok(Bytes::new())
        } else {
            let f = self.open_read().await?;
            let mmap = unsafe { Mmap::map(&f)?  };
            Ok(mmap.into())
        }
    }
}

#[async_trait]
impl FileStore for FileBackedStore {
    type Write = BufWriter<File>;

    async fn open_write(&self) -> io::Result<BufWriter<File>> {
        let mut options = tokio::fs::OpenOptions::new();
        options.read(true).write(true).create(true);
        let file = options.open(&self.path).await?;

        Ok(BufWriter::new(file))
    }
}
