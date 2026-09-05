use std::io;
use std::sync::Arc;

use super::Database;

impl Database {
    /// Async wrapper around [`Database::insert`] that offloads the
    /// blocking disk I/O (WAL write, potential flush/compaction) to
    /// Tokio's blocking thread pool via [`tokio::task::spawn_blocking`].
    pub async fn insert_async(self: &Arc<Self>, key: String, value: String) -> io::Result<()> {
        let db = Arc::clone(self);
        tokio::task::spawn_blocking(move || db.insert(key, value))
            .await
            .expect("spawn_blocking panicked")
    }

    /// Async wrapper around [`Database::get`] that offloads the
    /// potentially blocking SSTable reads to Tokio's blocking thread pool.
    pub async fn get_async(self: &Arc<Self>, key: &str) -> io::Result<Option<String>> {
        let db = Arc::clone(self);
        let key = key.to_owned();
        tokio::task::spawn_blocking(move || db.get(&key))
            .await
            .expect("spawn_blocking panicked")
    }

    /// Async wrapper around [`Database::delete`] that offloads the
    /// blocking disk I/O to Tokio's blocking thread pool.
    pub async fn delete_async(self: &Arc<Self>, key: &str) -> io::Result<()> {
        let db = Arc::clone(self);
        let key = key.to_owned();
        tokio::task::spawn_blocking(move || db.delete(&key))
            .await
            .expect("spawn_blocking panicked")
    }
}
