use kv_store::Database;
use tempfile::tempdir;
use std::sync::Arc;
use std::thread;

fn create_test_db(dir: &std::path::Path) -> Database {
    Database::open_in_dir(dir, 2, 2, 2).expect("Failed to create database")
}

#[test]
fn test_basic_crud() {
    let dir = tempdir().unwrap();
    let db = create_test_db(dir.path());

    // Insert
    db.insert("key1".into(), "value1".into()).unwrap();
    db.insert("key2".into(), "value2".into()).unwrap();

    // Get
    assert_eq!(db.get("key1").unwrap(), Some("value1".to_string()));
    assert_eq!(db.get("key2").unwrap(), Some("value2".to_string()));
    assert_eq!(db.get("nonexistent").unwrap(), None);

    // Update
    db.insert("key1".into(), "new_value1".into()).unwrap();
    assert_eq!(db.get("key1").unwrap(), Some("new_value1".to_string()));

    // Delete
    db.delete("key2").unwrap();
    assert_eq!(db.get("key2").unwrap(), None);
}

#[test]
fn test_flush_and_compaction() {
    let dir = tempdir().unwrap();
    // Threshold is 2 for data and 2 for sstables.
    let db = create_test_db(dir.path());

    // Trigger flush 1
    db.insert("a".into(), "1".into()).unwrap();
    db.insert("b".into(), "2".into()).unwrap();

    // Trigger flush 2, this will also trigger compaction since sstable_threshold is 2
    db.insert("c".into(), "3".into()).unwrap();
    db.insert("d".into(), "4".into()).unwrap();

    assert_eq!(db.get("a").unwrap(), Some("1".to_string()));
    assert_eq!(db.get("b").unwrap(), Some("2".to_string()));
    assert_eq!(db.get("c").unwrap(), Some("3".to_string()));
    assert_eq!(db.get("d").unwrap(), Some("4".to_string()));
    
    // Update and flush again
    db.insert("a".into(), "11".into()).unwrap();
    db.insert("b".into(), "22".into()).unwrap();

    assert_eq!(db.get("a").unwrap(), Some("11".to_string()));
    assert_eq!(db.get("b").unwrap(), Some("22".to_string()));
}

#[test]
fn test_wal_recovery() {
    let dir = tempdir().unwrap();
    
    {
        let db = create_test_db(dir.path());
        db.insert("key1".into(), "val1".into()).unwrap();
        // Database is dropped here, and data wasn't flushed (threshold is 2).
    }

    {
        let db = create_test_db(dir.path());
        // Verify recovered from WAL
        assert_eq!(db.get("key1").unwrap(), Some("val1".to_string()));
        
        // Let's add more to trigger flush
        db.insert("key2".into(), "val2".into()).unwrap();
    }
    
    {
        let db = create_test_db(dir.path());
        assert_eq!(db.get("key1").unwrap(), Some("val1".to_string()));
        assert_eq!(db.get("key2").unwrap(), Some("val2".to_string()));
    }
}

#[test]
fn test_concurrent_access() {
    let dir = tempdir().unwrap();
    let db = Arc::new(create_test_db(dir.path()));
    
    let mut handles = vec![];
    for i in 0..10 {
        let db_clone = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            let key = format!("key_{}", i);
            let val = format!("val_{}", i);
            db_clone.insert(key.clone(), val.clone()).unwrap();
            assert_eq!(db_clone.get(&key).unwrap(), Some(val));
        }));
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Check all are present
    for i in 0..10 {
        let key = format!("key_{}", i);
        let val = format!("val_{}", i);
        assert_eq!(db.get(&key).unwrap(), Some(val));
    }
}
