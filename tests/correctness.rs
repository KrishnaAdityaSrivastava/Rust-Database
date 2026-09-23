use std::sync::Arc;
use std::thread;

use kv_store::wal::log_record::Value;
use kv_store::Database;
use tempfile::tempdir;

fn string(value: &str) -> Value {
    Value::String(value.to_string())
}

fn create_test_db(dir: &std::path::Path) -> Database {
    Database::open_in_dir(dir, 2, 2, 2)
        .expect("Failed to create database")
}

#[test]
fn test_basic_crud() {
    let dir = tempdir().unwrap();
    let db = create_test_db(dir.path());

    // Insert
    db.insert("key1".into(), string("value1")).unwrap();
    db.insert("key2".into(), string("value2")).unwrap();

    // Get
    assert_eq!(
        db.get("key1").unwrap(),
        Some(string("value1"))
    );

    assert_eq!(
        db.get("key2").unwrap(),
        Some(string("value2"))
    );

    assert_eq!(
        db.get("nonexistent").unwrap(),
        None
    );

    // Update
    db.insert("key1".into(), string("new_value1")).unwrap();

    assert_eq!(
        db.get("key1").unwrap(),
        Some(string("new_value1"))
    );

    // Delete
    db.delete("key2").unwrap();

    assert_eq!(
        db.get("key2").unwrap(),
        None
    );
}

#[test]
fn test_flush_and_compaction() {
    let dir = tempdir().unwrap();
    let db = create_test_db(dir.path());

    // Trigger flush 1
    db.insert("a".into(), string("1")).unwrap();
    db.insert("b".into(), string("2")).unwrap();

    // Trigger flush 2.
    // This should also trigger compaction.
    db.insert("c".into(), string("3")).unwrap();
    db.insert("d".into(), string("4")).unwrap();

    assert_eq!(db.get("a").unwrap(), Some(string("1")));
    assert_eq!(db.get("b").unwrap(), Some(string("2")));
    assert_eq!(db.get("c").unwrap(), Some(string("3")));
    assert_eq!(db.get("d").unwrap(), Some(string("4")));

    // Update and flush again
    db.insert("a".into(), string("11")).unwrap();
    db.insert("b".into(), string("22")).unwrap();

    assert_eq!(db.get("a").unwrap(), Some(string("11")));
    assert_eq!(db.get("b").unwrap(), Some(string("22")));
}

#[test]
fn test_wal_recovery() {
    let dir = tempdir().unwrap();

    {
        let db = create_test_db(dir.path());

        db.insert("key1".into(), string("val1"))
            .unwrap();

        // Database is dropped here.
        // Data was not flushed because threshold is 2.
    }

    {
        let db = create_test_db(dir.path());

        // Verify recovery from WAL.
        assert_eq!(
            db.get("key1").unwrap(),
            Some(string("val1"))
        );

        // Add another entry to trigger a flush.
        db.insert("key2".into(), string("val2"))
            .unwrap();
    }

    {
        let db = create_test_db(dir.path());

        assert_eq!(
            db.get("key1").unwrap(),
            Some(string("val1"))
        );

        assert_eq!(
            db.get("key2").unwrap(),
            Some(string("val2"))
        );
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
            let key = format!("key_{i}");
            let value = format!("val_{i}");

            db_clone
                .insert(key.clone(), string(&value))
                .unwrap();

            assert_eq!(
                db_clone.get(&key).unwrap(),
                Some(string(&value))
            );
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Check all values are present.
    for i in 0..10 {
        let key = format!("key_{i}");
        let value = format!("val_{i}");

        assert_eq!(
            db.get(&key).unwrap(),
            Some(string(&value))
        );
    }
}