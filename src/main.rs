use std::io;
use std::sync::Arc;

use kv_store::database::Database;
use kv_store::wal::log_record::Value;

fn main() -> io::Result<()> {
    // Flush memtable after 2 entries.
    // Compact when 2 SSTables exist.
    let db = Arc::new(Database::new(2, 2, 3)?);

    let db2 = Arc::clone(&db);

    std::thread::spawn(move || {
        db2.insert("name".into(), Value::String("krishna".into()))
            .unwrap();
    })
    .join()
    .unwrap();

    println!("--- INSERTING ---");

    db.insert("name".to_string(), Value::String("Bob".to_string()))?;

    db.insert("age".to_string(), Value::Int(19))?;

    // Flush #1 happens here.

    db.insert("cat".to_string(), Value::String("Alice".to_string()))?;

    db.insert("cot".to_string(), Value::String("Charlie".to_string()))?;

    // Flush #2 happens here.
    // Since there are now 2 SSTables, compaction should happen.

    db.insert("cash".to_string(), Value::Int(100))?;

    // Still in memory.

    println!("\n--- INITIAL READS ---");

    print_value(&db, "name")?;
    print_value(&db, "age")?;
    print_value(&db, "cat")?;
    print_value(&db, "cot")?;
    print_value(&db, "cash")?;

    println!("\n--- OVERWRITE TEST ---");

    db.insert("name".to_string(), Value::String("Krishna".to_string()))?;

    // This should return the newest value.
    print_value(&db, "name")?;

    println!("\n--- DELETE TEST ---");

    db.delete("cat")?;

    print_value(&db, "cat")?;

    println!("\n--- MISSING KEY TEST ---");

    print_value(&db, "does_not_exist")?;

    Ok(())
}

fn print_value(db: &Database, key: &str) -> io::Result<()> {
    match db.get(key)? {
        Some(value) => println!("{key}: {:?}", value),
        None => println!("{key}: <not found>"),
    }

    Ok(())
}
