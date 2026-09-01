use std::io;

mod database;
mod lsm;
mod wal;

use database::Database;

use std::sync::{Arc, RwLock};

fn main() -> io::Result<()> {
    // Flush memtable after 2 entries.
    // Compact when 2 SSTables exist.
    let db = Arc::new(RwLock::new(Database::new(2, 2, 3)?));
    let db2 = Arc::clone(&db);

    std::thread::spawn(move || {
        let mut db = db2.write().unwrap();

        db.insert("name".into(), "krishna".into()).unwrap();
    });



    println!("--- INSERTING ---");
    db.write();
    db.insert("name".to_string(), "Bob".to_string())?;
    db.insert("age".to_string(), "19".to_string())?;
    // Flush #1 happens here.

    db.insert("cat".to_string(), "Alice".to_string())?;
    db.insert("cot".to_string(), "Charlie".to_string())?;
    // Flush #2 happens here.
    // Since there are now 2 SSTables, compaction should happen.

    db.insert("cash".to_string(), "100".to_string())?;
    // Still in memory.

    println!("\n--- INITIAL READS ---");

    print_value(&db, "name")?;
    print_value(&db, "age")?;
    print_value(&db, "cat")?;
    print_value(&db, "cot")?;
    print_value(&db, "cash")?;

    println!("\n--- OVERWRITE TEST ---");

    db.insert("name".to_string(), "Krishna".to_string())?;

    // This should return the newest value, not "Bob".
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
        Some(value) => println!("{key}: {value}"),
        None => println!("{key}: <not found>"),
    }

    Ok(())
}
