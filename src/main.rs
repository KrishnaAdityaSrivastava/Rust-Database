mod logger;
mod database;
mod log_record;
mod sstable;


use database::Database;

fn main() {
    let mut db = Database::new(2);
    // db.recover().expect("Failed to recover database");
    db.insert(
        "name".to_string(),
        "Bob".to_string(),
    );

    // db.delete("name");

    db.insert(
        "age".to_string(),
        "19".to_string(),
    );
    db.insert(
        "cat".to_string(),
        "Alice".to_string(),
    );
    db.insert(
        "cot".to_string(),
        "Alice".to_string(),
    );
    db.insert(
        "cash".to_string(),
        "Alice".to_string(),
    );

    println!(
        "Name: {}",
        db.get("name").unwrap_or("Not found".to_string())
    );

    println!(
        "Age: {}",
        db.get("age").unwrap_or("Not found".to_string())
    );
    println!(
        "Cat: {}",
        db.get("cat").unwrap_or("Not found".to_string())
    );
}