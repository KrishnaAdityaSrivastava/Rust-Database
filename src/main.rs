mod logger;
mod database;
mod log_record;


use database::Database;

fn main() {
    let mut db = Database::new();
    db.recover().expect("Failed to recover database");
    // db.insert(
    //     "name".to_string(),
    //     "Bob".to_string(),
    // );

    // db.delete("name");

    // db.insert(
    //     "age".to_string(),
    //     "19".to_string(),
    // );

    println!(
        "Name: {}",
        db.get("name").unwrap_or(&"Not found".to_string())
    );

    println!(
        "Age: {}",
        db.get("age").unwrap_or(&"Not found".to_string())
    );
}