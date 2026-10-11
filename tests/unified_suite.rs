use tempfile::tempdir;

use kv_store::testing::TestCluster;
use kv_store::wal::log_record::Value;
use kv_store::{Config, Database};

#[test]
fn test_unified_correctness_suite() {
    // -------------------------------------------------------------------------
    // 1. STANDALONE DATABASE ENGINE STRESS & CORRECTNESS (CRUD Operations)
    // -------------------------------------------------------------------------
    let standalone_dir = tempdir().unwrap();
    let standalone_db =
        Database::open_in_dir(standalone_dir.path(), Config::new(3, 200, 4)).unwrap();
    let num_ops = 10_000;

    // 1a. Sequential Insert
    for i in 0..num_ops {
        standalone_db
            .insert(
                format!("key_{:06}", i),
                Value::String(format!("val_{}", i)),
            )
            .unwrap();
    }

    // 1b. Existing Key Lookup (GET Hits)
    for i in 0..num_ops {
        let k = format!("key_{:06}", i);
        let val = standalone_db.get(&k).unwrap();
        assert_eq!(val, Some(Value::String(format!("val_{}", i))));
    }

    // 1c. Missing Key Lookup (GET Misses)
    for i in 0..num_ops {
        let k = format!("missing_{:06}", i);
        let val = standalone_db.get(&k).unwrap();
        assert_eq!(val, None);
    }

    // 1d. Overwrite / Update
    for i in 0..num_ops {
        let k = format!("key_{:06}", i);
        standalone_db
            .insert(k, Value::String(format!("new_val_{}", i)))
            .unwrap();
    }
    for i in 0..num_ops {
        let k = format!("key_{:06}", i);
        let val = standalone_db.get(&k).unwrap();
        assert_eq!(val, Some(Value::String(format!("new_val_{}", i))));
    }

    // 1e. Delete Operations
    for i in 0..(num_ops / 2) {
        standalone_db.delete(format!("key_{:06}", i)).unwrap();
    }
    for i in 0..(num_ops / 2) {
        let k = format!("key_{:06}", i);
        assert_eq!(standalone_db.get(&k).unwrap(), None);
    }
    for i in (num_ops / 2)..num_ops {
        let k = format!("key_{:06}", i);
        assert_eq!(
            standalone_db.get(&k).unwrap(),
            Some(Value::String(format!("new_val_{}", i)))
        );
    }

    // -------------------------------------------------------------------------
    // 2. DISTRIBUTED RAFT CONSENSUS CLUSTER CORRECTNESS
    // -------------------------------------------------------------------------
    let mut cluster_1 = TestCluster::new(1);
    cluster_1.execute_workload(1000);
    let c1_correct = cluster_1.verify_correctness();

    let mut cluster_3 = TestCluster::new(3);
    cluster_3.execute_workload(1000);
    let c3_correct = cluster_3.verify_correctness();

    let mut cluster_5 = TestCluster::new(5);
    cluster_5.execute_workload(1000);
    let c5_correct = cluster_5.verify_correctness();

    assert!(
        c1_correct && c3_correct && c5_correct,
        "All cluster correctness checks must pass"
    );
}
