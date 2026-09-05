# Rust-Database

sudo perf stat \
  -e cycles,instructions,context-switches,cpu-migrations,page-faults,minor-faults,major-faults \
  ./target/release/deps/unified_suite-9f095a716cb6b1fa --nocapture

running 1 test

==========================================================================================
        UNIFIED STRESS TEST, CORRECTNESS VERIFICATION & BENCHMARK SUITE                   
==========================================================================================

--- 1. CORRECTNESS VERIFICATION RESULTS ---
Standalone Engine Correctness (20,000 Ops):  PASSED [100% Data Match]
1-Node Raft Cluster Correctness (10,000 Ops): PASSED [100% Data Match]
3-Node Raft Cluster Correctness (10,000 Ops): PASSED [100% Data Match]
5-Node Raft Cluster Correctness (10,000 Ops): PASSED [100% Data Match]

--- 2. BENCHMARK METRICS SUMMARY TABLE ---
Configuration          | Throughput (ops/s) | p50 Latency  | p95 Latency  | p99 Latency 
------------------------------------------------------------------------------------------
Standalone SET (20k)   | 4910.06         | 7.448µs      | 12.632µs     | 21.538µs    
Standalone GET (20k)   | 119738.69       | 7.743µs      | 11.39µs      | 17.443µs    
1-Node Raft (10k)      | 9605.81         | 7.696µs      | 11.304µs     | 21.463µs    
3-Node Raft (10k)      | 3261.39         | 23.036µs     | 34.955µs     | 326.753µs   
5-Node Raft (10k)      | 1978.86         | 38.397µs     | 61.43µs      | 1.264357ms  

--- 3. RESOURCE FOOTPRINT & COMPACTION OVERHEAD ---
Process Memory (RSS):    6.65 MB
On-Disk Storage Size:    965.74 KB
Compactions Triggered:   25 passes
Total Compaction Time:   3.667924s
test run_unified_stress_correctness_and_benchmarks ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 13.52s


 Performance counter stats for './target/release/deps/unified_suite-9f095a716cb6b1fa --nocapture':

    33,590,124,199      cycles                                                                
    29,149,101,224      instructions                     #    0.87  insn per cycle            
               136      context-switches                                                      
                 8      cpu-migrations                                                        
            13,475      page-faults                                                           
            13,473      minor-faults                                                          
                 0      major-faults                                                          

      13.519647731 seconds time elapsed

       6.760684000 seconds user
       6.756684000 seconds sys


krishna@mx:~/Coding/Rust/kv-store
$ sudo perf record -g --call-graph dwarf \
  ./target/release/deps/unified_suite-9f095a716cb6b1fa --nocapture

running 1 test

==========================================================================================
        UNIFIED STRESS TEST, CORRECTNESS VERIFICATION & BENCHMARK SUITE                   
==========================================================================================

--- 1. CORRECTNESS VERIFICATION RESULTS ---
Standalone Engine Correctness (20,000 Ops):  PASSED [100% Data Match]
1-Node Raft Cluster Correctness (10,000 Ops): PASSED [100% Data Match]
3-Node Raft Cluster Correctness (10,000 Ops): PASSED [100% Data Match]
5-Node Raft Cluster Correctness (10,000 Ops): PASSED [100% Data Match]

--- 2. BENCHMARK METRICS SUMMARY TABLE ---
Configuration          | Throughput (ops/s) | p50 Latency  | p95 Latency  | p99 Latency 
------------------------------------------------------------------------------------------
Standalone SET (20k)   | 4839.09         | 7.487µs      | 11.954µs     | 22.384µs    
Standalone GET (20k)   | 113765.06       | 7.988µs      | 12.457µs     | 18.51µs     
1-Node Raft (10k)      | 9321.01         | 7.782µs      | 12.797µs     | 23.791µs    
3-Node Raft (10k)      | 3127.64         | 23.2µs       | 38.335µs     | 209.164µs   
5-Node Raft (10k)      | 1876.03         | 38.955µs     | 62.827µs     | 515.48µs    

--- 3. RESOURCE FOOTPRINT & COMPACTION OVERHEAD ---
Process Memory (RSS):    10.90 MB
On-Disk Storage Size:    965.74 KB
Compactions Triggered:   25 passes
Total Compaction Time:   3.732311s
test run_unified_stress_correctness_and_benchmarks ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 14.04s

[ perf record: Woken up 1749 times to write data ]
[ perf record: Captured and wrote 437.401 MB perf.data (55978 samples) ]








 sudo perf stat \
  -e cycles,instructions,context-switches,cpu-migrations,page-faults,minor-faults,major-faults \
  ./target/release/deps/unified_suite-9f095a716cb6b1fa --nocapture

running 1 test

==========================================================================================
        UNIFIED STRESS TEST, CORRECTNESS VERIFICATION & BENCHMARK SUITE                   
==========================================================================================

--- 1. CORRECTNESS VERIFICATION RESULTS ---
Standalone Engine Correctness (20,000 Ops):  PASSED [100% Data Match]
1-Node Raft Cluster Correctness (10,000 Ops): PASSED [100% Data Match]
3-Node Raft Cluster Correctness (10,000 Ops): PASSED [100% Data Match]
5-Node Raft Cluster Correctness (10,000 Ops): PASSED [100% Data Match]

--- 2. BENCHMARK METRICS SUMMARY TABLE ---
Configuration          | Throughput (ops/s) | p50 Latency  | p95 Latency  | p99 Latency 
------------------------------------------------------------------------------------------
Standalone SET (20k)   | 5010.49         | 7.448µs      | 12.203µs     | 22.251µs    
Standalone GET (20k)   | 119647.38       | 7.765µs      | 9.241µs      | 16.316µs    
1-Node Raft (10k)      | 9829.18         | 7.679µs      | 12.192µs     | 21.267µs    
3-Node Raft (10k)      | 3246.18         | 23.014µs     | 33.254µs     | 195.697µs   
5-Node Raft (10k)      | 1962.30         | 38.311µs     | 55.451µs     | 728.238µs   

--- 3. RESOURCE FOOTPRINT & COMPACTION OVERHEAD ---
Process Memory (RSS):    6.77 MB
On-Disk Storage Size:    965.74 KB
Compactions Triggered:   25 passes
Total Compaction Time:   3.591763s
test run_unified_stress_correctness_and_benchmarks ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 13.47s


 Performance counter stats for './target/release/deps/unified_suite-9f095a716cb6b1fa --nocapture':

    33,444,990,943      cycles                                                                
    29,154,448,839      instructions                     #    0.87  insn per cycle            
               161      context-switches                                                      
                 9      cpu-migrations                                                        
            13,527      page-faults                                                           
            13,525      minor-faults                                                          
                 0      major-faults                                                          

      13.474014531 seconds time elapsed

       6.665508000 seconds user
       6.801539000 seconds sys


krishna@mx:~/Coding/Rust/kv-store
$ sudo perf record -g --call-graph dwarf \
  ./target/release/deps/unified_suite-9f095a716cb6b1fa --nocapture

running 1 test

==========================================================================================
        UNIFIED STRESS TEST, CORRECTNESS VERIFICATION & BENCHMARK SUITE                   
==========================================================================================

--- 1. CORRECTNESS VERIFICATION RESULTS ---
Standalone Engine Correctness (20,000 Ops):  PASSED [100% Data Match]
1-Node Raft Cluster Correctness (10,000 Ops): PASSED [100% Data Match]
3-Node Raft Cluster Correctness (10,000 Ops): PASSED [100% Data Match]
5-Node Raft Cluster Correctness (10,000 Ops): PASSED [100% Data Match]

--- 2. BENCHMARK METRICS SUMMARY TABLE ---
Configuration          | Throughput (ops/s) | p50 Latency  | p95 Latency  | p99 Latency 
------------------------------------------------------------------------------------------
Standalone SET (20k)   | 4810.78         | 7.503µs      | 11.969µs     | 22.98µs     
Standalone GET (20k)   | 111091.04       | 7.859µs      | 12.986µs     | 21.092µs    
1-Node Raft (10k)      | 9308.63         | 7.745µs      | 12.024µs     | 21.855µs    
3-Node Raft (10k)      | 3121.03         | 23.356µs     | 39.248µs     | 244.423µs   
5-Node Raft (10k)      | 1882.33         | 41.505µs     | 65.202µs     | 853.316µs   

--- 3. RESOURCE FOOTPRINT & COMPACTION OVERHEAD ---
Process Memory (RSS):    10.55 MB
On-Disk Storage Size:    965.74 KB
Compactions Triggered:   25 passes
Total Compaction Time:   3.750094s
test run_unified_stress_correctness_and_benchmarks ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 14.06s

[ perf record: Woken up 1750 times to write data ]
[ perf record: Captured and wrote 437.707 MB perf.data (56087 samples) ]