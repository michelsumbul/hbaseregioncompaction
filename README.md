This java app start the major compaction of a hbase table region by region. you can specify the number of regions that can be compacting at the same time
This code have been developped on HBase 1.1.2

Usage: 
java -cp <jar-file> <tablename> <concurrency>
Example:
java -cp hbaseCompaction-1.0-jar-with-dependencies.jar hbasecompaction.hbasecompaction.compactBigTable test_table2 5
