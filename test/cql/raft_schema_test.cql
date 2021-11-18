CREATE TABLE t1 (pk INT PRIMARY KEY, c TEXT);
ALTER TABLE t1 ADD c2 TEXT;
SELECT * FROM system_schema.scylla_tables;
