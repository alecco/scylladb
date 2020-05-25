-- Check gc_grace_seconds is used as TTL for system.paxos entries
CREATE KEYSPACE k WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE k;
CREATE TABLE t1 (pk int PRIMARY KEY) WITH gc_grace_seconds = 1;
CREATE TABLE t2 (pk int PRIMARY KEY) WITH gc_grace_seconds = 100;
INSERT INTO  t1 (pk) VALUES (1) IF NOT EXISTS;
INSERT INTO  t2 (pk) VALUES (2) IF NOT EXISTS;
SELECT row_key, TTL(promise) FROM system.paxos;
