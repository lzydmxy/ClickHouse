CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.03129_test sync;

CREATE TABLE test.03129_test (key1 String, key2 UInt32, value UInt32) Engine=ReplicatedRocksDB; -- { serverError 36 }
CREATE TABLE test.03129_test (key1 String, key2 UInt32, value UInt32) Engine=ReplicatedRocksDB PRIMARY KEY(key3); -- { serverError 47 }
CREATE TABLE test.03129_test (key1 String, key2 UInt32, value UInt32) Engine=ReplicatedRocksDB PRIMARY KEY(key1, key2);

INSERT INTO test.03129_test SELECT '1_1', number, number FROM numbers(1000);
SELECT COUNT(1) == 1000 FROM test.03129_test;

-- duplicate primary key check
INSERT INTO test.03129_test SELECT '1_1', number, number FROM numbers(1000);
SELECT COUNT(1) == 1000 FROM test.03129_test;

TRUNCATE TABLE test.03129_test sync;

INSERT INTO test.03129_test SELECT concat(toString(number), '_1'), 1, number FROM numbers(1000);
SELECT COUNT(1) == 1000 FROM test.03129_test;

SELECT uniqExact(key1) == 32 FROM (SELECT * FROM test.03129_test LIMIT 32 SETTINGS max_block_size = 1);
SELECT uniqExact(key2) == 1 FROM (SELECT * FROM test.03129_test LIMIT 32 SETTINGS max_block_size = 1);
SELECT SUM(value) == 1 + 99 + 900 FROM test.03129_test WHERE key1 IN ('1_1', '99_1', '900_1');

DROP TABLE IF EXISTS test.03129_test;

CREATE TABLE test.03129_test (key1 String, key2 UInt32, key3 UInt16, value UInt32) Engine=ReplicatedRocksDB PRIMARY KEY(key1, key2, key3);

INSERT INTO test.03129_test SELECT '1_1', 1, number, number FROM numbers(1000);
SELECT COUNT(1) == 1000 FROM test.03129_test;

-- duplicate primary key check
INSERT INTO test.03129_test SELECT '1_1', 1, number, number FROM numbers(1000);
SELECT COUNT(1) == 1000 FROM test.03129_test;

TRUNCATE TABLE test.03129_test sync;

INSERT INTO test.03129_test SELECT concat(toString(number), '_1'), 1, 2, number FROM numbers(1000);
SELECT COUNT(1) == 1000 FROM test.03129_test;

SELECT uniqExact(key1) == 32 FROM (SELECT * FROM test.03129_test LIMIT 32 SETTINGS max_block_size = 1);
SELECT uniqExact(key2) == 1 FROM (SELECT * FROM test.03129_test LIMIT 32 SETTINGS max_block_size = 1);
SELECT uniqExact(key3) == 1 FROM (SELECT * FROM test.03129_test LIMIT 32 SETTINGS max_block_size = 1);
SELECT SUM(value) == 1 + 99 + 900 FROM test.03129_test WHERE key1 IN ('1_1', '99_1', '900_1');

-- DROP TABLE IF EXISTS test.03129_test;
