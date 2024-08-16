import logging
import random
import string
import time
import pytest
from helpers.cluster import ClickHouseCluster

TABLE_NAME1 = "test1"
TABLE_NAME2 = "test2"

@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)

        cluster.add_instance(
            "node1",
            main_configs=["configs/config.d/storage_conf1.xml"],
            macros={"replica": "1"},
            with_minio=True,
            with_zookeeper=False,
        )

        cluster.add_instance(
            "node2",
            main_configs=["configs/config.d/storage_conf2.xml"],
            macros={"replica": "2"},
            with_zookeeper=False,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()

def create_table(cluster, additional_settings=None):
    create_table_statement1 = f"""
        CREATE TABLE {TABLE_NAME1}
        (
            `id` String,
            `name` String,
            `age` UInt32,
            `weight` Float32
        )
        ENGINE = ReplicatedRocksDB('test2')
        PRIMARY KEY id;
    """
    create_table_statement2 = f"""
        CREATE TABLE {TABLE_NAME2}
        (
            `id` String,
            `name` String,
            `age` UInt32,
            `weight` Float32
        )
        ENGINE = ReplacingMergeTree
        ORDER BY id;
    """
    for node in cluster.instances.values():
        node.query(create_table_statement1)
        node.query(create_table_statement2)

def drop_table(cluster):
    for node in cluster.instances.values():
        node.query(f"DROP TABLE IF EXISTS {TABLE_NAME1}")
        node.query(f"DROP TABLE IF EXISTS {TABLE_NAME2}")

def assert_mutation(node, sql , value):
    done = False
    for wait_times_for_mutation in range(10):
        time.sleep(1) # wait for replication 10 seconds max
        real_value = node.query(sql).strip()
        print(f"{sql}, query value {real_value}, expect value {value}")
        if real_value == value:
            done = True
            break
    return done

def insert(cluster, begin, batch, verify=True):
    insert_sql = f"""
        INSERT INTO {TABLE_NAME1}
        SELECT toString(number), toString(cityHash64(number)), number, toFloat32(number)
        FROM numbers({begin},{batch});
    """
    node1, node2 = cluster.instances.values()
    node1.query(insert_sql)
    if verify:
        select_sql1 = f"SELECT COUNT() FROM {TABLE_NAME1};"
        select_sql2 = f"SELECT COUNT() FROM {TABLE_NAME2};"
        assert( node1.query(select_sql1) == node1.query(select_sql2) )
        assert( node2.query(select_sql1) == node2.query(select_sql2) )

def select(cluster, ids, verify=True):
    node1, node2 = cluster.instances.values()
    if verify:
        select_sql1 = f"SELECT COUNT() FROM {TABLE_NAME1} WHERE id IN {ids};"
        select_sql2 = f"SELECT COUNT() FROM {TABLE_NAME2} WHERE id IN {ids};"
        assert( int(node1.query(select_sql1)) == len(ids) )
        assert( int(node1.query(select_sql2)) == len(ids) )

def delete(cluster, ids, verify=True):
    delete_sql = f"ALTER TABLE {TABLE_NAME1} DELETE WHERE id IN {ids};"
    node1, node2 = cluster.instances.values()
    node1.query(delete_sql)
    if verify:
        select_sql1 = f"SELECT COUNT() FROM {TABLE_NAME1} WHERE id IN {ids};"
        select_sql2 = f"SELECT COUNT() FROM {TABLE_NAME2} FINAL WHERE id IN {ids};"
        assert( int(node1.query(select_sql1)) == 0 )
        assert( assert_mutation(node1, select_sql2, '0') == True )

def update(cluster, id, value, verify=True):
    update_sql = f"ALTER TABLE {TABLE_NAME1} UPDATE age={value} WHERE id = '{id}';"
    node1, node2 = cluster.instances.values()
    node1.query(update_sql)
    if verify:
        select_sql1 = f"SELECT age FROM {TABLE_NAME1} WHERE id = '{id}';"
        select_sql2 = f"SELECT age FROM {TABLE_NAME2} FINAL WHERE id = '{id}';"
        assert( int(node1.query(select_sql1)) == int(value) )
        assert( assert_mutation(node1, select_sql2, value) == True )

def test_insert_replicated_rocksdb_mergetree(cluster):
    create_table(cluster)
    try:
        insert(cluster, 0, 10, True)
    finally:
        drop_table(cluster)

def test_select_replicated_rocksdb_mergetree(cluster):
    create_table(cluster)
    try:
        insert(cluster, 0, 10, True)
        select(cluster, ('1','2'), True)
    finally:
        drop_table(cluster)

def test_delete_replicated_rocksdb_mergetree(cluster):
    create_table(cluster)
    try:
        insert(cluster, 0, 10, True)
        delete(cluster, ('1','2'), True)
    finally:
        drop_table(cluster)

def test_update_replicated_rocksdb_mergetree(cluster):
    create_table(cluster)
    try:
        insert(cluster, 0, 10, True)
        update(cluster, '1', '2', True)
    finally:
        drop_table(cluster)
