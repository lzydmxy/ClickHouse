import logging
import random
import string

import pytest
from helpers.cluster import ClickHouseCluster

TABLE_NAME = "test1"


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
    create_table_statement = f"""
        CREATE TABLE {TABLE_NAME}
        (
            `id` String,
            `name` String,
            `age` UInt32,
            `weight` Float32
        )
        ENGINE = ReplicatedRocksDB
        PRIMARY KEY id;
    """
    for node in cluster.instances.values():
        node.query(create_table_statement)

def drop_table(cluster):
    for node in cluster.instances.values():
        node.query(f"DROP TABLE IF EXISTS {TABLE_NAME}")

def insert(cluster, begin, batch, verify=True):
    insert_sql = f"""
        INSERT INTO {TABLE_NAME}
        SELECT toString(number), toString(cityHash64(number)), number, toFloat32(number)
        FROM numbers({begin},{batch});
    """
    node1, node2 = cluster.instances.values()
    node1.query(insert_sql)
    if verify:
        select_sql = f"SELECT COUNT() FROM {TABLE_NAME};"
        assert( node1.query(select_sql) == node2.query(select_sql) )

def select(cluster, ids, verify=True):
    node1, node2 = cluster.instances.values()
    if verify:
        select_sql = f"SELECT COUNT() FROM {TABLE_NAME} WHERE id IN {ids};"
        assert( int(node1.query(select_sql)) == len(ids) )
        assert( int(node1.query(select_sql)) == int(node2.query(select_sql)) )

def delete(cluster, ids, verify=True):
    delete_sql = f"ALTER TABLE {TABLE_NAME} DELETE WHERE id IN {ids};"
    node1, node2 = cluster.instances.values()
    node1.query(delete_sql)
    if verify:
        select_sql = f"SELECT COUNT() FROM {TABLE_NAME} WHERE id IN {ids};"
        assert( int(node1.query(select_sql)) == 0 )
        assert( int(node1.query(select_sql)) == int(node2.query(select_sql)) )

def update(cluster, id, value, verify=True):
    update_sql = f"ALTER TABLE {TABLE_NAME} UPDATE age={value} WHERE id = '{id}';"
    node1, node2 = cluster.instances.values()
    node1.query(update_sql)
    if verify:
        select_sql = f"SELECT age FROM {TABLE_NAME} WHERE id = '{id}';"
        assert( int(node1.query(select_sql)) == value )
        assert( int(node2.query(select_sql)) == value )


def test_insert_replicated(cluster):
    create_table(cluster)
    try:
        insert(cluster, 0, 10, True)
    finally:
        drop_table(cluster)

def test_select_replicated(cluster):
    create_table(cluster)
    try:
        insert(cluster, 0, 10, True)
        select(cluster, ('1','2'), True)
    finally:
        drop_table(cluster)

def test_delete_replicated(cluster):
    create_table(cluster)
    try:
        insert(cluster, 0, 10, True)
        delete(cluster, ('1','2'), True)
    finally:
        drop_table(cluster)

def test_update_replicated(cluster):
    create_table(cluster)
    try:
        insert(cluster, 0, 10, True)
        update(cluster, '1', 2, True)
    finally:
        drop_table(cluster)
