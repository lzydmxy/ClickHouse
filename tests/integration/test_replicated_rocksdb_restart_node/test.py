import logging
import time
import random
import string
import pytest
from helpers.cluster import ClickHouseCluster

TABLE_NAME = "test1"

curr_dir = "configs/config.d/{}"
root_dir = "/etc/clickhouse-server/config.d/{}"
config = ("storage_3_node1.xml", "storage_3_node2.xml", "storage_3_node3.xml")

@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node1",
            main_configs=[curr_dir.format(config[0])],
            macros={"replica": "1"},
            with_minio=True,
            with_zookeeper=False,
            stay_alive=True,
        )
        cluster.add_instance(
            "node2",
            main_configs=[curr_dir.format(config[1])],
            macros={"replica": "2"},
            with_minio=True,
            with_zookeeper=False,
            stay_alive=True,
        )
        cluster.add_instance(
            "node3",
            main_configs=[curr_dir.format(config[2])],
            macros={"replica": "3"},
            with_minio=True,
            with_zookeeper=False,
            stay_alive=True,
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

def drop_table_cluster(cluster):
    for node in cluster.instances.values():
        drop_table(node)

def drop_table(node):
    node.query(f"DROP TABLE IF EXISTS {TABLE_NAME}")

def replicated_node_select(node, count):
    for wait_times_for_mutation in range(10):
        time.sleep(1)   # wait for replication 10 seconds max
        select_sql = f"SELECT COUNT() FROM {TABLE_NAME};"
        if ( int(node.query(select_sql)) == count ):
            return True
    return False

def insert(node, begin, batch):
    select_sql = f"SELECT COUNT() FROM {TABLE_NAME};"
    cnt = int(node.query(select_sql))
    insert_sql = f"""
        INSERT INTO {TABLE_NAME}
        SELECT toString(number), toString(cityHash64(number)), number, toFloat32(number)
        FROM numbers({begin},{batch});
    """
    node.query(insert_sql)
    assert( int(node.query(select_sql)) == cnt + batch)

def getRaftNode(cluster):
    follower = []
    for node in cluster.instances.values():
        log = node.grep_in_log("BECOME LEADER")
        if len(log) > 0:
            leader = node
            print("Leader : " + node.name)
            print(log)
        else:
            follower.append(node)
            print("Follower : " + node.name)
    return leader, follower

def test_forward(cluster):
    create_table(cluster)
    try:
        leader, follower = getRaftNode(cluster)
        batch = 10
        print("Insert data to follower[0] " + follower[0].name)
        insert(follower[0], 0, batch)
        print("Check leader data " + leader.name)
        replicated_node_select(leader, batch)
        print("Check follower[1] data " + follower[1].name)
        replicated_node_select(follower[1], batch)
    finally:
        drop_table_cluster(cluster)
def test_restart_node(cluster):
    create_table(cluster)
    try:
        leader, follower = getRaftNode(cluster)
        print("Stop clickhouse leader " + leader.name)
        leader.stop_clickhouse()
        batch = 10
        time.sleep(5) # waiting for re-election
        print("Insert data to follower[0] " + follower[0].name)
        insert(follower[0], 0, batch)
        print("Check follower[1] data " + follower[1].name)
        replicated_node_select(follower[1], batch)
        print("Start old leader " + leader.name)
        leader.start_clickhouse()
        leader.wait_start(5)
        print("Check old leader data " + leader.name)
        replicated_node_select(leader, batch)
    finally:
        drop_table_cluster(cluster)
