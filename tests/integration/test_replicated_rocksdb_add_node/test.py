import logging
import os
import time
import random
import string
import pytest
from helpers.cluster import ClickHouseCluster

TABLE_NAME = "test1"

log = logging.getLogger(__name__)

curr_dir = "configs/config.d/{}"
root_dir = "/etc/clickhouse-server/config.d/{}"
data_dir = "/var/lib/clickhouse/data"

old_config = ("storage_2_node1.xml", "storage_2_node2.xml", "storage_2_node3.xml")
new_config = ("storage_3_node1.xml", "storage_3_node2.xml", "storage_3_node3.xml")

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node1",
            main_configs=[curr_dir.format(old_config[0])],
            macros={"replica": "1"},
            with_minio=True,
            with_zookeeper=False,
            stay_alive=True,
        )
        cluster.add_instance(
            "node2",
            main_configs=[curr_dir.format(old_config[1])],
            macros={"replica": "2"},
            with_minio=True,
            with_zookeeper=False,
            stay_alive=True,
        )
        cluster.add_instance(
            "node3",
            main_configs=[curr_dir.format(old_config[2])],
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

def insert(node, begin, count):
    select_sql = f"SELECT COUNT() FROM {TABLE_NAME};"
    cnt = int(node.query(select_sql))
    for i in range(count):
        insert_sql = f"""
            INSERT INTO {TABLE_NAME}
            SELECT toString(number), toString(cityHash64(number)), number, toFloat32(number)
            FROM numbers({begin},1);
        """
        node.query(insert_sql)
        begin += 1
    assert( int(node.query(select_sql)) == cnt + count)

def assert_select(node, count):
    select_sql = f"SELECT COUNT() FROM {TABLE_NAME};"
    assert( int(node.query(select_sql)) == count )

def replicated_node_select(node, count):
    for wait_times_for_mutation in range(10):
        time.sleep(1)   # wait for replication 10 seconds max
        select_sql = f"SELECT COUNT() FROM {TABLE_NAME};"
        if ( int(node.query(select_sql)) == count ):
            return True
    return False

def replace_config_file(node, old_config, new_config):
    log.debug("old config {}, new config {}".format(old_config, new_config))
    config = open(new_config, "r")
    new_str = config.read()
    node.replace_config(old_config, new_str)

def add_node(cluster):
    for i in range(len(cluster.instances)):
        node = list(cluster.instances.values())[i]
        new_config_path = os.path.join(SCRIPT_DIR, curr_dir.format(new_config[i]))
        replace_config_file(node, root_dir.format(old_config[i]), new_config_path)
        if i == 0 or i == 1:
            node.query("SYSTEM RELOAD CONFIG")
        elif i == 2:
            #remove log and state, add raft server config, then restart
            log.debug("Clear log and state, then restart node3")
            node.exec_in_container(['bash', '-c', f'rm -rf {data_dir}/raft_log/* {data_dir}/raft_state/*'])
            node.restart_clickhouse()

def test_add_node(cluster):
    create_table(cluster)
    try:
        node1, node2, node3 = cluster.instances.values()
        #insert and create snapshot
        count = 101
        log.debug("Insert data one by one, count {}".format(count))
        time.sleep(1)
        insert(node1, 0, count)
        node1.wait_for_log_line("Created snapshot", timeout=60, look_behind_lines=1000000)
        log.debug("Add node3 to cluster")
        add_node(cluster)
        log.debug("Wait apply snapshot")
        node3.wait_for_log_line("Apply snapshot", timeout=60, look_behind_lines=1000000)
        assert_select(node1, count)
        assert (replicated_node_select(node2, count) == True)
        assert (replicated_node_select(node3, count) == True)
    finally:
        drop_table_cluster(cluster)
