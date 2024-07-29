import os
import logging
import subprocess
import logging
import sys
import urllib
import time
import argparse
import json
import shutil
import copy

FORMAT = '%(asctime)-15s [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s'
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format=FORMAT)
basePath = os.path.join(os.getcwd(), "cfs-client")

openSourceCfg = {
    "masterAddr": "sparkchubaofs.jd.local",
    "mountPoint": "/cfs/mnt",
    "volName": "ltptest",
    "owner": "ltptest",
    "logDir": "/log/clickhouse/cfs/%s",
    "logLevel": "warn",
    "exporterPort": 9500,
    "warnLogDir": "/log/clickhouse/cfs/UMP-Monitor/logs/",
    "profPort": "17410",
    "rdonly": False
}
innerCfg = {
    "mountpoint": "/mnt/intest",
    "volname": "webtest",
    "master": "dbbak.jd.local",
    "logpath": "/export/Logs/cfs/%s",
    "profport": "10094",
    "owner": "cfs",
    "loglvl": "warn",
    "rdonly": False
}

clusters = [
    {
        "clusterName" : "cfs_dbBack",
        "clusterAddr" : "cn.chubaofs-seqwrite.jd.local",
        "type" : "seqwrite version",
        "fusecfg" : innerCfg,
        "cfs-client" : "http://storage.jd.local/dpgimage/cfsdbbak/cfs-client",
    },
    {
        "clusterName" : "yn_cfs_dbback",
        "clusterAddr" : "id.chubaofs-seqwrite.jd.local",
        "type" : "seqwrite version",
        "fusecfg" : innerCfg,
        "cfs-client" : "http://storage.jd.local/dpgimage/cfsdbbak/cfs-client",
    },
    {
        "clusterName" : "cfs_taiguo",
        "clusterAddr" : "th.chubaofs-seqwrite.jd.local",
        "type" : "seqwrite version",
        "fusecfg" : innerCfg,
        "cfs-client" : "http://storage.jd.local/dpgimage/cfsdbbak/cfs-client",
    },
    {
        "clusterName" : "spark",
        "clusterAddr" : "cn.chubaofs.jd.local",
        "type" : "full version",
        "fusecfg" : openSourceCfg,
        "cfs-client" : "http://storage.jd.local/dpgimage/cfs_spark/cfs-client-hot-upgrade",
    },
    {
        "clusterName" : "elasticdb",
        "clusterAddr" : "cn.elasticdb.jd.local",
        "type" : "full version",
        "fusecfg" : openSourceCfg,
        "cfs-client" : "http://storage.jd.local/dpgimage/cfs_spark/cfs-client-randomwrite",
    },
    {
        "clusterName" : "elasticmq",
        "clusterAddr" : "cn.elasticmq.jd.local",
        "type" : "full version",
        "fusecfg" : openSourceCfg,
        "cfs-client" : "http://storage.jd.local/dpgimage/cfs_spark/cfs-client-randomwrite",
    },
    {
        "clusterName" : "id",
        "clusterAddr" : "id.chubaofs.jd.local",
        "type" : "full version",
        "fusecfg" : openSourceCfg,
        "cfs-client" : "http://storage.jd.local/dpgimage/cfs_spark/cfs-client-randomwrite",
    },
    {
        "clusterName" : "th",
        "clusterAddr" : "th.chubaofs.jd.local",
        "type" : "full version",
        "fusecfg" : openSourceCfg,
        "cfs-client" : "http://storage.jd.local/dpgimage/cfs_spark/cfs-client-randomwrite",
    },
    {
        "clusterName" : "test",
        "clusterAddr" : "test.chubaofs.jd.local",
        "type" : "full version",
        "fusecfg" : openSourceCfg,
        "cfs-client" : "http://storage.jd.local/dpgimage/cfs_spark/cfs-client-randomwrite",
    },
    {
        "clusterName" : "clickhouse",
        "clusterAddr" : "11.127.28.100:8868",
        "type" : "full version",
        "fusecfg" : openSourceCfg,
        "cfs-client" : "http://storage.jd.local/dpgimage/cfs_spark/cfs-client-randomwrite",
    },
]


def retry(times, sec):
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            i = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    i += 1
                    if i > times:
                        logging.exception(e)
                        raise e
                    time.sleep(sec)
        return inner_wrapper
    return wrapper

class MountFailed(Exception):
    pass

class CalledSubprocessError(Exception):
    pass

def subProcessCall(cmd):
    p = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE)
    p.wait()

    if p.returncode != 0:
        _, error = p.communicate()
        # print(error)
        raise CalledSubprocessError("Command failed with error message: {0}".format(str(error)))

def subProcessGetOutput(cmd):
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p.wait()
    output, error = p.communicate()
    if p.returncode != 0:
        raise CalledSubprocessError("Command failed with error message: {0}".format(str(error)))
    return output

def checkCFSMountStatus(mountPoint):
    if os.stat(mountPoint).st_ino == 1:
        try:
            logging.info("CFS client mounted %s ." % mountPoint)
            return True
        except:
            pass
    return False

def checkcfs(mountPoint,stat,volname):
    is_succeed = "cfs client mount fail,Please check the current environment"
    logging.info("check %s cfs mount status." % mountPoint)
    if stat == "first":
        if checkCFSMountStatus(mountPoint):
            return True
    else:
        for i in range(10):
            if checkCFSMountStatus(mountPoint):
                return True
            else:
                time.sleep(1)
        logging.info("%s mount failed."% mountPoint)
    return False


def downloadBinFile(downloadUrl):
    NodeExec = "cfs-client"
    wgetNodeShell = "wget {0} -O {1}".format(downloadUrl, NodeExec)
    logging.info(wgetNodeShell)
    subProcessCall(wgetNodeShell)
    os.chmod(NodeExec, 0o755)


def searchVolnameForCluster(volname):
    for cluster in clusters:
        url = "http://{0}/admin/getCluster".format(cluster["clusterAddr"])
        user_agent = 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'
        h = { 'User-Agent' : user_agent }
        req = urllib.request.Request(url, headers=h)
        response = urllib.request.urlopen(req)
        result = response.read()
        resultJson = json.loads(result)

        if cluster["type"] == "full version":
            vols = resultJson["data"]["VolStatInfo"]
            for vol in vols:
                if vol["Name"] == volname:
                    out = "clusterAddr : {0}, volname : {1}, clusterName : {2}, type: {3}".format(cluster["clusterAddr"], volname, cluster["clusterName"], cluster["type"],)
                    print(out)
        else:
            vols = resultJson["VolStat"]
            for vol in vols:
                if vol["Name"] == volname:
                    out = "clusterAddr : {0}, volname : {1}, clusterName : {2}, type: {3}".format(cluster["clusterAddr"], volname, cluster["clusterName"], cluster["type"],)
                    print(out)

def createCfg(cfg):
    logging.info("create {0}/fuse.json".format(os.getcwd()))
    with open('fuse.json',"w") as fp:
        json.dump(cfg, fp, indent=4)

def createStart(mountpoint):
    logging.info("create {0}/start.sh".format(os.getcwd()))
    startCMD = "{0}/cfs-client -c {1}/fuse.json".format(basePath, basePath)
    with open('start.sh',"w") as fp:
        fp.write(startCMD)

    os.chmod("start.sh", 0o755)

    stopCMD = "umount -l {0}".format(mountpoint)
    with open('stop.sh',"w") as fp:
        fp.write(stopCMD)
    os.chmod("stop.sh", 0o755)

def runCfsClient():
    logging.info("run start.sh")
    subProcessCall("/bin/bash start.sh")

def installFuseForDeb():
    logging.info("Install fuse for deb")
    fuse_name = "fuse_2.9.9-5_amd64.deb"
    fuse_lib_name = "libfuse2_2.9.9-5_amd64.deb"
    download_fuse_url = "http://storage.jd.local/jdolap-deploy/clickhouse/lizhuoyu/{0}".format(fuse_name)
    download_fuse_lib_url = "http://storage.jd.local/jdolap-deploy/clickhouse/lizhuoyu/{0}".format(fuse_lib_name)
    wget_fuse_cmd = "wget {0} -O {1}".format(download_fuse_url, fuse_name)
    subProcessCall(wget_fuse_cmd)
    wget_fuse_lib_cmd = "wget {0} -O {1}".format(download_fuse_lib_url, fuse_lib_name)
    subProcessCall(wget_fuse_lib_cmd)

    pwd = os.getcwd()
    subProcessCall("apt install {0}/{1} {2}/{3}".format(pwd, fuse_name, pwd, fuse_lib_name))

    subProcessGetOutput("fusermount -V")

def mountCfs(searchVol="", clusterAddr="cn.chubaofs.jd.local", volname="test", owner="liyang453",
             listen="17410", mountpoint="/mnt/chubaofs0", token="4961b2d68b7df23e15621ca584be080c",
             rdonly="false", loglevel="warn", writecache="false", logdir="/log/clickhouse/cfs"):
    rdonly = str(rdonly).lower()
    if rdonly == "true":
        rdonly = True
    else:
        rdonly = False
    writecache = str(writecache).lower()
    if writecache == "true":
        writecache = True
    else:
        writecache = False

    if searchVol :
        searchVolnameForCluster(searchVol)
        exit()

    if os.path.exists(basePath):
        shutil.rmtree(basePath)
    print(basePath)
    os.makedirs(basePath)

    original_directory = os.getcwd()

    try:
        os.chdir(basePath)

        if not os.path.exists(mountpoint):
            os.makedirs(mountpoint)
        if checkcfs(mountpoint,"first","x"):
            logging.warning("{0} is already mounted by cfs".format(mountpoint))
            return

        for cluster in copy.deepcopy(clusters):
            if cluster["clusterAddr"]  == clusterAddr:
                cfg = cluster["fusecfg"]

                if cluster["type"] == "full version":
                    cfg["masterAddr"] = clusterAddr
                    cfg["volName"] = volname
                    cfg["owner"] = owner
                    if listen == "random":
                        del cfg["profPort"]
                    else:
                        cfg["profPort"] = listen
                    cfg["mountPoint"] = mountpoint
                    cfg["rdonly"] = rdonly
                    cfg["logDir"] = logdir
                    cfg["logLevel"] = loglevel
                    cfg["writecache"] = writecache
                else:
                    cfg["master"] = clusterAddr
                    cfg["volname"] = volname
                    cfg["owner"] = owner
                    if listen == "random":
                        del cfg["profport"]
                    else:
                        cfg["profport"] = listen
                    cfg["mountpoint"] = mountpoint
                    cfg["rdonly"] = rdonly
                    cfg["logpath"] = logdir
                    cfg["loglvl"] = loglevel
                    cfg["writecache"] = writecache
                if token:
                    cfg["token"] = token
                break

        downloadBinFile(cluster["cfs-client"])

        createCfg(cfg)

        createStart(mountpoint)

        installFuseForDeb()

        runCfsClient()
    except Exception as e:
        logging.warning("can't mount chubaofs {0}".format(e))
        raise MountFailed("mount chubaofs failed")
    finally:
        os.chdir(original_directory)

    if not checkcfs(mountpoint,"s",volname):
        wgetNodeShell = "tail {0}/output.log".format(cfg["logDir"])
        try:
            subProcessCall(wgetNodeShell)
        except Exception as e:
            logging.warning("can't get {0}/output.log with: {1}".format(cfg["logDir"], e))
        raise MountFailed("mount chubaofs failed")

if __name__ == '__main__':
    try:
        mountCfs(volname='integration_test', mountpoint="/data6/lizhuoyu/ClickHouse_new/tests/integration/test_replicated_merge_tree_nfs/_instances/nfs", logdir="/data6/lizhuoyu/ClickHouse_new/tests/integration/test_replicated_merge_tree_nfs/_instances")
    except CalledSubprocessError as e:
        logging.error(e)
    except Exception as e:
        logging.error("Unknown Exception!!!")
        logging.exception(e)




