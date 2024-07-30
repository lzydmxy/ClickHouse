import os
import logging
import subprocess
import commands
import logging
import sys
import urllib
import urllib2
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

def yuminstall(package):
  cmd = "yum -y -q install {0} ".format(package)
  info = commands.getoutput(cmd)
  logging.info(cmd)
  return

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
    req = urllib2.Request(url,headers=h)
    response = urllib2.urlopen(req)
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
  logging.info("create fuse.json")
  with open('fuse.json',"w") as fp:
    json.dump(cfg, fp, indent=4)

def createStart(mountpoint):
  logging.info("create start.sh")
  startCMD = "{0}/cfs-client -c {1}/fuse.json".format(basePath, basePath)
  with open('start.sh',"w") as fp:
    fp.write(startCMD)

  os.chmod("start.sh", 0o755)

  stopCMD = "umount {0}".format(mountpoint)
  with open('stop.sh',"w") as fp:
    fp.write(stopCMD)
  os.chmod("stop.sh", 0o755)
  
def runCfsClient():
  logging.info("run start.sh")
  subProcessCall("/bin/bash start.sh")

def getVolumeOwner(volName):
  vol_info_url = 'http://api.jfs.jd.local/cfs/volGetByPrefix?cluster=spark&source=ClickHouse&token=4961b2d68b7df23e15621ca584be080c&prefix='
  vol_info_url += volName

  try:
    res = urllib2.urlopen(vol_info_url)
    res_data = res.read()
    json_obj = json.loads(res_data)
    if json_obj["code"] != 0:
      print("get vol info failed:", json_obj["message"])
      return None
    else:
      if len(json_obj["data"]) != 1:
        print("get vol info failed, result not only, ", json_obj["data"])
        return None
      else:
        uOwner = json_obj["data"][0]["owner"]
        owner = uOwner.encode('utf-8')
        print("get vol info success, owner is:", owner)
        return owner
  except Exception as e:
    print("get vol info failed", e)
    return None

@retry(6, 5)
def mountCfs(args):
  clusterAddr = args.clusterAddr
  volname = args.volname
  owner = getVolumeOwner(volname)
  if owner is None:
    owner = "liyang453"
  listen = args.listen
  mountpoint = args.mountpoint
  searchVol = args.search
  token = args.token
  loglevel = args.loglevel
  rdonly = args.rdonly
  rdonly = str(rdonly).lower()
  if rdonly == "true":
    rdonly = True
  else:
    rdonly = False
  writecache = args.writecache
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
        cfg["logDir"] = cfg["logDir"] % volname
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
        cfg["logpath"] = cfg["logpath"] % volname
        cfg["loglvl"] = loglevel
        cfg["writecache"] = writecache
      if token:
        cfg["token"] = token
      break

  downloadBinFile(cluster["cfs-client"])

  createCfg(cfg)
  
  createStart(mountpoint)

  runCfsClient()
  if not checkcfs(mountpoint,"s",volname):
    wgetNodeShell = "tail {0}/output.log".format(cfg["logDir"])
    try:
      subProcessCall(wgetNodeShell)
    except Exception as e:
      logging.warning("can't get {0}/output.log with: {1}".format(cfg["logDir"], e))
    raise MountFailed("mount chubaofs failed")

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='manual to this script')
  parser.add_argument('--search',type=str,default="", help='Query the cluster of volumes')
  parser.add_argument('--clusterAddr',type=str,default="cn.chubaofs.jd.local", help='clusterAddr')
  parser.add_argument('--volname',type=str,default="test", help='volname')
  parser.add_argument('--owner',type=str,default="liyang453", help='owner')
  parser.add_argument('--listen',type=str,default="17410", help='listen')
  parser.add_argument('--mountpoint',type=str,default="/mnt/chubaofs0", help='mountpoint')
  parser.add_argument('--token',type=str,default="4961b2d68b7df23e15621ca584be080c", help='token')
  parser.add_argument('--rdonly',type=str, default="false", help='readonly')
  parser.add_argument('--loglevel',type=str,default="warn", help='loglevel')
  parser.add_argument('--writecache',type=str,default="false", help='writecache')
  args = parser.parse_args()
  

  try:
    mountCfs(args)
  except AttributeError:
    parser.print_usage()
  except CalledSubprocessError as e:
    logging.error(e)
  except Exception as e:
    logging.error("Unknown Exception!!!")
    logging.error(e)
  



