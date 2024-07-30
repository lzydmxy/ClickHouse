#!/usr/bin/python3.8

import logging
import sys
import requests
import json
import datetime as dt
import re
import sys
from datetime import date
from datetime import timedelta

# CNP域名
CNP_DOMAIN = "http://cnp.jd.com"
# CMDBAppId
CNP_APP_KEY = "olap"

CNP_TOKEN = ""
CNP_APP_MARK = ""
OLAP_IMG = ""

if len(sys.argv) != 4:
    print("invalid params, example: ./update_test_cluster_img.py cnp_token cnp_app_mark image_name")
    exit(1)
else:
   CNP_TOKEN=sys.argv[1]
   CNP_APP_MARK=sys.argv[2]
   OLAP_IMG=sys.argv[3]
   print(CNP_TOKEN, CNP_APP_MARK, OLAP_IMG)


url = CNP_DOMAIN+"/api/v1/application/image?mark="+CNP_APP_MARK
headers = {"appKey": CNP_APP_KEY, "token": CNP_TOKEN, "Content-Type": "application/json"}
req_body = {"modifier": "liyang453", "components": [{"name": "clickhouse", "image": OLAP_IMG}], "operation": {"strategy": {"name": "rolling", "concurrency": 10}}}
req_body_json = json.dumps(req_body)

print(req_body_json)
res = requests.put(url=url, headers=headers, data=req_body_json)

if res.status_code != 200:
    print("数据获取错误:", res.text)
    exit(1)

print(res.text)
res_json = json.loads(res.text)
if res_json["code"] != 0:
    print("执行失败:", res_json["message"])
    exit(1)
else:
    print("执行成功")
