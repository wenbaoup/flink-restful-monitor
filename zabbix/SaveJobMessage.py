from zabbix import RedisUtil
from zabbix import YarnUtil
import urllib.request as ur
import json

def run(yarnId):
    flink_url,yarn_name=YarnUtil.getFlinkUrl(yarnId)
    response=ur.urlopen(flink_url+"/jobs/overview")
    responseString=response.read()
    response_json=json.loads(responseString)
    jobs_message=response_json['jobs']
    connect=RedisUtil.getConnect()
    # connect.delete(yarn_name)
    for job_message in jobs_message:
        print(yarn_name)
        print(job_message['name'])
        print(job_message['state'])
        connect.set_hash_key(yarn_name,job_message['name'],job_message['state'])
