import json
import urllib.request as ur
import json
from zabbix import RedisUtil

#获取活跃的master
def getActiveRN(master1,master2):
    response=ur.urlopen("http://"+master1+"/ws/v1/cluster/info")
    jsonstring=response.read()
    # print(jsonstring)
    j1=json.loads(jsonstring)
    # print(master1 +" resourcemanager state is :"+j1['clusterInfo']['haState'])

    response=ur.urlopen("http://"+master2+"/ws/v1/cluster/info")
    jsonstring=response.read()
    # print(jsonstring)
    j2=json.loads(jsonstring)
    # print(master2 +" resourcemanager state is :"+j2['clusterInfo']['haState'])

    if j1['clusterInfo']['haState']=='ACTIVE':
        # print("active master is "+master1)
        activemaster=master1
    elif j2['clusterInfo']['haState']=='ACTIVE':
        # print("active master is "+master2)
        activemaster=master2
    else:
        raise Exception("on active resourcemanger in %s,%s "%(master1,master2))
    return activemaster

def getFlinkUrl(AppId):
    master1="master4.cloudera.yijiupidev.com:8088"
    master2="master5.cloudera.yijiupidev.com:8088"
    activemaster=getActiveRN(master1,master2)
    print(activemaster)
    response=ur.urlopen("http://"+activemaster+"/ws/v1/cluster/apps/"+AppId)
    jsonstring=response.read()
    # print(jsonstring)
    jsonDic=json.loads(jsonstring)
    flink_url=jsonDic['app']['trackingUrl']
    yarn_name=jsonDic['app']['name']
    # print(yarn_name)
    return flink_url,yarn_name



