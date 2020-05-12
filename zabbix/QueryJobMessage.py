from zabbix import RedisUtil

def run(yarnName,jobName):
    connect=RedisUtil.getConnect()
    print(connect.get_hash_key(yarnName,jobName))

