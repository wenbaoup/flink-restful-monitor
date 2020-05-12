from redis.sentinel import Sentinel

class redisSentinelHelper():
    def __init__(self,sentinel_list,service_name,db):
        self.sentinel = Sentinel(sentinel_list,socket_timeout=0.5)
        self.service_name = service_name
        self.db = db

    def get_master_redis(self):
        return self.sentinel.discover_master(self.service_name)

    def get_slave_redis(self):
        return self.sentinel.discover_slaves(self.service_name)

    def set_key(self,key,value):
        master = self.sentinel.master_for(
            service_name=self.service_name,
            socket_timeout=0.5,
            db=self.db
        )
        return master.set(key,value)


    def set_hash_key(self,hkey,key,value):
        master = self.sentinel.master_for(
            service_name=self.service_name,
            socket_timeout=0.5,
            db=self.db
        )
        return master.hset(hkey,key,value)

    def get_key(self,key):
        slave = self.sentinel.slave_for(
            service_name=self.service_name,
            socket_timeout=0.5,
            db=self.db
        )
        return slave.get(key)

    def get_hash_key(self,hkey,hey):
        slave = self.sentinel.slave_for(
            service_name=self.service_name,
            socket_timeout=0.5,
            db=self.db
        )
        return slave.hget(hkey,hey)

    def delete(self,key):
        master = self.sentinel.master_for(
            service_name=self.service_name,
            socket_timeout=0.5,
            db=self.db
        )
        return master.delete(key)


def getConnect():
    # redis info
    sentinel_list = [('197.255.20.213', 26379),('197.255.20.214', 26379),('197.255.20.215', 26379)]
    db = 13
    service_name = 'mymaster'

    # create redis link
    rsh = redisSentinelHelper(sentinel_list=sentinel_list,service_name=service_name,db=db)

    # test set key : key1 test-insert-key1
    # rsh.set_key('key1','test-insert-key1')
    #
    # rsh.set_hash_key('key2','key','test-insert-key2')

    # print(rsh.get_hash_key('key2','key'))
    # # get key1
    # print(rsh.get_key('key1'))

    return rsh

# if __name__ == '__main__':
#     getConnect()