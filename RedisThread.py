import json
import threading
import time
from rediscluster import RedisCluster
import redis

class RedisThread(threading.Thread):
    PUBMODE = 0
    SUBMODE = 1

    def __init__(self, args):
        threading.Thread.__init__(self)
        self.mode = RedisThread.PUBMODE if args['P'] == True else RedisThread.SUBMODE
        if self.mode == RedisThread.PUBMODE:
            self.sleep = 1.0 / float(args['pub_mps'])
            self.data = {
                'seq': 0,
                'time': 0,
                'data': 'A' * args['msg_size'],
            }
        self.RedisConnect = {
            'host': args['host'],
            'port': args['port'],
            'password': args['password'],
            'encoding': args['enc'],
        }
        self.queue = args['queue_prefix'] + args['queue_name']

    def run(self):
        while True:
            try:
                print('Connect to Redis....', end='', flush=True)
                self.Redis = redis.StrictRedis( # RedisCluster(
                    host=self.RedisConnect['host'],
                    port=self.RedisConnect['port'],
                    password=self.RedisConnect['password'],
                    encoding=self.RedisConnect['encoding'],
                    decode_responses=True
                )
                self.Redis.ping()
            except Exception as ex:
                print('Falied! Error: ', ex)
                print('Wait for Redis....')
                time.sleep(1)
            else:
                break
        print('connected.')

        if self.mode == RedisThread.PUBMODE:
            while True:
                self.data['seq'] = self.data['seq'] + 1
                self.data['time'] = time.time_ns()
                self.Redis.publish(self.queue, json.dumps(self.data))
                time.sleep(self.sleep)
        else:
            p = self.Redis.pubsub()
            p.subscribe(self.queue)
            for mes in p.listen():
                print(self.name, mes)