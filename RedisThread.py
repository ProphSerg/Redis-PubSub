import json
import threading
import time
from rediscluster import RedisCluster
import redis
import logging

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
        self.stat = {
            'last-seq': -1,
            'recv': 0,
            'lost': 0,
            'delay-min': 999999,
            'delay-max': -1,
        }

    def run(self):
        while True:
            try:
                logging.info('Connect to Redis....')
                self.Redis = redis.StrictRedis( # RedisCluster(
                    host=self.RedisConnect['host'],
                    port=self.RedisConnect['port'],
                    password=self.RedisConnect['password'],
                    encoding=self.RedisConnect['encoding'],
                    decode_responses=True
                )
                self.Redis.ping()
            except Exception as ex:
                logging.info('Falied! Error: %s', ex)
                logging.info('Wait for Redis....')
                time.sleep(1)
            else:
                break
        logging.info('Redis connected.')

        if self.mode == RedisThread.PUBMODE:
            while True:
                self.data['seq'] = self.data['seq'] + 1
                self.data['time'] = time.time_ns()
                self.Redis.publish(self.queue, json.dumps(self.data))
                #if self.data['seq'] % 1000 == 0:
                    #logging.info(self.data)
                time.sleep(self.sleep)
        else:
            p = self.Redis.pubsub()
            p.subscribe(self.queue)
            for mes in p.listen():
                if mes['type'] == 'message':
                    ctm = time.time_ns()
                    msg = json.loads(mes['data'])

                    self.stat['recv'] = self.stat['recv'] + 1
                    dl = ctm - msg['time']
                    self.stat['delay-min'] = min(self.stat['delay-min'], dl)
                    self.stat['delay-max'] = max(self.stat['delay-max'], dl)

                    lost = msg['seq'] - self.stat['last-seq'] - 1
                    if self.stat['last-seq'] != -1 and lost > 0:
                        self.stat['lost'] = self.stat['lost'] + lost

                    self.stat['last-seq'] = msg['seq']

                    if self.stat['recv'] % 100 == 0:
                        logging.info(
                            'Receive: %d, lost: %d, delay(%.4f ms, %.4f ms)',
                            self.stat['recv'],
                            self.stat['lost'],
                            self.stat['delay-min'] / 1e6,
                            self.stat['delay-max'] / 1e6
                        )
