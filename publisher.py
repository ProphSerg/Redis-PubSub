import argparse
import time
from RedisThread import RedisThread

def createParser():
    parser = argparse.ArgumentParser()
    #parser.add_argument('name', nargs='?')

    modeGroupM = parser.add_mutually_exclusive_group(required=True)
    modeGroupM.add_argument('-P', action='store_true', default=False, help='Publisher mode')
    modeGroupM.add_argument('-S', action='store_true', default=False, help='Subcriber mode')

    redisGroup = parser.add_argument_group('Redis Server setting')
    redisGroup.add_argument('--host', default='127.0.0.1', type=str, help='Redis host (default: %(default)s)')
    redisGroup.add_argument('--port', default=6379, type=int, help='Redis port (default: %(default)s)')
    redisGroup.add_argument('--password', default=None, type=str, help='Redis password (default: %(default)s)')
    redisGroup.add_argument('--enc', default='windows-1251', type=str, help='Redis string encoding (default: %(default)s)')

    mesGroup = parser.add_argument_group('Message setting')
    mesGroup.add_argument('--queue-prefix', default='LOAD-', type=str, help='Messages queue prefix (default: %(default)s)')
    mesGroup.add_argument('--queue-name', default='test', type=str, help='Messages queue name (default: %(default)s)')
    mesGroup.add_argument('--msg-size', default=200000, type=int, help='Messages size (default: %(default)s)')

    pubGroup = parser.add_argument_group('Pub/Sub setting')
    mesGroup.add_argument('--pub-mps', default=10, type=int, help='Send Messages per Second (mps) (default: %(default)s)')
    mesGroup.add_argument('--sub-count', default=5, type=int, help='Count subscriber threads (default: %(default)s)')

    return parser


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    args = vars(createParser().parse_args())
    print(args)

    threads = []
    for i in range(1 if args['P'] == True else args['sub_count']):
        t = RedisThread(args)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    '''
    format = "%(asctime)s: <%(%(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")
    thrFull = RedisThread(pubType=RedisThread.FULLDATA)
    thrFull.start()
    thrFull.join()

    thrInc = RedisThread(pubType=RedisThread.INCDATA)
    thrInc.start()
    thrInc.join()
'''
