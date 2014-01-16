import logging
import os
from recvmmsg import recv_mmsg
import stats

def make_unix_sock(sockname, bufsize=65536, unlink=False):
    import socket
    import os
    if unlink:
        os.remove(sockname)
    s = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, bufsize)
    s.bind(sockname)
    return s

def make_udp_sock(port=514, bufsize=65536):
    import socket
    s = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
    # dont forget to set net.core.rmem_max to equal or greater value
    # linux supports SO_RCVBUFFORCE, but it requires CAP_NET_ADMIN privilege
    s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, bufsize)
    s.bind(('::', port))
    return s

def recv(stream, sock, bufsize=9000):
    for _ in stream:
        yield sock.recv(bufsize)

def send_stdout(stream, separator=b'\n'):
    import sys
    for msg in stream:
        sys.stdout.buffer.write(msg)
        sys.stdout.buffer.write(separator)
        sys.stdout.buffer.flush()
        yield msg

def send_print(stream):
    for msg in stream:
        print(msg)
        yield msg

def send_logging(stream, level=logging.DEBUG):
    for msg in stream:
        logging.log(level, msg)
        yield msg

def parse_syslog(stream):
    import re
    import collections
    syslogre = re.compile(b'^<(?P<pri>[0-9]{1,3})>(?P<timestamp>[A-Z][a-z]{2} [ 0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}) (?P<tag>[^:]+): (?P<msg>.*)$')
    SyslogEntry = collections.namedtuple('SyslogEntry', ['pri', 'timestamp', 'tag', 'msg'])
    for msg in stream:
        match = syslogre.match(msg)
        if match is None:
            raise Exception('failed to parse syslog string')
        entry = match.groupdict()
        yield entry

def parse_syslog_pri(stream):
    severities = ['emergency', 'alert', 'critical', 'error', 'warning', 'notice', 'info', 'debug']
    facilities = ['kernel', 'user', 'mail', 'daemon', 'auth', 'syslog', 'printer', 'nntp', 'uucp', 'clock', 'audit', 'ftp', 'ntp', 'audit', 'alert', 'cron', 'local0', 'local1', 'local2', 'local3', 'local4', 'local5', 'local6', 'local7']
    for msg in stream:
        pri = msg['pri'] = int(msg['pri'])
        msg['severity'] = severity = pri % 8
        msg['facility'] = facility = pri // 8
        msg['severity-text'] = severities[severity]
        msg['facility-text'] = facilities[facility]
        yield msg

def parse_syslog_tag(stream):
    import re
    tagre = re.compile(b'(?P<host>[^ ]+) (?P<programname>[^\[]+)(?:\[(?P<pid>[0-9]+)\])?') 
    for msg in stream:
        match = tagre.match(msg['tag'])
        msg.update(match.groupdict())
        msg['pid'] = int(msg['pid'])
        yield msg

def parse_syslog_timestamp(stream):
    import datetime
    import functools
    @functools.lru_cache(maxsize=10)
    def parse(timestamp):
        return datetime.datetime.strptime(timestamp, '%b %d %H:%M:%S').replace(year=datetime.datetime.now().year)

    for msg in stream:
        msg['timestamp'] = parse(msg['timestamp'])
        yield msg

def consume(stream):
    for _ in stream:
        pass

def consume_threaded(stream, workers=1):
    import threading
    threads = []
    for worker in range(workers):
        threads.append(threading.Thread(target=consume, name='worker #{}'.format(worker), args=(stream,)))
    for thread in threads:
        thread.start()
    def join():
        for thread in threads:
            thread.join()
    return join

def rename(stream, renames):
    for msg in stream:
        for k,v in renames.items():
            msg[v] = msg[k]
            del msg[k]
        yield msg

def send_es(stream, index='log-{@timestamp:%Y}-{@timestamp:%m}-{@timestamp:%d}', type='events', servers='http://localhost:9200/', timeout=10):
    import pyelasticsearch
    conn = pyelasticsearch.ElasticSearch(servers, timeout=timeout)
    for msg in stream:
        conn.index(index.format(**msg), type, msg)
        yield msg

def group(stream, count=100000, timeout=10, timefield='timestamp'):
    import time
    msgs = []
    start = time.time()
    for msg in stream:
        if len(msgs) > 0 and (len(msgs) == count or time.time() - start > timeout or msg[timefield].date() != msgs[-1][timefield].date()):
            yield msgs
            msgs = []
            start = time.time()
        msgs.append(msg)

def gen_uuid(stream):
    import uuid
    for msg in stream:
        msg['uuid'] = str(uuid.uuid4())
        yield msg

def send_es_bulk(stream, index='log-{@timestamp:%Y}-{@timestamp:%m}-{@timestamp:%d}', type='events', servers='http://localhost:9200/', timeout=30):
    import pyelasticsearch
    conn = pyelasticsearch.ElasticSearch(servers, timeout=timeout, max_retries=4)
    sent = stats.Counter('sent_to_es')
    for msgs in stream:
        conn.bulk_index(index.format(**msgs[0]), type, msgs, consistency="one")
        sent += len(msgs)
        yield msgs

def produce(running):
    while running.value:
        yield None

def fork(stream, processes):
    import os
    import random
    pids = []
    for i in range(processes):
        pid = os.fork()
        if pid == 0:
            random.seed()
            fork.workernum = i
            yield from stream
            return
        else:
            pids.append(pid)
    for pid in pids:
        os.waitpid(pid, 0)

def count_messages(stream, name):
    count = stats.Counter(name)
    for msg in stream:
        count += 1
        yield msg

def send_stats(stream, graphite_server, graphite_port, prefix):
    stats.startsending(server=graphite_server, port=graphite_port, prefix='{}{}'.format(prefix, fork.workernum))
    yield from stream

def make_running():
    import multiprocessing
    return multiprocessing.Value('d', 1)

def decode(stream, field):
    for msg in stream:
        msg[field] = msg[field].decode(errors='ignore')
        yield msg

def make_queue(size=1024):
    import multiprocessing
    return multiprocessing.Queue(maxsize=size)

def send_queue(stream, queue):
    for msg in stream:
        queue.put(msg)
        yield msg
    # to avoid locking in recv_queue 
    queue.put(None)

def recv_queue(stream, queue):
    for _ in stream:
        msg = queue.get()
        if msg:
            yield msg

def main():
    logging.basicConfig(level=logging.INFO)
    #logging.getLogger('pyelasticsearch').setLevel(logging.DEBUG)
    logging.getLogger('graphitesend').setLevel(logging.DEBUG)
    logging.TRACE = 5

    s = make_udp_sock(port=5514, bufsize=1024*1024*100)
    q = make_queue(size=1024*1024*3)
    r = make_running()
    stats.Gauge('queue_size', q.qsize)
    stats.create_system_counters()

    # dont forget to enable packet steering, like
    # echo f > /sys/class/net/eth0/queues/rx-0/rps_cpus
    x = produce(running=r)
    x = send_stats(x, 'graphite-shard1', 2024, 'receiver')
    x = fork(x, processes=4)
    #x = produce(running=r)
    x = recv_mmsg(x, s, vlen=100000)
    x = count_messages(x, 'messages_received')
    x = send_logging(x, level=logging.TRACE)
    x = parse_syslog(x)
    x = parse_syslog_tag(x)
    x = filter(lambda msg: msg['programname'] == b'trapper', x)
    x = parse_syslog_pri(x)
    x = decode(x, 'timestamp')
    x = parse_syslog_timestamp(x)
    x = decode(x, 'msg')
    x = gen_uuid(x)
    x = rename(x, {'timestamp': '@timestamp', 'uuid': 'id'})
    x = send_logging(x, level=logging.TRACE)
    x = send_queue(x, q)
    x = count_messages(x, 'messages_queued')
    consume_threaded(x)

    y = produce(running=r)
    y = send_stats(y, 'graphite-shard1', 2024, 'sender')
    y = fork(y, processes=4)
    y = recv_queue(y, q)
    y = send_logging(y, level=logging.TRACE)
    y = count_messages(y, 'messages_queued_to_es')
    y = group(y, count=10000, timefield='@timestamp')
    y = send_es_bulk(y, index='debug-{@timestamp:%Y}-{@timestamp:%m}-{@timestamp:%d}', servers=['http://elastic{}:9200/'.format(i) for i in range(4)], timeout=600)
    consume_threaded(y)

    def handler(signal, frame):
        logging.debug('catched signal, stopping...')
        r.value = 0
    import signal
    signal.signal(signal.SIGINT, handler)
    logging.info('done initialization, main thread exiting')

if __name__ == '__main__':
    main()
