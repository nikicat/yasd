import logging
import os
from recvmmsg import recv_mmsg

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

def multiply(stream, factor=2):
    for msg in stream:
        for i in range(factor):
            yield msg

def split(stream, factor=2):
    return [stream]*factor

def consume(streams, running=True):
    while running:
        for stream in streams:
            next(stream)

def consume_threaded(streams, workers=1):
    import threading
    import multiprocessing
    threads = []
    running = multiprocessing.Value('d', 1)
    for worker in range(workers):
        threads.append(threading.Thread(target=consume, name='worker #{}'.format(worker), args=(streams,running)))
    for thread in threads:
        thread.start()
    def stop():
        running = 0
        for thread in threads:
            thread.join()
    return stop

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
    for msgs in stream:
        conn.bulk_index(index.format(**msgs[0]), type, msgs, consistency="one")
        yield msgs

def produce(running=True):
    while running:
        yield None

def produce_forked(processes):
    import os
    import multiprocessing
    import random
    pids = []
    running = multiprocessing.Value('d', 1)
    for i in range(processes):
        pid = os.fork()
        if pid == 0:
            random.seed()
            yield from produce(running)
        else:
            pids.append(pid)
    for pid in pids:
        os.waitpid(pid, 0)

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

def recv_queue(stream, queue):
    for _ in stream:
        yield queue.get()

def main():
    logging.basicConfig(level=logging.INFO)
    #logging.getLogger('pyelasticsearch').setLevel(logging.DEBUG)

    s = make_udp_sock(port=5514, bufsize=1024*1024*100)
    q = make_queue(size=1024*1024)

    x = produce_forked(processes=4)
    x = recv_mmsg(x, s, vlen=100000)
    x = send_logging(x)
    x = parse_syslog(x)
    x = parse_syslog_tag(x)
    x = filter(lambda msg: msg['programname'] == b'trapper', x)
    x = parse_syslog_pri(x)
    x = decode(x, 'timestamp')
    x = parse_syslog_timestamp(x)
    x = decode(x, 'msg')
    x = gen_uuid(x)
    x = rename(x, {'timestamp': '@timestamp', 'uuid': 'id'})
    x = send_logging(x)
    x = send_queue(x, q)
    stop = consume_threaded([x])

    y = produce()
    y = recv_queue(y, q)
    y = send_logging(y)
    y = group(y, count=10000, timefield='@timestamp')
    y = send_es_bulk(y, index='debug-{@timestamp:%Y}-{@timestamp:%m}-{@timestamp:%d}', servers=['http://elastic{}:9200/'.format(i) for i in range(4)], timeout=600)
    
    try:
        consume([y])
    finally:
        stop()

if __name__ == '__main__':
    main()
