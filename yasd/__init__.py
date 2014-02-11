import logging

logger = logging.getLogger('yasd')

try:
    from .recvmmsg import recv_mmsg as recv
except:
    logging.warning('recvmmsg unavailable, using recv')
    def recv(stream, sock, bufsize=9000, **kwargs):
        for _ in stream:
            yield sock.recv(bufsize)

from . import stats

def make_unix_sock(path, bufsize=65536, unlink=False):
    import socket
    import os
    import os.path
    if unlink and os.path.exists(path):
        os.remove(path)
    s = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, bufsize)
    s.bind(path)
    os.chmod(path, 0o777)
    return s

def make_udp_sock(bindaddr, bufsize=65536):
    import socket
    s = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
    # dont forget to set net.core.rmem_max to equal or greater value
    # linux supports SO_RCVBUFFORCE, but it requires CAP_NET_ADMIN privilege
    s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, bufsize)
    s.bind(bindaddr)
    return s

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

def send_logging(stream, level=logging.DEBUG, logger=logging.getLogger('yasd.dump')):
    for msg in stream:
        logger.log(level, msg)
        yield msg

def parse_syslog(stream):
    import re
    import collections
    syslogre = re.compile(b'^<(?P<pri>[0-9]{1,3})>(?P<timestamp>[A-Z][a-z]{2} [ 0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}) (?P<tag>[^:]+): (?P<msg>.*)$', re.MULTILINE|re.DOTALL)
    SyslogEntry = collections.namedtuple('SyslogEntry', ['pri', 'timestamp', 'tag', 'msg'])
    for msg in stream:
        try:
            match = syslogre.match(msg)
            msg = match.groupdict()
        except:
            logger.warning('failed to parse syslog string, passing whole msg "{}"'.format(msg), exc_info=True)
            msg = {'msg': msg}
        yield msg

def parse_syslog_pri(stream):
    severities = ['emergency', 'alert', 'critical', 'error', 'warning', 'notice', 'info', 'debug']
    facilities = ['kernel', 'user', 'mail', 'daemon', 'auth', 'syslog', 'printer', 'nntp', 'uucp', 'clock', 'audit', 'ftp', 'ntp', 'audit', 'alert', 'cron', 'local0', 'local1', 'local2', 'local3', 'local4', 'local5', 'local6', 'local7']
    for msg in stream:
        try:
            pri = msg['pri'] = int(msg['pri'])
            msg['severity'] = severity = pri % 8
            msg['facility'] = facility = pri // 8
            msg['severity-text'] = severities[severity]
            msg['facility-text'] = facilities[facility]
        except:
            logger.warning('failed to parse syslog pri', exc_info=True)
        yield msg

def parse_syslog_tag(stream):
    import re
    tagre = re.compile(b'(?P<host>[^ ]+) (?P<programname>[^\[]+)(?:\[(?P<pid>[0-9]+)\])?') 
    for msg in stream:
        try:
            match = tagre.match(msg['tag'])
            msg.update(match.groupdict())
            if msg['pid'] is not None:
                msg['pid'] = int(msg['pid'])
        except:
            logger.warning('failed to parse syslog tag', exc_info=True)
        yield msg

def parse_syslog_timestamp(stream, timezone=None):
    import datetime
    import functools
    import dateutil.tz
    tzlocal = dateutil.tz.gettz(timezone)
    tzutc = dateutil.tz.tzutc()
    @functools.lru_cache(maxsize=10)
    def parse(timestamp):
        return datetime.datetime.strptime(timestamp, '%b %d %H:%M:%S').replace(year=datetime.datetime.now().year, tzinfo=tzlocal).astimezone(tzutc)

    for msg in stream:
        try:
            msg['timestamp'] = parse(msg['timestamp'])
        except:
            logger.warning('failed to parse syslog timestamp, using current time', exc_info=True)
            msg['timestamp'] = datetime.datetime.utcnow()
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
            try:
                msg[v] = msg[k]
                del msg[k]
            except:
                logger.warning('failed to rename "{k}" to "{v}"'.format(k=k, v=v), exc_info=True)
        yield msg

def send_es(stream, index='log-{@timestamp:%Y}-{@timestamp:%m}-{@timestamp:%d}', type='events', servers='http://localhost:9200/', timeout=10):
    import pyelasticsearch
    conn = pyelasticsearch.ElasticSearch(servers, timeout=timeout, max_retries=9999999999)
    for msg in stream:
        conn.index(index.format(**msg), type, msg)
        yield msg

def group(stream, count=100000, timeout=10, timefield='timestamp'):
    import time
    msgs = []
    start = time.time()
    for msg in stream:
        msgs.append(msg)
        if len(msgs) > 0 and (len(msgs) == count or time.time() - start > timeout or msg[timefield].date() != msgs[-1][timefield].date()):
            yield msgs
            msgs = []
            start = time.time()

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

def count_messages(stream, counter, delta=lambda x: 1):
    for msg in stream:
        counter += delta(msg)
        yield msg

def make_running():
    import multiprocessing
    return multiprocessing.Value('d', 1)

def decode(stream, field):
    for msg in stream:
        try:
            msg[field] = msg[field].decode(errors='ignore')
        except:
            logger.warning('failed to decode field "{}"'.format(field), exc_info=True)
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

def syslog_to_elastic(bindaddr, elasticurls, elasticindex, receivers=1, senders=1, bufsize=1024*1024*100, bulksize=1, queuesize=1024*1024*10, timezone=None):
    logging.TRACE = 5

    if isinstance(bindaddr, str):
        s = make_unix_sock(bindaddr, bufsize=bufsize, unlink=True)
    else:
        s = make_udp_sock(bindaddr, bufsize=bufsize)

    q = make_queue(size=queuesize)
    r = make_running()
    stats.Gauge('queue_size', q.qsize)
    stats.create_system_counters()

    # dont forget to enable packet steering, like
    # echo f > /sys/class/net/eth0/queues/rx-0/rps_cpus
    x = produce(running=r)
    x = fork(x, processes=receivers)
    x = recv(x, s, vlen=100000)
    x = count_messages(x, stats.Counter('messages_received'))
    x = send_logging(x, level=logging.TRACE)
    x = parse_syslog(x)
    x = parse_syslog_tag(x)
    x = parse_syslog_pri(x)
    x = decode(x, 'timestamp')
    x = parse_syslog_timestamp(x, timezone=timezone)
    x = decode(x, 'msg')
    x = gen_uuid(x)
    x = rename(x, {'timestamp': '@timestamp', 'uuid': 'id'})
    x = send_logging(x, level=logging.TRACE)
    x = send_queue(x, q)
    x = count_messages(x, stats.Counter('messages_enqueued'))
    consume_threaded(x)

    y = produce(running=r)
    y = fork(y, processes=senders)
    y = recv_queue(y, q)
    y = count_messages(y, stats.Counter('messages_dequeued'))
    y = send_logging(y, level=logging.TRACE)
    y = group(y, count=bulksize, timefield='@timestamp')
    y = send_es_bulk(y, index=elasticindex, servers=elasticurls, timeout=600)
    y = count_messages(y, stats.Counter('messages_sent'), delta=lambda x: len(x))
    consume_threaded(y)

    def handler(signal, frame):
        logging.debug('catched signal, stopping...')
        r.value = 0
    import signal
    signal.signal(signal.SIGINT, handler)
