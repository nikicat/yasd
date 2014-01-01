import sys
import os
import socket
import re
import collections

def recv_unix(sockname, bufsize=65536, unlink=False):
    if unlink:
        os.remove(sockname)
    s = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    s.bind(sockname)
    while True:
        yield s.recv(bufsize)

def send_stdout(stream, separator=b'\n'):
    for msg in stream:
        sys.stdout.buffer.write(msg)
        sys.stdout.buffer.write(separator)
        sys.stdout.buffer.flush()
        yield msg

def send_print(stream):
    for msg in stream:
        print(msg)
        yield msg

def parse_syslog(stream):
    syslogre = re.compile(b'^<([0-9]{2})>([A-Z][a-z]{2} [ 0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}) ([^:]+): (.*)$')
    SyslogEntry = collections.namedtuple('SyslogEntry', ['pri', 'timestamp', 'tag', 'msg'])
    for msg in stream:
        match = syslogre.match(msg)
        if match is None:
            raise Exception('failed to parse syslog string')
        entry = SyslogEntry(*match.groups())
        yield entry

def multiply(stream, factor=2):
    for msg in stream:
        for i in range(factor):
            yield msg

def split(stream, factor=2):
    return [stream]*factor

def consume(streams):
    while True:
        for stream in streams:
            next(stream)

def main():
    x = recv_unix('/run/systemd/journal/syslog', unlink=True)
    x = multiply(x)
    x,y = split(x)

    y = send_print(y)

    x = parse_syslog(x)
    x = send_print(x)

    consume([y,x])

if __name__ == '__main__':
    main()
