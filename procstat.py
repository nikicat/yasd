import os

# clock ticks per second... jiffies (HZ)
JIFFIES_PER_SEC = os.sysconf('SC_CLK_TCK')

# KiB
PAGE_SIZE = os.sysconf('SC_PAGE_SIZE') / 1024

def get_self():
    stat = open('/proc/self/stat', 'rt').readline().split()
    return {
        'utime': int(stat[13]) / JIFFIES_PER_SEC,
        'stime': int(stat[14]) / JIFFIES_PER_SEC,
        'cutime': int(stat[15]) / JIFFIES_PER_SEC,
        'cstime': int(stat[16]) / JIFFIES_PER_SEC,
        'vsize': int(stat[22]) / JIFFIES_PER_SEC,
        'rss': int(stat[23]) * PAGE_SIZE
    }

def get_cpu():
    stat = open('/proc/stat', 'rt').readline().split()
    return {
        'user': int(stat[1]),
        'nice': int(stat[2]),
        'sys': int(stat[3]),
        'idle': int(stat[4]),
        'iowait': int(stat[5]),
        'irq': int(stat[6]),
        'sirq': int(stat[7])
    }
