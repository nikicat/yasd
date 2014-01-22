import multiprocessing
from . import procstat

_stats = {}

class Gauge:
    def __init__(self, name, func):
        self._func = func
        _stats[name] = self

    @property
    def value(self):
        return self._func()

class Counter:
    def __init__(self, name):
        self._value = multiprocessing.Value('d', 0)
        _stats[name] = self

    @property
    def value(self):
        return self._value.value

    @value.setter
    def value(self, value):
        self._value.value = value

    def __iadd__(self, other):
        with self._value.get_lock():
            self._value.value += other
        return self

    def __isub__(self, other):
        with self._value.get_lock():
            self._value.value -= other
        return self

def startsending(server, port=2004, period=5, group='yasd'):
    import socket
    import graphitesend
    client = graphitesend.GraphiteClient(prefix='servers', graphite_server=server, graphite_port=port, group=group)
    def send():
        import time
        while True:
            for name, stat in _stats.items():
                value = stat.value
                if isinstance(value, dict):
                    client.send_dict({'{}.{}'.format(name, key): value for key, value in value.items()})
                else:
                    client.send(name, value)
            time.sleep(period)
    import threading
    threading.Thread(target=send, daemon=True).start()

def create_system_counters():
    import gc
    for i in [0,1,2]:
        Gauge('gc_count_{}'.format(i), lambda: gc.get_count()[int(i)])
    Gauge('gc_count_objects', lambda: len(gc.get_objects()))
    Gauge('self', procstat.get_self)
    Gauge('cpu', procstat.get_cpu)
