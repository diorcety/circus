import errno
import zmq
from zmq.eventloop.zmqstream import ZMQStream
import tornado

from circus.util import DEFAULT_ENDPOINT_SUB, get_connection
from circus.py3compat import b

class AsyncCircusConsumer(object):
    def __init__(self, topics, callback, context=None, endpoint=DEFAULT_ENDPOINT_SUB,
                 ssh_server=None):
        self.topics = topics
        self.keep_context = context is not None
        self._init_context(context)
        self.endpoint = endpoint
        self.pubsub_socket = self.context.socket(zmq.SUB)
        get_connection(self.pubsub_socket, self.endpoint, ssh_server)
        for topic in self.topics:
            self.pubsub_socket.setsockopt(zmq.SUBSCRIBE, b(topic))
        self.stream = ZMQStream(self.pubsub_socket, tornado.ioloop.IOLoop.instance())
        def inner_callback(a):
            callback(a[0], a[1])
        self.stream.on_recv(inner_callback)

    def _init_context(self, context):
        self.context = context or zmq.Context()

    def stop(self):
        self.stream.stop_on_recv()
        # only supported by libzmq >= 3
        if hasattr(self.pubsub_socket, 'disconnect'):
            self.pubsub_socket.disconnect(self.endpoint)
        self.stream.close()

        if self.keep_context:
            return
        try:
            self.context.destroy(0)
        except zmq.ZMQError as e:
            if e.errno == errno.EINTR:
                pass
            else:
                raise

class CircusConsumer(object):
    def __init__(self, topics, context=None, endpoint=DEFAULT_ENDPOINT_SUB,
                 ssh_server=None, timeout=1.):
        self.topics = topics
        self.keep_context = context is not None
        self._init_context(context)
        self.endpoint = endpoint
        self.pubsub_socket = self.context.socket(zmq.SUB)
        get_connection(self.pubsub_socket, self.endpoint, ssh_server)
        for topic in self.topics:
            self.pubsub_socket.setsockopt(zmq.SUBSCRIBE, b(topic))
        self._init_poller()
        self.timeout = timeout

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ On context manager exit, destroy the zmq context """
        self.stop()

    def __iter__(self):
        return self.iter_messages()

    def _init_context(self, context):
        self.context = context or zmq.Context()

    def _init_poller(self):
        self.poller = zmq.Poller()
        self.poller.register(self.pubsub_socket, zmq.POLLIN)

    def iter_messages(self):
        """ Yields tuples of (topic, message) """
        with self:
            while True:
                try:
                    events = dict(self.poller.poll(self.timeout * 1000))
                except zmq.ZMQError as e:
                    if e.errno == errno.EINTR:
                        continue
                    raise

                if len(events) == 0:
                    continue

                topic, message = self.pubsub_socket.recv_multipart()
                yield topic, message

    def stop(self):
        if self.keep_context:
            return
        try:
            self.context.destroy(0)
        except zmq.ZMQError as e:
            if e.errno == errno.EINTR:
                pass
            else:
                raise
