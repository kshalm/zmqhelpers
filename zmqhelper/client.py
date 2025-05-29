# client.py
import zmq

class Client:
    """
    A thin REQ/REP client with built-in heartbeat and recv/send timeouts.
    """
    def __init__(self, ip, port):
        self.ip   = f"tcp://{ip}"
        self.port = str(port)
        self.simple_init()

    def simple_init(self):
        self.context = zmq.Context()
        self.socket  = self.context.socket(zmq.REQ)

        # heartbeat as before…
        self.socket.setsockopt(zmq.HEARTBEAT_IVL,    5000)
        self.socket.setsockopt(zmq.HEARTBEAT_TIMEOUT, 2000)
        self.socket.setsockopt(zmq.HEARTBEAT_TTL,    60000)

        # **enforce a send timeout** so .send() won’t block forever
        self.socket.setsockopt(zmq.SNDTIMEO, 2000)  # e.g. 2 s send timeout
        # you can also set a default recv timeout if you like:
        # self.socket.setsockopt(zmq.RCVTIMEO, 2000)

        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.connect(self.ip + ":" + self.port)

        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)
        self.connected = True

    def send_message(self, msg: bytes, timeout: int = None) -> str:
        """
        If timeout is None, uses the poller’s default in your code (or infinite).
        Otherwise, polls up to `timeout` ms for a reply.
        """
        try:
            self.socket.send(msg.encode("utf-8"))            # now will error if >2 s blocked
        except zmq.error.Again:
            return "Timeout"

        # if you want per‐call recv‐timeout instead of RCVTIMEO socket opt:
        if timeout is not None:
            socks = dict(self.poller.poll(timeout))
            if socks.get(self.socket) != zmq.POLLIN:
                return "Timeout"
            return self.socket.recv().decode()

        # else block on recv() (or socket.RCVTIMEO if you set it)
        try:
            return self.socket.recv().decode()
        except zmq.error.Again:
            return "Timeout"

    def reconnect(self):
        self.close()
        self.simple_init()

    def close(self):
        self.socket.close()
        self.context.term()
