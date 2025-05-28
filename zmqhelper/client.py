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

        # Heartbeat: ping every 5 s of silence
        self.socket.setsockopt(zmq.HEARTBEAT_IVL,    5000)
        # If no traffic arrives within 2 s after a ping, consider peer dead
        self.socket.setsockopt(zmq.HEARTBEAT_TIMEOUT, 2000)
        # Purge peers that haven’t reconnected within 60 s
        self.socket.setsockopt(zmq.HEARTBEAT_TTL,    30000)

        # Linger=0 so close() never blocks
        self.socket.setsockopt(zmq.LINGER, 0)

        self.socket.connect(f"{self.ip}:{self.port}")

        # Poller for recv timeouts (in ms)
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)

        self.connected = True

    def send_message(self, msg, timeout=10_000):
        """Send a UTF-8 message and wait up to `timeout` ms for a reply."""
        payload = msg.encode()
        try:
            self.socket.send(payload)
        except zmq.error.Again:
            return "Timeout"

        socks = dict(self.poller.poll(timeout))
        if socks.get(self.socket) == zmq.POLLIN:
            try:
                reply = self.socket.recv().decode()
                return reply
            except zmq.error.ZMQError:
                # connection broken mid-recv
                self.connected = False
                self.reconnect()
                return "Timeout"
        else:
            # poll timed out
            self.connected = False
            self.reconnect()
            return "Timeout"

    def reconnect(self):
        self.close()
        self.simple_init()

    def close(self):
        self.socket.close()
        self.context.term()


#   Author: Krister Shalm
#   Simple ZMQ client in Python
#   Connects REQ socket

# import zmq
# import time

# class Client():
#     """

#     """
#     def __init__(self, ip, port):
#         self.ip = 'tcp://'+str(ip)
#         self.port = str(port)
#         self.simple_init()

#     def simple_init(self):
#         self.context = zmq.Context()
#         self.socket = self.context.socket(zmq.REQ)

#         # Send a ping every 5 s of inactivity
#         self.sock.setsockopt(zmq.HEARTBEAT_IVL, 5000)
#         # If no traffic arrives within 2 s after ping, consider peer dead
#         self.sock.setsockopt(zmq.HEARTBEAT_TIMEOUT, 2000)
#         # Drop any peers that exceed TTL of 60 s without reconnecting
#         self.sock.setsockopt(zmq.HEARTBEAT_TTL, 60000)
        
#         self.socket.connect(self.ip + ":" + self.port)

#         self.poller = zmq.Poller()
#         self.poller.register(self.socket, zmq.POLLIN)
#         self.connected = True


#     def send_message(self, msg, timeout=10000):
#         msg = msg.encode()
#         msgTimeout = 'Timeout'
#         # msgTimeout = msgTimeout.encode()
        
#         try:
#             self.socket.send(msg)
#         except:
#             return(msgTimeout)
#         socks = dict(self.poller.poll(timeout))
#         if socks:
#             if socks.get(self.socket) == zmq.POLLIN:
#                 try:
#                     response = self.socket.recv()
#                     # print(msg)
#                     # print(response)
#                     # print('')
#                     response = response.decode()
#                 except:
#                     # response = msgTimeout
#                     self.connected = False
#                     self.reconnect()
#                 return response
#         else:
#             response = msgTimeout
#             self.connected = False
#             self.reconnect()


#         return(response)

#     def reconnect(self):
#         self.close()
#         self.simple_init()


#     def close(self):
#         self.socket.close()
#         self.context.term()


# if __name__ == '__main__':
#     import time
#     ip = '0.0.0.0'
#     port = '5555'
#     print('creating connection')
#     con = Client(ip, port)
#     clientID = time.time()
#     for i in range(100):
#         resp = con.send_message('clientID:'+str(clientID)+' '+str(i))
#         time.sleep(0.1)
#         print(resp)
    
