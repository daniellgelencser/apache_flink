import json
import socket


class NetworkStreamer:
    connection: socket.socket

    def __init__(self, host, port, queue):
        self.host = host
        self.port = port
        self.queue = queue

    # def update(self, data: []) -> None:
    #     with self.connection as connection:
    #         self.send(data, connection)

    def consume_loop(self, consume_f, *args):
        print("yo")
        self.queue.start()

        for _ in range(self.queue.get_budget()):
            data = self.queue.next()
            consume_f(json.dumps(data) + '\n', *args)

    def send(self, data, c):
        c.sendall(data.encode())

    def run(self):
        print("start streamer for host " + self.host)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as streaming_socket:

            print("binding with socket on port " + str(self.port))
            streaming_socket.bind((self.host, self.port))

            print("waiting for connection")
            streaming_socket.listen(8)

            conn, addr = streaming_socket.accept()
            
            with conn:
                self.consume_loop(self.send, conn)

            streaming_socket.close()