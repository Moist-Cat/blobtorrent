import xmlrpc.server
import sys
import os
import scgi
from scgi import scgi_server
from scgi.util import SCGIHandler
from config import Config
from mapper import MethodMapper
from xmlrpc.server import SimpleXMLRPCDispatcher
import socket

BACKEND_URL = Config().BACKEND_URL


class Handler(SCGIHandler):
    def __init__(self, *args, **kwargs):
        self.dispatcher = SimpleXMLRPCDispatcher(allow_none=True, encoding=None)
        self.dispatcher.register_introspection_functions()
        self.dispatcher.register_instance(MethodMapper(BACKEND_URL))
        super().__init__(*args, **kwargs)

    def produce(self, env, bodysize, input, output):
        # Read the request body
        data = input.read(bodysize)

        # Process XML-RPC request
        response = self.dispatcher._marshaled_dispatch(data)

        # Write response
        output.write(b"Status: 200 OK\r\n")
        output.write(b"Content-Type: text/xml\r\n")
        output.write(f"Content-Length: {len(response)}\r\n".encode())
        output.write(b"\r\n")
        output.write(response)


class SCGIUnixSocketServer(scgi.scgi_server.SCGIServer):
    def __init__(self, socket_path, handler_class, max_children):
        self.socket_path = socket_path
        # Remove existing socket file if it exists
        try:
            os.unlink(socket_path)
        except OSError:
            if os.path.exists(socket_path):
                raise
        # Initialize parent with None for host/port since we're using UNIX sockets
        super().__init__(
            host=None, port=None, handler_class=handler_class, max_children=max_children
        )

    def get_listening_socket(self):
        # Create UNIX socket
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.bind(self.socket_path)
            # Set permissions to allow access from host
            os.chmod(self.socket_path, 0o666)
            sock.listen(5)
            return sock
        except Exception:
            sock.close()
            raise


def run():
    config = Config()

    # Remove existing socket file if it exists
    if config.SCGI_SOCKET and os.path.exists(config.SCGI_SOCKET):
        os.unlink(config.SCGI_SOCKET)

    # Create SCGI server with UNIX socket
    if config.SCGI_SOCKET:
        server = SCGIUnixSocketServer(
            socket_path=config.SCGI_SOCKET,
            handler_class=Handler,
            max_children=1,
        )

        print(f"Listening on UNIX socket: {config.SCGI_SOCKET}")
    else:
        server = scgi.scgi_server.SCGIServer(
            host=config.SCGI_HOST,
            port=config.SCGI_PORT,
            handler_class=Handler,
        )
        print(f"Listening on HOST/PORT: {config.SCGI_HOST} {config.SCGI_PORT}")
    print(f"Backend URL: {config.BACKEND_URL}")

    try:
        server.serve()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        # Clean up socket file on exit
        if config.SCGI_SOCKET and os.path.exists(config.SCGI_SOCKET):
            os.unlink(config.SCGI_SOCKET)


if __name__ == "__main__":
    run()
