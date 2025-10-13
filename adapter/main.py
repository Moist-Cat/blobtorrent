# adapter.py
import xmlrpc.server
import sys
import os
import scgi
from scgi import scgi_server
from scgi.util import SCGIHandler
from config import Config
from mapper import MethodMapper
from xmlrpc.server import SimpleXMLRPCDispatcher

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

def run():
    config = Config()

    server = scgi_server.SCGIServer(host=config.SCGI_HOST, port=config.SCGI_PORT, handler_class=Handler, max_children=1)

    print(f"Starting SCGI server for BitTorrent backend...")
    print(f"Backend URL: {config.BACKEND_URL}")
    
    print(f"Listening on TCP: {config.SCGI_HOST}:{config.SCGI_PORT}")
    
    try:
        server.serve()
    except KeyboardInterrupt:
        print("\nShutting down server...")

if __name__ == '__main__':
    run()
