import socket
import struct
import hashlib
import threading
from typing import Tuple, Any, Optional
import time

from log import logged


@logged
class PeerWireProtocol:
    """
    Implements the BitTorrent peer wire protocol for communication between peers.

    This class handles both incoming and outgoing connections, message serialization/deserialization,
    and maintains the state of the connection with a peer.

    Key functionalities:
    - Establishing connections (both outgoing and incoming)
    - Handshake negotiation
    - Message sending and receiving
    - Connection management
    - Error handling and logging

    Data flow:
    1. Connection established (either outgoing via connect() or incoming via handle_incoming_connection())
    2. Handshake exchanged to verify compatibility and info_hash
    3. Messages are exchanged using receive_message() and various send_*() methods
    4. Connection is closed via close() when done

    The protocol follows the BitTorrent specification described in BEP 3 and related BEPs.
    """

    # Protocol constants for message types
    CHOKE = 0
    UNCHOKE = 1
    INTERESTED = 2
    NOT_INTERESTED = 3
    HAVE = 4
    BITFIELD = 5
    REQUEST = 6
    PIECE = 7
    CANCEL = 8
    PORT = 9  # For DHT support (BEP 5)
    EXTENDED = 20  # For extended messages (BEP 10)

    # Mapping of message IDs to human-readable names
    MESSAGE_NAMES = {
        CHOKE: "CHOKE",
        UNCHOKE: "UNCHOKE",
        INTERESTED: "INTERESTED",
        NOT_INTERESTED: "NOT_INTERESTED",
        HAVE: "HAVE",
        BITFIELD: "BITFIELD",
        REQUEST: "REQUEST",
        PIECE: "PIECE",
        CANCEL: "CANCEL",
        PORT: "PORT",
        EXTENDED: "EXTENDED",
    }

    def __init__(self, peer: Tuple[str, int], info_hash: bytes, peer_id: bytes):
        """
        Initialize a new PeerWireProtocol instance.

        Args:
            peer: Tuple of (IP address, port) for the peer
            info_hash: The SHA1 hash of the torrent's info dictionary
            peer_id: Unique identifier for this client

        Note: This constructor doesn't establish a connection. Use connect() for outgoing
        connections or handle_incoming_connection() for incoming connections.
        """
        self.peer = peer
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connected = False
        self.bitfield = None
        self.handshake_done = False
        self.connection_timeout = 120  # 2 minutes timeout
        self.logger.debug(f"PeerWireProtocol initialized for {peer}")

    def __hash__(self):
        """Hash implementation based on peer address and port."""
        return hash((self.peer[0], self.peer[1]))

    def __eq__(self, other):
        """Equality implementation based on peer address and port."""
        return (self.peer[0] == other.peer[0]) and (self.peer[1] == other.peer[1])

    def connect(self) -> bool:
        """
        Establish an outgoing connection to the peer.

        Returns:
            bool: True if connection and handshake were successful, False otherwise

        This method:
        1. Attempts to connect to the peer with a timeout
        2. Performs the BitTorrent handshake protocol
        3. Sets the connection state if successful
        """
        try:
            self.logger.info(f"Connecting to peer {self.peer}")
            self.socket.settimeout(10)
            self.socket.connect(self.peer)
            self.logger.info(f"Connected to peer {self.peer}")
            self._handshake()
            self.connected = True
            return True
        except socket.timeout:
            self.logger.warning(f"Connection timeout to peer {self.peer}")
            return False
        except ConnectionRefusedError:
            self.logger.warning(f"Connection refused by peer {self.peer}")
            return False
        except Exception as e:
            self.logger.error(f"Failed to connect to peer {self.peer}: {e}")
            return False

    def handle_incoming_connection(
        self, socket: socket.socket, peer: Tuple[str, int]
    ) -> bool:
        """
        Handle an incoming connection from a peer.

        Args:
            socket: The already-connected socket from the peer
            peer: Tuple of (IP address, port) for the peer

        Returns:
            bool: True if handshake was successful, False otherwise

        This method is used for passive peer discovery where another peer connects to us.
        It validates the handshake and prepares the protocol for message exchange.
        """
        self.socket = socket
        self.peer = peer

        try:
            # Perform handshake for incoming connection
            return self._handshake(is_incoming=True)
        except Exception as e:
            self.logger.error(f"Error handling incoming connection from {peer}: {e}")
            return False
        self.connected = True
        return True

    def _handshake(self, is_incoming: bool = False) -> bool:
        """
        Perform the BitTorrent handshake protocol.

        Args:
            is_incoming: True if this is an incoming connection, False for outgoing

        Returns:
            bool: True if handshake was successful, False otherwise

        The handshake consists of:
        1. For outgoing: Send handshake, then receive and validate response
        2. For incoming: Receive handshake, validate, then send response

        The handshake format is:
            <pstrlen><pstr><reserved><info_hash><peer_id>
        Where:
            pstrlen: length of protocol string (1 byte)
            pstr: protocol string (19 bytes)
            reserved: 8 reserved bytes
            info_hash: 20-byte SHA1 hash of info dictionary
            peer_id: 20-byte peer identifier
        """
        handshake_msg = (
            b"\x13BitTorrent protocol" + b"\x00" * 8 + self.info_hash + self.peer_id
        )
        try:
            if not is_incoming:
                self.logger.debug(f"Sending handshake to {self.peer} (outgoing)")
                self.socket.send(handshake_msg)

            # Set a shorter timeout for handshake
            self.socket.settimeout(30)
            response = self._recv_exact(68)
            self.logger.debug(f"Received handshake response of length {len(response)}")

            if (
                len(response) != 68
                or response[0:1] != b"\x13"
                or response[1:20] != b"BitTorrent protocol"
                or response[28:48] != self.info_hash
            ):
                self.logger.error(f"Invalid handshake response from {self.peer}")
                raise ConnectionError("Invalid handshake response")

            their_peer_id = response[48:68]

            # Check for self-connection
            if their_peer_id == self.peer_id:
                self.logger.error(f"Detected self-connection to {self.peer}. Closing.")
                self.close()
                return False

            if is_incoming:
                self.logger.debug(f"Sending handshake to {self.peer} (incoming)")
                self.socket.send(handshake_msg)

            # Set normal timeout after handshake
            self.socket.settimeout(self.connection_timeout)
            self.handshake_done = True
            self.logger.info(f"Handshake successful with {self.peer}")
            return True
        except Exception as e:
            self.logger.error(f"Handshake failed with {self.peer}: {e}")
            raise

    def _recv_exact(self, n: int) -> bytes:
        """
        Read exactly n bytes from the socket.

        Args:
            n: Number of bytes to read

        Returns:
            bytes: The read data

        Raises:
            ConnectionError: If connection is closed before reading n bytes
            socket.timeout: If read operation times out
            Exception: For other socket errors

        This method handles partial reads by continuing to read until exactly n bytes are received.
        """
        data = b""
        while len(data) < n:
            try:
                chunk = self.socket.recv(n - len(data))
                if not chunk:
                    raise ConnectionError("Connection closed")
                data += chunk
            except socket.timeout:
                raise
            except Exception as e:
                raise ConnectionError(f"Error receiving data: {e}")
        return data

    def receive_message(self) -> Tuple[Optional[int], Optional[bytes]]:
        """
        Receive a message from the peer.

        Returns:
            Tuple of (message_id, payload) or (None, None) on error or keep-alive

        Message format:
            <length prefix><message ID><payload>
        Where:
            length prefix: 4-byte big-endian integer indicating message length
            message ID: 1-byte message type identifier
            payload: Variable-length message data

        Keep-alive messages have a length prefix of 0 and no message ID or payload.
        """
        # Check if connection is still active
        if not self.connected:
            return None, None
        try:
            # Read exactly 4 bytes for the length prefix
            length_prefix = self._recv_exact(4)
            length = struct.unpack(">I", length_prefix)[0]

            # Handle keep-alive messages
            if length == 0:
                self.logger.debug("Received keep-alive message")
                return None, None

            # Read exactly 'length' bytes for the message body
            message_body = self._recv_exact(length)
            message_id = message_body[0]  # first byte is message ID
            payload = message_body[1:]  # rest is payload

            # Get message name or default to UNKNOWN
            message_name = self.MESSAGE_NAMES.get(message_id, f"UNKNOWN({message_id})")

            # Log error for unknown message types
            if message_id not in self.MESSAGE_NAMES:
                self.logger.error(f"Received unknown message type: {message_id}")
            else:
                self.logger.debug(
                    f"Received message: {message_name}, length: {length}, payload: {len(payload)} bytes"
                )

            return message_id, payload
        except socket.timeout:
            self.logger.warning(f"Timeout while receiving message from {self.peer}")
            return None, None
        except ConnectionError as e:
            self.logger.error(f"Connection error from {self.peer}: {e}")
            return None, None
        except Exception as e:
            self.logger.error(f"Error receiving message from {self.peer}: {e}")
            return None, None

    def send_message(self, message_id: int, payload: bytes = b"") -> bool:
        """
        Send a message to the peer.

        Args:
            message_id: The message type identifier
            payload: The message payload data

        Returns:
            bool: True if message was sent successfully, False otherwise

        This is a generic method for sending all types of messages.
        Message format:
            <length prefix><message ID><payload>
        """
        try:
            # Calculate message length (1 byte for message ID + payload length)
            length = 1 + len(payload)

            # Pack message format: <length><message_id><payload>
            message = struct.pack(">IB", length, message_id) + payload

            # Send the message
            self.socket.send(message)

            # Log the message
            message_name = self.MESSAGE_NAMES.get(message_id, f"UNKNOWN({message_id})")
            if message_id not in self.MESSAGE_NAMES:
                self.logger.error(f"Sent unknown message type: {message_id}")
            else:
                self.logger.debug(f"Sent {message_name} message")

            return True
        except Exception as e:
            self.logger.error(f"Failed to send message {message_id}: {e}")
            self.close()
            return False

    def send_interested(self) -> bool:
        """Send an INTERESTED message to indicate we want pieces from the peer."""
        return self.send_message(self.INTERESTED)

    def send_not_interested(self) -> bool:
        """Send a NOT_INTERESTED message to indicate we don't want pieces from the peer."""
        return self.send_message(self.NOT_INTERESTED)

    def send_unchoke(self) -> bool:
        """Send an UNCHOKE message to allow the peer to request pieces from us."""
        return self.send_message(self.UNCHOKE)

    def send_choke(self) -> bool:
        """Send a CHOKE message to temporarily stop the peer from requesting pieces."""
        return self.send_message(self.CHOKE)

    def send_have(self, piece_index: int) -> bool:
        """Send a HAVE message to indicate we have a piece."""
        payload = struct.pack(">I", piece_index)
        return self.send_message(self.HAVE, payload)

    def send_bitfield(self, bitfield: bytes) -> bool:
        """Send a BITFIELD message to indicate which pieces we have."""
        return self.send_message(self.BITFIELD, bitfield)

    def send_request(self, index: int, begin: int, length: int) -> bool:
        """Send a REQUEST message to request a block of data."""
        payload = struct.pack(">III", index, begin, length)
        return self.send_message(self.REQUEST, payload)

    def send_piece(self, index: int, begin: int, block: bytes) -> bool:
        """Send a PIECE message with requested block data."""
        payload = struct.pack(">II", index, begin) + block
        return self.send_message(self.PIECE, payload)

    def send_cancel(self, index: int, begin: int, length: int) -> bool:
        """Send a CANCEL message to cancel a previous request."""
        payload = struct.pack(">III", index, begin, length)
        return self.send_message(self.CANCEL, payload)

    def send_port(self, port: int) -> bool:
        """Send a PORT message for DHT support (BEP 5)."""
        payload = struct.pack(">H", port)
        return self.send_message(self.PORT, payload)

    def send_keep_alive(self) -> bool:
        """Send a keep-alive message to maintain the connection."""
        try:
            # Keep-alive message is just a length prefix of 0
            self.socket.send(struct.pack(">I", 0))
            self.logger.debug("Sent keep-alive message")
            return True
        except Exception as e:
            self.logger.error(f"Failed to send keep-alive message: {e}")
            return False

    def close(self):
        """Close the connection to the peer and clean up resources."""
        try:
            self.socket.close()
        except Exception as e:
            self.logger.error(f"Error closing connection to {self.peer}: {e}")
        self.connected = False
        self.handshake_done = False
        self.logger.info(f"Closed connection to peer {self.peer}")
