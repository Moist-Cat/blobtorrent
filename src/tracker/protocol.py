"""
Distributed Tracker Gossip Protocol for BitTorrent.
Uses the existing PeerWireProtocol to create a decentralized tracker network.
"""
import socket
import struct
import time
import json
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, asdict
import binascii
import logging

from log import logged

logger = logging.getLogger(__name__)


@dataclass
class TrackerInfo:
    """Information about a tracker in the swarm."""

    hostname: str
    ip: str
    port: int
    info_hash: bytes  # Swarm ID
    peer_id: bytes
    version: str
    timestamp: float
    uptime: float
    load: float  # 0.0 to 1.0
    capabilities: List[str]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        data["info_hash"] = binascii.hexlify(self.info_hash).decode()
        data["peer_id"] = binascii.hexlify(self.peer_id).decode()
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TrackerInfo":
        """Create from dictionary."""
        data = data.copy()
        data["info_hash"] = (
            binascii.b2a_hex(data["info_hash"])
            if isinstance(data["info_hash"], bytes)
            else data["info_hash"]
        )
        data["peer_id"] = (
            binascii.b2a_hex(data["peer_id"])
            if isinstance(data["peer_id"], bytes)
            else data["peer_id"]
        )

        if isinstance(data["info_hash"], str):
            data["info_hash"] = binascii.unhexlify(data["info_hash"])
        if isinstance(data["peer_id"], str):
            data["peer_id"] = binascii.unhexlify(data["peer_id"])

        return cls(**data)


@logged
class TrackerGossipProtocol:
    GOSSIP_HELLO = 100
    GOSSIP_PEER_LIST = 101
    GOSSIP_TRACKER_LIST = 102
    GOSSIP_STATS = 103
    GOSSIP_METADATA = 104
    GOSSIP_PING = 105
    GOSSIP_PONG = 106

    MESSAGE_NAMES = {
        GOSSIP_HELLO: "GOSSIP_HELLO",
        GOSSIP_PEER_LIST: "GOSSIP_PEER_LIST",
        GOSSIP_TRACKER_LIST: "GOSSIP_TRACKER_LIST",
        GOSSIP_STATS: "GOSSIP_STATS",
        GOSSIP_METADATA: "GOSSIP_METADATA",
        GOSSIP_PING: "GOSSIP_PING",
        GOSSIP_PONG: "GOSSIP_PONG",
    }

    def __init__(self, peer: Tuple[str, int], info_hash: bytes, peer_id: bytes):
        """
        Initialize tracker gossip protocol.

        Args:
            peer: Tuple of (IP address, port) for the tracker peer
            info_hash: SHA1 hash of swarm identifier (e.g., "blobtorrent-tracker-swarm")
            peer_id: Unique identifier for this tracker
        """
        self.peer = peer
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.remote_peer_id = None
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connected = False
        self.handshake_done = False
        self.connection_timeout = 120  # 2 minutes timeout

        self.is_incoming = False

        self.last_gossip = 0
        self.gossip_interval = 30  # seconds between gossips

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
            self.socket.settimeout(self.connection_timeout)
            self.logger.info(f"Connected to peer {self.peer}")
            if self._handshake():
                self.connected = True
                return True
            else:
                return False
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
        self.connected = True

        try:
            # Perform handshake for incoming connection
            return self._handshake(is_incoming=True)
        except Exception as e:
            self.logger.error(f"Error handling incoming connection from {peer}: {e}")
            return False
        return True

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
                    self.close()
                    raise ConnectionError(f"Connection closed ({self.peer})")
                else:
                    data += chunk
            except socket.timeout:
                self.close()
                raise
            except Exception as e:
                raise ConnectionError(f"Error receiving data: {e}") from e
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
                self.logger.debug("Received keep-alive message (%s)", self.peer)
                return None, None

            # Read exactly 'length' bytes for the message body
            message_body = self._recv_exact(length)
            message_id = message_body[0]  # first byte is message ID
            payload = message_body[1:]  # rest is payload

            # Get message name or default to UNKNOWN
            message_name = self.MESSAGE_NAMES.get(message_id, f"UNKNOWN({message_id})")

            # Log error for unknown message types
            if message_id not in self.MESSAGE_NAMES:
                self.logger.error(
                    f"Received unknown message type: %s (%s)", message_id, self.peer
                )
            else:
                self.logger.debug(
                    f"Received message: {message_name}, length: {length}, payload: {len(payload)} bytes ({self.peer})"
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

    def _handshake(self, is_incoming: bool = False) -> bool:
        handshake_msg = (
            b"\x13TckTorrent protocol" + b"\x00" * 8 + self.info_hash + self.peer_id
        )
        self.is_incoming = is_incoming
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
                or response[1:20] != b"TckTorrent protocol"
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
            self.remote_peer_id = their_peer_id

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
        finally:
            # Set normal timeout after handshake
            self.socket.settimeout(self.connection_timeout)

    def send_gossip_hello(self, tracker_info: TrackerInfo) -> bool:
        """Send HELLO message with tracker information."""
        try:
            data = json.dumps(tracker_info.to_dict()).encode("utf-8")
            return self.send_message(self.GOSSIP_HELLO, data)
        except Exception as e:
            logger.error(f"Failed to send gossip HELLO: {e}")
            return False

    def send_gossip_peer_list(
        self, info_hash: bytes, peers: List[Tuple[str, int]]
    ) -> bool:
        """Send peer list for a specific torrent."""
        try:
            data = {
                "info_hash": binascii.hexlify(info_hash).decode(),
                "peers": [f"{ip}:{port}" for ip, port in peers],
                "timestamp": time.time(),
            }
            payload = json.dumps(data).encode("utf-8")
            return self.send_message(self.GOSSIP_PEER_LIST, payload)
        except Exception as e:
            logger.error(f"Failed to send gossip peer list: {e}")
            return False

    def send_gossip_tracker_list(self, trackers: List[TrackerInfo]) -> bool:
        """Send list of known trackers."""
        try:
            data = {
                "trackers": [t.to_dict() for t in trackers],
                "timestamp": time.time(),
            }
            payload = json.dumps(data).encode("utf-8")
            return self.send_message(self.GOSSIP_TRACKER_LIST, payload)
        except Exception as e:
            logger.error(f"Failed to send gossip tracker list: {e}")
            return False

    def send_gossip_stats(self, stats: Dict[str, Any]) -> bool:
        """Send statistics."""
        try:
            payload = json.dumps(stats).encode("utf-8")
            return self.send_message(self.GOSSIP_STATS, payload)
        except Exception as e:
            logger.error(f"Failed to send gossip stats: {e}")
            return False

    def send_gossip_ping(self) -> bool:
        """Send PING message."""
        try:
            payload = struct.pack(">d", time.time())
            return self.send_message(self.GOSSIP_PING, payload)
        except Exception as e:
            logger.error(f"Failed to send gossip PING: {e}")
            return False

    def send_gossip_pong(self, ping_time: float) -> bool:
        """Send PONG response to PING."""
        try:
            payload = struct.pack(">d", ping_time)
            return self.send_message(self.GOSSIP_PONG, payload)
        except Exception as e:
            logger.error(f"Failed to send gossip PONG: {e}")
            return False

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


    def parse_gossip_message(
        self, message_id: int, payload: bytes
    ) -> Optional[Dict[str, Any]]:
        """Parse gossip message payload."""
        if message_id in {
            self.GOSSIP_HELLO,
            self.GOSSIP_PEER_LIST,
            self.GOSSIP_TRACKER_LIST,
            self.GOSSIP_STATS,
            self.GOSSIP_METADATA,
        }:
            data = json.loads(payload.decode("utf-8"))

            # Handle special field conversions
            if message_id == self.GOSSIP_HELLO:
                if "info_hash" in data and isinstance(data["info_hash"], str):
                    data["info_hash"] = binascii.unhexlify(data["info_hash"])
                if "peer_id" in data and isinstance(data["peer_id"], str):
                    data["peer_id"] = binascii.unhexlify(data["peer_id"])

            elif message_id == self.GOSSIP_PEER_LIST:
                if "info_hash" in data and isinstance(data["info_hash"], str):
                    data["info_hash"] = binascii.unhexlify(data["info_hash"])

                # Parse peer strings back to tuples
                if "peers" in data:
                    parsed_peers = []
                    for peer_str in data["peers"]:
                        ip, port_str = peer_str.split(":")
                        parsed_peers.append((ip, int(port_str)))
                    data["peers"] = parsed_peers

            elif message_id == self.GOSSIP_TRACKER_LIST:
                if "trackers" in data:
                    trackers = []
                    for tracker_dict in data["trackers"]:
                        try:
                            tracker = TrackerInfo.from_dict(tracker_dict)
                            trackers.append(tracker)
                        except Exception as e:
                            logger.error(f"Failed to parse tracker info: {e}")
                    data["trackers"] = trackers

            return data
        elif message_id in {self.GOSSIP_PING, self.GOSSIP_PONG}:
            if len(payload) == 8:  # double precision float
                timestamp = struct.unpack(">d", payload)[0]
                return {"timestamp": timestamp}
        return None

    def close(self):
        """Close the connection to the peer and clean up resources."""
        try:
            self.socket.close()
        except Exception as e:
            self.logger.error(f"Error closing connection to {self.peer}: {e}")
        self.connected = False
        self.handshake_done = False
        self.logger.info(f"Closed connection to peer {self.peer}")
