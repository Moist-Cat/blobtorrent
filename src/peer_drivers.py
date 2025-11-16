"""
Drivers for protocols that discover new peers in a network
"""
from abc import ABC, abstractmethod
import os
import asyncio
import binascii
import socket
import struct
import time
import urllib.parse
import requests
import random
from typing import List, Tuple, Dict, Set, Optional, Union
import threading
import hashlib
import ipaddress
import concurrent.futures
import threading
import select
import subprocess
import re


from filesystem import BencodeDecoder, BencodeEncoder
from log import logged


class PeerDiscovery(ABC):
    """Abstract base class for all peer discovery mechanisms"""

    @abstractmethod
    def initialize(self) -> bool:
        """Initialize the discovery mechanism"""
        pass

    @abstractmethod
    def get_peers(self) -> List[Tuple[str, int]]:
        """Get the current list of discovered peers"""
        pass

    @abstractmethod
    def cleanup(self):
        """Clean up resources used by the discovery mechanism"""
        pass

    @abstractmethod
    def start_discovery(self):
        """Start the peer discovery process"""
        pass

    @abstractmethod
    def stop_discovery(self):
        """Stop the peer discovery process"""
        pass

    @abstractmethod
    def update_stats(self, downloaded: int, uploaded: int, left: int):
        """Update statistics for the discovery mechanism"""
        pass


# UDP tracker protocol constants
UDP_CONNECT_MAGIC = 0x41727101980
UDP_ACTION_CONNECT = 0
UDP_ACTION_ANNOUNCE = 1
UDP_ACTION_SCRAPE = 2
UDP_ACTION_ERROR = 3


@logged
class TrackerDriver(PeerDiscovery):
    def __init__(
        self,
        torrent: "TorrentFile",
        peer_id: bytes,
        port: int = 6881,
    ):
        self.torrent = torrent
        self.peer_id = peer_id
        self.port = port
        self.uploaded = 0
        self.downloaded = 0
        self.left = torrent.total_size
        self.interval = 1800  # Default interval (30 minutes)
        self.min_interval = 300  # Default minimum interval (5 minutes)
        self.last_announce = 0
        self.complete = 0
        self.incomplete = 0
        self.peers: List[Tuple[str, int]] = []
        self.lock = threading.Lock()
        self._stop_announcing = False
        self.announce_thread: Optional[threading.Thread] = None
        self.initialized = False

        # Get all announce URLs from the torrent
        self.announce_urls = self._get_announce_urls()
        self.current_announce_url_index = 0

        # UDP tracker connection state
        self.udp_connection_id: Optional[int] = None
        self.udp_connection_time = 0
        self.udp_transaction_id = 0
        # global timeout
        # mainly for UDP
        self.timeout = 30

        self.logger.info(
            f"TrackerDriver created for {torrent} with {len(self.announce_urls)} announce URLs"
        )

    def _get_announce_urls(self) -> List[str]:
        """Extract all announce URLs from the torrent metadata"""
        urls = []

        # Primary announce URL
        if b"announce" in self.torrent.metadata:
            urls.append(self.torrent.metadata[b"announce"].decode())

        # Announce list (if available)
        if b"announce-list" in self.torrent.metadata:
            for announce_entry in self.torrent.metadata[b"announce-list"]:
                if isinstance(announce_entry, list) and announce_entry:
                    urls.append(announce_entry[0].decode())

        return urls

    def get_current_announce_url(self) -> str:
        """Get the current announce URL, cycling through available URLs if needed"""
        if not self.announce_urls:
            raise ValueError("No announce URLs available")

        return self.announce_urls[self.current_announce_url_index]

    def rotate_announce_url(self):
        """Move to the next announce URL"""
        if len(self.announce_urls) > 1:
            self.current_announce_url_index = (
                self.current_announce_url_index + 1
            ) % len(self.announce_urls)
            self.logger.info(
                f"Rotated to announce URL: {self.get_current_announce_url()}"
            )

    def update_stats(self, downloaded: int, uploaded: int, left: int):
        """Update the tracker statistics"""
        with self.lock:
            self.downloaded = downloaded
            self.uploaded = uploaded
            self.left = left

    # announce
    def _generate_transaction_id(self) -> int:
        """Generate a new transaction ID for UDP tracker communication"""
        self.udp_transaction_id = (self.udp_transaction_id + 1) % 0xFFFFFFFF
        return self.udp_transaction_id

    def _parse_udp_tracker_url(self, url: str) -> Tuple[str, int]:
        """Parse UDP tracker URL into host and port"""
        parsed = urllib.parse.urlparse(url)
        host = parsed.hostname
        port = parsed.port or 80  # Default port for UDP trackers is typically 80

        if not host:
            raise ValueError(f"Invalid UDP tracker URL: {url}")

        return host, port

    def _udp_connect(self, host: str, port: int) -> bool:
        """Establish connection with UDP tracker"""
        try:
            # Create UDP socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(self.timeout)  # 10 second timeout

            # Generate connection request
            transaction_id = self._generate_transaction_id()
            connect_request = struct.pack(
                "!QII", UDP_CONNECT_MAGIC, UDP_ACTION_CONNECT, transaction_id
            )

            # Send connect request
            sock.sendto(connect_request, (host, port))

            # Receive response
            response_data, _ = sock.recvfrom(16)  # Connect response is 16 bytes
            sock.close()

            if len(response_data) < 16:
                self.logger.error("UDP connect response too short")
                return False

            # Parse response
            action, transaction_id_resp, connection_id = struct.unpack(
                "!IIQ", response_data
            )

            if action != UDP_ACTION_CONNECT:
                self.logger.error(f"UDP connect failed: unexpected action {action}")
                return False

            if transaction_id_resp != transaction_id:
                self.logger.error("UDP connect failed: transaction ID mismatch")
                return False

            # Store connection state
            self.udp_connection_id = connection_id
            self.udp_connection_time = time.time()
            self.logger.info(f"UDP tracker connection established: {host}:{port}")
            return True

        except socket.timeout:
            self.logger.error(f"UDP connect timeout: {host}:{port}")
            return False
        except Exception as e:
            self.logger.error(f"UDP connect failed: {e}")
            return False

    def _udp_announce(
        self, host: str, port: int, event: str = ""
    ) -> List[Tuple[str, int]]:
        """Announce to UDP tracker"""
        if self.udp_connection_id is None:
            # Try to establish connection first
            if not self._udp_connect(host, port):
                return []

        try:
            # Map event string to UDP event code
            event_map = {
                "": 0,  # None
                "completed": 1,  # Completed
                "started": 2,  # Started
                "stopped": 3,  # Stopped
            }
            event_code = event_map.get(event, 0)

            # Generate transaction ID
            transaction_id = self._generate_transaction_id()

            # Build announce request
            key = 0x12345678  # Random key for tracking purposes
            num_want = 50  # Request as many peers as possible

            announce_request = struct.pack(
                "!QII20s20sQQQIIIiH",
                self.udp_connection_id,
                UDP_ACTION_ANNOUNCE,
                transaction_id,
                self.torrent.info_hash,
                self.peer_id,
                self.downloaded,
                self.left,
                self.uploaded,
                event_code,
                0,  # IP address (0 = use sender's address)
                key,
                num_want,
                self.port,
            )

            # Create socket and send request
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(self.timeout)
            sock.sendto(announce_request, (host, port))

            # Receive response (minimum 20 bytes header + peer data)
            response_data, _ = sock.recvfrom(4096)
            sock.close()

            if len(response_data) < 20:
                self.logger.error("UDP announce response too short")
                return []

            # Parse response header
            action, transaction_id_resp, interval, leechers, seeders = struct.unpack(
                "!IIIII", response_data[:20]
            )

            if action == UDP_ACTION_ERROR:
                error_msg = response_data[8:].decode("utf-8", errors="ignore")
                self.logger.error(f"UDP tracker error: {error_msg}")
                return []

            if action != UDP_ACTION_ANNOUNCE:
                self.logger.error(f"UDP announce failed: unexpected action {action}")
                return []

            if transaction_id_resp != transaction_id:
                self.logger.error("UDP announce failed: transaction ID mismatch")
                return []

            # Update tracker stats
            self.interval = interval
            self.complete = seeders
            self.incomplete = leechers

            self.logger.info(
                f"UDP tracker response: {seeders} seeders, {leechers} leechers, interval {interval}s"
            )

            # Parse peer list (compact format)
            peer_data = response_data[20:]
            peer_list = []

            for i in range(0, len(peer_data), 6):
                if i + 6 <= len(peer_data):
                    ip_bytes = peer_data[i : i + 4]
                    port_bytes = peer_data[i + 4 : i + 6]
                    ip = socket.inet_ntoa(ip_bytes)
                    port = struct.unpack("!H", port_bytes)[0]
                    peer_list.append((ip, port))
                    self.logger.debug(f"Discovered UDP peer: {ip}:{port}")

            self.logger.info(
                f"Successfully discovered {len(peer_list)} peers from UDP tracker"
            )
            self.last_announce = time.time()
            return peer_list

        except socket.timeout:
            self.logger.error(f"UDP announce timeout: {host}:{port}")
            # Reset connection on timeout
            self.udp_connection_id = None
            return []
        except Exception as e:
            self.logger.error(f"UDP announce failed: {e}")
            # Reset connection on error
            self.udp_connection_id = None
            return []

    def _http_announce(self, url: str, event: str = "") -> List[Tuple[str, int]]:
        """Announce to HTTP/HTTPS tracker"""
        self.logger.info(f"Announcing to HTTP tracker: {url} (event: {event})")

        parsed = urllib.parse.urlparse(url)
        query_params = {
            #"info_hash": self.torrent.info_hash,
            "info_hash": self.torrent.info_hash,
            "peer_id": self.peer_id,
            "port": self.port,
            "uploaded": self.uploaded,
            "downloaded": self.downloaded,
            "left": self.left,
            "compact": 1,
        }

        if event:
            query_params["event"] = event

        headers = {
            "User-Agent": "BlobTorrent/0.1.0",
        }

        try:
            self.logger.debug(f"Sending tracker request with params: {query_params}")
            response = requests.get(
                url, params=query_params, headers=headers, timeout=30
            )

            if not response.ok:
                self.logger.error(
                    f"Tracker responded with error: {response.status_code} - {response.reason}"
                )
                self.rotate_announce_url()
                return []

            response_data = response.content
            self.logger.debug(f"Tracker response size: {len(response_data)} bytes")
            decoded_response = BencodeDecoder.decode_full(response_data)
            self.logger.info(f"Tracker response: {list(decoded_response.keys())}")

            # Update tracker interval if provided
            if b"interval" in decoded_response:
                self.interval = decoded_response[b"interval"]
                self.logger.info(f"Tracker interval set to {self.interval} seconds")

            if b"min interval" in decoded_response:
                self.min_interval = decoded_response[b"min interval"]
                self.logger.info(
                    f"Tracker min interval set to {self.min_interval} seconds"
                )

            # Update peer counts if provided
            if b"complete" in decoded_response:
                self.complete = decoded_response[b"complete"]
                self.logger.info(f"Seeders: {self.complete}")

            if b"incomplete" in decoded_response:
                self.incomplete = decoded_response[b"incomplete"]
                self.logger.info(f"Leechers: {self.incomplete}")

            # Parse peers
            peers = decoded_response.get(b"peers", b"")
            peer_list = []

            if isinstance(peers, bytes):
                self.logger.info(
                    f"Received compact peer list with {len(peers)//6} peers"
                )
                for i in range(0, len(peers), 6):
                    ip = socket.inet_ntoa(peers[i : i + 4])
                    port = struct.unpack("!H", peers[i + 4 : i + 6])[0]
                    peer_list.append((ip, port))
                    self.logger.debug(f"Discovered peer: {ip}:{port}")

            elif isinstance(peers, list):
                self.logger.info(
                    f"Received non-compact peer list with {len(peers)} peers"
                )
                for peer in peers:
                    ip = (
                        peer[b"ip"].decode()
                        if isinstance(peer[b"ip"], bytes)
                        else peer[b"ip"]
                    )
                    port = peer[b"port"]
                    peer_list.append((ip, port))
                    self.logger.debug(f"Discovered peer: {ip}:{port}")
            else:
                self.logger.warning(f"Unknown peer format: {type(peers)}")
                return []

            self.logger.info(
                f"Successfully discovered {len(peer_list)} peers from HTTP tracker"
            )
            self.last_announce = time.time()
            if not peer_list:
                self.rotate_announce_url()
            return peer_list

        except requests.exceptions.Timeout:
            self.logger.error(f"Tracker request timed out: {url}")
            self.rotate_announce_url()
            return []
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Tracker request failed: {e}")
            self.rotate_announce_url()
            return []
        except Exception as e:
            self.logger.error(f"Error during tracker announce: {e}")
            self.rotate_announce_url()
            return []

    def announce(self, event: str = "") -> List[Tuple[str, int]]:
        """Announce to the tracker with optional event"""
        if not self.initialized:
            self.logger.error("Tracker not initialized")
            return []

        if not self.announce_urls:
            self.logger.error("No announce URLs available")
            return []

        url = self.get_current_announce_url()
        parsed = urllib.parse.urlparse(url)

        try:
            if parsed.scheme.startswith("http"):
                peer_list = self._http_announce(url, event)
            elif parsed.scheme == "udp":
                host, port = self._parse_udp_tracker_url(url)
                peer_list = self._udp_announce(host, port, event)
            else:
                self.logger.error(f"Unsupported tracker scheme: {parsed.scheme}")
                self.rotate_announce_url()
                return []

            # Update our peer list
            with self.lock:
                self.peers = peer_list

            return peer_list

        except Exception as e:
            self.logger.error(f"Error during tracker announce: {e}")
            self.rotate_announce_url()
            return []

    def initialize(self) -> bool:
        """Initialize the tracker connection"""
        try:
            if self.announce_urls:
                test_url = self.announce_urls[0]
                parsed = urllib.parse.urlparse(test_url)

                if parsed.scheme.startswith("http") or parsed.scheme == "udp":
                    self.initialized = True
                    self.logger.info("TrackerDriver initialized successfully")
                    return True
                else:
                    self.logger.warning(f"Unsupported tracker scheme: {parsed.scheme}")
                    return False
            else:
                self.logger.error("No announce URLs available")
                return False
        except Exception as e:
            self.logger.error(f"Failed to initialize TrackerDriver: {e}")
            return False

    def cleanup(self):
        """Clean up tracker resources"""
        self.stop_discovery()
        self.initialized = False
        # Reset UDP connection state
        self.udp_connection_id = None
        self.udp_connection_time = 0
        self.logger.info("TrackerDriver cleaned up")

    def start_discovery(self):
        """Start the tracker discovery process"""
        if not self.initialized:
            self.logger.error("Tracker not initialized")
            return

        if self.announce_thread and self.announce_thread.is_alive():
            self.logger.warning("Announce thread is already running")
            return

        self._stop_announcing = False

        def announce_loop():
            # Initial announce with "started" event
            self.announce("started")

            while not self._stop_announcing:
                try:
                    # Regular announce
                    self.announce()

                    # Sleep for the interval
                    time.sleep(self.interval)
                except Exception as e:
                    self.logger.error(f"Error in announce loop: {e}")
                    time.sleep(60)  # Wait a minute before retrying

        self.announce_thread = threading.Thread(target=announce_loop, daemon=True)
        self.announce_thread.start()
        self.logger.info("Started periodic tracker announcing")

    def stop_discovery(self):
        """Stop periodic announcing and send a final 'stopped' event"""
        self._stop_announcing = True

        if self.announce_thread:
            self.announce_thread.join(timeout=5)

        # Send final "stopped" event
        self.announce("stopped")
        self.logger.info("Stopped tracker announcing")

    def get_peers(self) -> List[Tuple[str, int]]:
        """Get the current list of peers"""
        with self.lock:
            return self.peers.copy()


@logged
class LocalPeerDiscoveryDriver(PeerDiscovery):
    """Local Peer Discovery implementation for LAN environments"""

    # LPD multicast address and port as per BEP 14
    LPD_MULTICAST_ADDR = "239.192.152.143"
    LPD_MULTICAST_PORT = 6771
    LPD_ANNOUNCE_INTERVAL = 30  # 5 minutes as per specification

    def __init__(self, torrent, peer_id, port=6881):
        self.torrent = torrent
        self.peer_id = peer_id
        self.port = port
        self.peers: List[Tuple[str, int]] = []
        self.socket = None
        self.running = False
        self.discovery_thread = None
        self.announce_thread = None
        self.lock = threading.Lock()
        self.initialized = False

    def initialize(self) -> bool:
        """Initialize LPD by creating a multicast socket"""
        try:
            # Create UDP socket
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            # Bind to the LPD port
            # self.socket.bind(("", self.LPD_MULTICAST_PORT))
            self.socket.bind(("", self.LPD_MULTICAST_PORT))

            # Add socket to multicast group
            mreq = struct.pack(
                "4sl", socket.inet_aton(self.LPD_MULTICAST_ADDR), socket.INADDR_ANY
            )
            self.socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

            # Set socket timeout to allow for graceful shutdown
            self.socket.settimeout(1.0)

            self.initialized = True
            self.logger.info("LPD initialized successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to initialize LPD: {e}")
            return False

    def get_peers(self) -> List[Tuple[str, int]]:
        """Get the current list of discovered peers"""
        with self.lock:
            return self.peers.copy()

    def cleanup(self):
        """Clean up LPD resources"""
        self.stop_discovery()
        if self.socket:
            try:
                self.socket.close()
            except:
                pass
            self.socket = None
        self.initialized = False
        self.logger.info("LPD cleaned up")

    def start_discovery(self):
        """Start the LPD discovery process"""
        if not self.initialized:
            self.logger.error("LPD not initialized")
            return

        if self.running:
            return

        self.running = True

        # Start thread for receiving announcements
        self.discovery_thread = threading.Thread(
            target=self._discovery_loop, daemon=True
        )
        self.discovery_thread.start()

        # Start thread for sending announcements
        self.announce_thread = threading.Thread(target=self._announce_loop, daemon=True)
        self.announce_thread.start()

        self.logger.info("LPD discovery started")

    def stop_discovery(self):
        """Stop the LPD discovery process"""
        self.running = False

        if self.discovery_thread:
            self.discovery_thread.join(timeout=5)
            self.discovery_thread = None

        if self.announce_thread:
            self.announce_thread.join(timeout=5)
            self.announce_thread = None

        self.logger.info("LPD discovery stopped")

    def update_stats(self, downloaded: int, uploaded: int, left: int):
        """Update statistics - LPD doesn't use stats, but we need to implement the interface"""
        # LPD doesn't use stats, so this is a no-op
        pass

    def _discovery_loop(self):
        """Listen for LPD announcements from other peers"""
        while self.running and self.socket:
            try:
                data, addr = self.socket.recvfrom(1024)
                self._process_announcement(data, addr[0])
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:  # Only log if we're still running
                    self.logger.error(f"Error in LPD discovery loop: {e}")

    def _announce_loop(self):
        """Periodically send LPD announcements"""
        # Send initial announcement immediately
        self._send_announcement()

        # Then send at regular intervals
        while self.running:
            time.sleep(self.LPD_ANNOUNCE_INTERVAL)
            self._send_announcement()

    def _send_announcement(self):
        """Send an LPD announcement"""
        try:
            # Format: BT-SEARCH * HTTP/1.1\r\nHost: <multicast address>:<port>\r\nPort: <port>\r\nInfohash: <infohash>\r\n\r\n
            message = (
                f"BT-SEARCH * HTTP/1.1\r\n"
                f"Host: {self.LPD_MULTICAST_ADDR}:{self.LPD_MULTICAST_PORT}\r\n"
                f"Port: {self.port}\r\n"
                f"Infohash: {binascii.hexlify(self.torrent.info_hash).decode()}\r\n"
                f"\r\n"
            ).encode("utf-8")

            self.socket.sendto(
                message, (self.LPD_MULTICAST_ADDR, self.LPD_MULTICAST_PORT)
            )
            self.logger.debug("Sent LPD announcement")

        except Exception as e:
            self.logger.error(f"Failed to send LPD announcement: {e}")

    def _process_announcement(self, data: bytes, source_ip: str):
        """Process an incoming LPD announcement"""
        try:
            message = data.decode("utf-8")
            lines = message.split("\r\n")

            # Parse the announcement
            port = None
            infohash = None

            for line in lines:
                if line.startswith("Port:"):
                    port = int(line.split(":")[1].strip())
                elif line.startswith("Infohash:"):
                    infohash = line.split(":")[1].strip()

            # Validate the announcement
            if not port or not infohash:
                self.logger.debug("Invalid LPD announcement received")
                return

            # Check if the infohash matches our torrent
            if infohash != binascii.hexlify(self.torrent.info_hash).decode():
                self.logger.debug(f"LPD announcement for different torrent: {infohash}")
                return

            # Add the peer to our list
            with self.lock:
                peer = (source_ip, port)
                if peer not in self.peers:
                    self.peers.append(peer)
                    self.logger.info(f"Discovered peer via LPD: {source_ip}:{port}")

        except Exception as e:
            self.logger.error(f"Error processing LPD announcement: {e}")


@logged
class DHTDiscovery(PeerDiscovery):
    """Distributed Hash Table (DHT) implementation for peer discovery (BEP 5)"""

    def __init__(self, torrent, peer_id, port=6881, dht_port=7883):
        self.torrent = torrent
        self.dht_id = self._generate_node_id()
        self.peer_id = peer_id
        self.port = port
        self.dht_port = dht_port
        self.peers: List[Tuple[str, int]] = []
        self.nodes: List[Tuple[str, int]] = []  # Known DHT nodes
        self.socket = None
        self.running = False
        self.dht_thread = None
        self.announce_thread = None
        self.lock = threading.Lock()
        self.initialized = False
        self.transactions = {}  # Track ongoing transactions

        self.bootstrap_nodes = [
            ("router.bittorrent.com", 6881),
            ("dht.transmissionbt.com", 6881),
            ("router.utorrent.com", 6881),
            ("dht.aelitis.com", 6881),
        ]

    def initialize(self) -> bool:
        """Initialize DHT by creating a UDP socket and joining the DHT network"""
        try:
            # Create UDP socket for DHT
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.socket.bind(("0.0.0.0", self.dht_port))

            # Add bootstrap nodes
            self.nodes.extend(self.bootstrap_nodes)

            self.initialized = True
            self.logger.info("DHT initialized successfully on port %d", self.dht_port)
            return True

        except Exception as e:
            self.logger.error("Failed to initialize DHT: %s", e)
            return False

    def get_peers(self) -> List[Tuple[str, int]]:
        """Get the current list of discovered peers"""
        with self.lock:
            return self.peers.copy()

    def cleanup(self):
        """Clean up DHT resources"""
        self.stop_discovery()
        if self.socket:
            try:
                self.socket.close()
            except:
                pass
            self.socket = None
        self.initialized = False
        self.logger.info("DHT cleaned up")

    def start_discovery(self):
        """Start the DHT discovery process"""
        if not self.initialized:
            self.logger.error("DHT not initialized")
            return

        if self.running:
            return

        self.running = True

        # Start DHT thread
        self.dht_thread = threading.Thread(target=self._dht_loop, daemon=True)
        self.dht_thread.start()

        # Bootstrap the DHT
        self._bootstrap()

        def announce_loop():
            while not self.running:
                try:
                    # Regular announce
                    self._bootstrap()
                    # Sleep for the interval
                    time.sleep(60)
                except Exception as e:
                    self.logger.error(f"Error in announce loop: {e}")
                    time.sleep(60)  # Wait a minute before retrying

        self.announce_thread = threading.Thread(target=announce_loop, daemon=True)

        self.logger.info("DHT discovery started")

    def stop_discovery(self):
        """Stop the DHT discovery process"""
        self.running = False

        if self.dht_thread:
            self.dht_thread.join(timeout=5)
            self.dht_thread = None

        self.logger.info("DHT discovery stopped")

    def update_stats(self, downloaded: int, uploaded: int, left: int):
        """Update statistics - DHT doesn't use stats, but we need to implement the interface"""
        # DHT doesn't use stats, so this is a no-op
        pass

    def _dht_loop(self):
        """Main DHT loop for handling incoming messages"""
        while self.running and self.socket:
            try:
                # Receive data
                data, addr = self.socket.recvfrom(2048)
                self._handle_message(data, addr)

            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    self.logger.error("Error in DHT loop: %s", e)

    def _handle_message(self, data: bytes, addr: Tuple[str, int]):
        """Handle incoming DHT message"""
        try:
            # Decode bencoded message
            message = BencodeDecoder.decode_full(data)
            if not isinstance(message, dict):
                return

            # Get message type
            msg_type = message.get(b"y", b"")

            if msg_type == b"r":
                # Response
                self._handle_response(message, addr)
            elif msg_type == b"q":
                # Query
                self._handle_query(message, addr)
            elif msg_type == b"e":
                # Error
                self._handle_error(message, addr)

        except Exception as e:
            self.logger.error("Error handling DHT message from %s: %s", addr, e)

    def _handle_response(self, message: dict, addr: Tuple[str, int]):
        """Handle DHT response message"""
        try:
            # Check transaction ID
            t = message.get(b"t")
            if t not in self.transactions:
                return

            # Get the original query info
            query_info = self.transactions[t]
            query_type = query_info["type"]

            # Handle based on query type
            if query_type == "ping":
                self._handle_ping_response(message, addr)
            elif query_type == "find_node":
                self._handle_find_node_response(message, addr)
            elif query_type == "get_peers":
                self._handle_get_peers_response(message, addr)
            elif query_type == "announce_peer":
                self._handle_announce_peer_response(message, addr)

            # Remove completed transaction
            del self.transactions[t]

        except Exception as e:
            self.logger.error("Error handling DHT response from %s: %s", addr, e)

    def _handle_query(self, message: dict, addr: Tuple[str, int]):
        """Handle DHT query message"""
        try:
            q = message.get(b"q", b"")
            a = message.get(b"a", {})
            t = message.get(b"t", b"")

            if q == b"ping":
                self._handle_ping_query(a, t, addr)
            elif q == b"find_node":
                self._handle_find_node_query(a, t, addr)
            elif q == b"get_peers":
                self._handle_get_peers_query(a, t, addr)
            elif q == b"announce_peer":
                self._handle_announce_peer_query(a, t, addr)

        except Exception as e:
            self.logger.error("Error handling DHT query from %s: %s", addr, e)

    def _handle_error(self, message: dict, addr: Tuple[str, int]):
        """Handle DHT error message"""
        try:
            e = message.get(b"e", [])
            if len(e) >= 2:
                error_code = e[0]
                error_msg = e[1]
                self.logger.warning(
                    "DHT error from %s: %d - %s", addr, error_code, error_msg
                )

        except Exception as e:
            self.logger.error("Error handling DHT error from %s: %s", addr, e)

    def _handle_ping_query(self, a: dict, t: bytes, addr: Tuple[str, int]):
        """Handle ping query"""
        try:
            # Send ping response
            response = {b"t": t, b"y": b"r", b"r": {b"id": self.dht_id}}

            self._send_message(response, addr)

        except Exception as e:
            self.logger.error("Error handling ping query: %s", e)

    def _handle_find_node_query(self, a: dict, t: bytes, addr: Tuple[str, int]):
        """Handle find_node query"""
        try:
            # Send find_node response with closest nodes
            response = {
                b"t": t,
                b"y": b"r",
                b"r": {
                    b"id": self.dht_id,
                    b"nodes": self._get_closest_nodes(a.get(b"target", b"")),
                },
            }

            self._send_message(response, addr)

        except Exception as e:
            self.logger.error("Error handling find_node query: %s", e)

    def _handle_get_peers_query(self, a: dict, t: bytes, addr: Tuple[str, int]):
        """Handle get_peers query"""
        try:
            info_hash = a.get(b"info_hash", b"")

            # Check if we have peers for this info_hash
            peers = self._get_peers_for_info_hash(info_hash)

            if peers:
                # We have peers, return them
                response = {
                    b"t": t,
                    b"y": b"r",
                    b"r": {b"id": self.dht_id, b"values": peers},
                }
            else:
                # No peers, return closest nodes
                response = {
                    b"t": t,
                    b"y": b"r",
                    b"r": {
                        b"id": self.dht_id,
                        b"nodes": self._get_closest_nodes(info_hash),
                        b"token": self._generate_token(addr),
                    },
                }

            self._send_message(response, addr)

        except Exception as e:
            self.logger.error("Error handling get_peers query: %s", e)

    def _handle_announce_peer_query(self, a: dict, t: bytes, addr: Tuple[str, int]):
        """Handle announce_peer query"""
        try:
            info_hash = a.get(b"info_hash", b"")
            token = a.get(b"token", b"")
            port = a.get(b"port", 0)

            # Validate token
            if not self._validate_token(token, addr):
                self.logger.warning("Invalid token from %s for announce_peer", addr)
                return

            # Add the peer to our list
            peer_ip = addr[0]
            if b"implied_port" in a and a[b"implied_port"] == 1:
                peer_port = addr[1]
            else:
                peer_port = port

            with self.lock:
                peer = (peer_ip, peer_port)
                if peer not in self.peers:
                    self.peers.append(peer)
                    self.logger.info(
                        "Discovered peer via DHT announce: %s:%d", peer_ip, peer_port
                    )

            # Send response
            response = {b"t": t, b"y": b"r", b"r": {b"id": self.dht_id}}

            self._send_message(response, addr)

        except Exception as e:
            self.logger.error("Error handling announce_peer query: %s", e)

    def _handle_ping_response(self, message: dict, addr: Tuple[str, int]):
        """Handle ping response"""
        # Add node to our routing table
        self._add_node(addr)
        self._send_get_peers(addr, self.torrent.info_hash)

    def _handle_find_node_response(self, message: dict, addr: Tuple[str, int]):
        """Handle find_node response"""
        try:
            r = message.get(b"r", {})
            nodes = r.get(b"nodes", b"")

            # Add nodes to our routing table
            self._add_nodes_from_compact(nodes)

        except Exception as e:
            self.logger.error("Error handling find_node response: %s", e)

    def _handle_get_peers_response(self, message: dict, addr: Tuple[str, int]):
        """Handle get_peers response"""
        try:
            r = message.get(b"r", {})

            if b"values" in r:
                # Response contains peers
                values = r[b"values"]
                if isinstance(values, list):
                    for peer in values:
                        if isinstance(peer, bytes) and len(peer) == 6:
                            ip = socket.inet_ntoa(peer[:4])
                            port = struct.unpack("!H", peer[4:6])[0]
                            with self.lock:
                                peer_addr = (ip, port)
                                if peer_addr not in self.peers:
                                    self.peers.append(peer_addr)
                                    self.logger.info(
                                        "Discovered peer via DHT: %s:%d", ip, port
                                    )
            elif b"nodes" in r:
                # Response contains nodes
                nodes = r[b"nodes"]
                self._add_nodes_from_compact(nodes)

        except Exception as e:
            self.logger.error("Error handling get_peers response: %s", e)

    def _handle_announce_peer_response(self, message: dict, addr: Tuple[str, int]):
        """Handle announce_peer response"""
        # Nothing to do for now
        pass

    def _bootstrap(self):
        """Bootstrap the DHT by pinging initial nodes"""
        self.logger.info("Bootstrapping with %d nodes", len(self.nodes))
        for node in self.nodes:
            self._send_ping(node)

    def _send_ping(self, node: Tuple[str, int]):
        """Send ping to a DHT node"""
        try:
            t = self._generate_transaction_id()
            message = {b"t": t, b"y": b"q", b"q": b"ping", b"a": {b"id": self.dht_id}}

            self.transactions[t] = {"type": "ping", "node": node}
            self._send_message(message, node)
            self.logger.info("Sent ping to %s", node)

        except Exception as e:
            self.logger.error("Error sending ping to %s: %s", node, e)

    def _send_find_node(self, node: Tuple[str, int], target: bytes):
        """Send find_node to a DHT node"""
        try:
            t = self._generate_transaction_id()
            message = {
                b"t": t,
                b"y": b"q",
                b"q": b"find_node",
                b"a": {b"id": self.dht_id, b"target": target},
            }

            self.transactions[t] = {"type": "find_node", "node": node, "target": target}
            self._send_message(message, node)

        except Exception as e:
            self.logger.error("Error sending find_node to %s: %s", node, e)

    def _send_get_peers(self, node: Tuple[str, int], info_hash: bytes):
        """Send get_peers to a DHT node"""
        try:
            t = self._generate_transaction_id()
            message = {
                b"t": t,
                b"y": b"q",
                b"q": b"get_peers",
                b"a": {b"id": self.dht_id, b"info_hash": info_hash},
            }

            self.transactions[t] = {
                "type": "get_peers",
                "node": node,
                "info_hash": info_hash,
            }
            self._send_message(message, node)

        except Exception as e:
            self.logger.error("Error sending get_peers to %s: %s", node, e)

    def _send_announce_peer(
        self, node: Tuple[str, int], info_hash: bytes, token: bytes
    ):
        """Send announce_peer to a DHT node"""
        try:
            t = self._generate_transaction_id()
            message = {
                b"t": t,
                b"y": b"q",
                b"q": b"announce_peer",
                b"a": {
                    b"id": self.dht_id,
                    b"info_hash": info_hash,
                    b"port": self.port,
                    b"token": token,
                },
            }

            self.transactions[t] = {
                "type": "announce_peer",
                "node": node,
                "info_hash": info_hash,
            }
            self._send_message(message, node)

        except Exception as e:
            self.logger.error("Error sending announce_peer to %s: %s", node, e)

    def _send_message(self, message: dict, addr: Tuple[str, int]):
        """Send a DHT message to an address"""
        try:
            encoded = BencodeEncoder.encode(message)
            self.socket.sendto(encoded, addr)
        except Exception as e:
            self.logger.error("Error sending message to %s: %s", addr, e)

    def _generate_transaction_id(self) -> bytes:
        """Generate a random transaction ID"""
        return os.urandom(2)

    def _generate_node_id(self) -> bytes:
        return os.urandom(20)

    def _generate_token(self, addr: Tuple[str, int]) -> bytes:
        """Generate a token for announce_peer validation"""
        # Simple token generation based on IP and timestamp
        # In a real implementation, this should be more secure
        token_data = f"{addr[0]}:{time.time()}".encode()
        return hashlib.sha1(token_data).digest()[:8]

    def _validate_token(self, token: bytes, addr: Tuple[str, int]) -> bool:
        """Validate an announce_peer token"""
        # Simple validation - in a real implementation, this should be more secure
        expected_token = self._generate_token(addr)
        return token == expected_token

    def _add_node(self, addr: Tuple[str, int]):
        """Add a node to our routing table"""
        if addr not in self.nodes:
            self.logger.info("Added %s to the DHT routing table", addr)
            self.nodes.append(addr)

    def _add_nodes_from_compact(self, nodes: bytes):
        """Add nodes from compact node info format"""
        try:
            for i in range(0, len(nodes), 26):
                if i + 26 > len(nodes):
                    break

                node_info = nodes[i : i + 26]
                node_id = node_info[:20]
                ip = socket.inet_ntoa(node_info[20:24])
                port = struct.unpack("!H", node_info[24:26])[0]

                self._add_node((ip, port))

        except Exception as e:
            self.logger.error("Error adding nodes from compact format: %s", e)

    def _get_closest_nodes(self, target: bytes) -> bytes:
        """Get closest nodes to target in compact format"""
        # Simple implementation - return all known nodes
        # In a real implementation, this should return the closest nodes to the target
        nodes = b""
        for node in self.nodes:
            try:
                ip_bytes = socket.inet_aton(node[0])
                port_bytes = node[1].to_bytes(2, "big")
                # Use a random node ID for now
                node_id = os.urandom(20)
                nodes += node_id + ip_bytes + port_bytes
            except:
                continue

        return nodes

    def _get_peers_for_info_hash(self, info_hash: bytes) -> List[bytes]:
        """Get peers for a given info_hash in compact format"""
        peers = []

        # Check if this is our torrent
        if info_hash == self.torrent.info_hash:
            # Add ourselves as a peer
            try:
                # Get our external IP (simplified)
                # Too simplified!
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.connect(("8.8.8.8", 80))
                our_ip = s.getsockname()[0]
                s.close()

                peer = socket.inet_aton(our_ip) + self.port.to_bytes(2, "big")
                peers.append(peer)
            except:
                pass

        return peers


@logged
class SubnetScannerDriver(PeerDiscovery):
    """Efficient subnet scanner using ICMP for host discovery followed by targeted port scanning"""

    # Common BitTorrent ports to scan
    COMMON_PORTS = [
        6881,
        6882,
        6883,
        6884,
        6885,
        6886,
        6887,
        6888,
        6889,
        6890,
        6969,
        6999,
        8999,
        9999,
        10000,
        10001,
    ]

    # Configuration
    MAX_WORKERS = 6553  # powerful...
    SCAN_INTERVAL = 300  # 5 minutes between full scans
    ICMP_TIMEOUT = 0.1  # ICMP response timeout
    CONNECTION_TIMEOUT = 1  # TCP connection timeout

    def __init__(
        self,
        torrent,
        peer_id,
        port=6881,
        subnet_cidrs: str = None,
        custom_ports: List[int] = None,
    ):
        self.torrent = torrent
        self.peer_id = peer_id
        self.port = port
        self.peers: List[Tuple[str, int]] = []
        self.lock = threading.Lock()
        self.running = False
        self.scan_thread = None
        self.initialized = False
        self.use_icmp = True

        # Network configuration
        self.subnet_cidrs = subnet_cidrs or self._detect_local_subnets()
        if len(self.subnet_cidrs) > 3:
            self.logger.warning(
                f"Too many subnets ({self.subnet_cidrs}). Manually specify the subnets the client should connect to"
            )
            self.subnet_cidrs = [
                "192.168.43.130",
            ]
        self.ports_to_scan = custom_ports or self.COMMON_PORTS

        # Host discovery results
        self.active_hosts: Set[str] = set()

        # Statistics
        self.scan_attempts = 0
        self.successful_discoveries = 0
        self.last_scan_time = 0

        self.logger.info(
            f"ICMP SubnetScanner initialized for subnet(s): {self.subnet_cidrs}"
        )

    def _detect_local_subnets(self) -> List[str]:
        """Use ip command to get all subnets"""
        subnets = set()

        try:
            # Get IPv4 routes
            result = subprocess.run(
                ["ip", "-4", "route", "show"], capture_output=True, text=True, timeout=5
            )

            if result.returncode == 0:
                for line in result.stdout.split("\n"):
                    line = line.strip()
                    # Look for lines with subnet notation
                    if "scope link" in line or "dev" in line:
                        # Extract subnet pattern (x.x.x.x/y)
                        match = re.search(r"(\d+\.\d+\.\d+\.\d+/\d+)", line)
                        if match:
                            subnet = match.group(1)
                            try:
                                # Validate it's a proper network
                                network = ipaddress.IPv4Network(subnet, strict=False)
                                # Skip very large networks that aren't useful for scanning
                                if network.prefixlen >= 16 and not network.is_loopback:
                                    subnets.add(str(network))
                            except (ValueError, ipaddress.NetmaskValueError):
                                continue

            # Get interface information
            result = subprocess.run(
                ["ip", "-4", "addr", "show"], capture_output=True, text=True, timeout=5
            )

            if result.returncode == 0:
                current_interface = None
                for line in result.stdout.split("\n"):
                    line = line.strip()
                    # Interface line
                    if line and not line.startswith(" "):
                        current_interface = (
                            line.split(":")[1].strip() if ":" in line else None
                        )
                    # Inet line with CIDR
                    elif "inet " in line:
                        match = re.search(r"inet (\d+\.\d+\.\d+\.\d+/\d+)", line)
                        if match and current_interface and current_interface != "lo":
                            subnet = match.group(1)
                            try:
                                network = ipaddress.IPv4Network(subnet, strict=False)
                                if not network.is_loopback:
                                    subnets.add(str(network))
                            except (ValueError, ipaddress.NetmaskValueError):
                                continue

        except (
            subprocess.TimeoutExpired,
            subprocess.SubprocessError,
            FileNotFoundError,
        ) as e:
            self.logger.debug(f"Error running ip command: {e}")

        return list(subnets)

    def _detect_local_subnets_fallback(self) -> str:
        """Detect the subnet of the interface acting as a gateway (heuristic)"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(("8.8.8.8", 80))
                local_ip = s.getsockname()[0]

            if local_ip.startswith("10."):
                return [
                    "10.0.0.0/8",
                ]
            elif local_ip.startswith("172.16."):
                return [
                    "172.16.0.0/12",
                ]
            elif local_ip.startswith("192.168."):
                return [
                    "192.168.0.0/16",
                ]
            elif local_ip.startswith("169.254."):
                return [
                    "169.254.0.0/16",
                ]
            else:
                ip_parts = local_ip.split(".")
                return [f"{ip_parts[0]}.{ip_parts[1]}.{ip_parts[2]}.0/24"]

        except Exception as e:
            self.logger.warning(f"Failed to detect local subnet: {e}")
            return [
                "192.168.1.0/24",
            ]

    def _create_icmp_socket(self):
        """Create raw socket for ICMP (requires root privileges on most systems)"""
        try:
            # Try to create raw ICMP socket
            return socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_ICMP)
        except PermissionError:
            self.logger.warning(
                "Raw ICMP socket requires root privileges. Falling back to alternative method."
            )
            return None

    def _send_icmp_echo(
        self, sock: socket.socket, dest_addr: str, icmp_id: int, seq: int
    ) -> bool:
        """Send ICMP echo request to destination"""
        # no error handling
        # we send way to many of these
        # Create ICMP header
        checksum = 0
        header = struct.pack("!BBHHH", 8, 0, checksum, icmp_id, seq)

        # Calculate checksum
        checksum = self._calculate_checksum(header)
        header = struct.pack("!BBHHH", 8, 0, checksum, icmp_id, seq)

        # Send packet
        try:
            sock.sendto(header, (dest_addr, 0))
        except:
            # be happy
            pass

    def _calculate_checksum(self, data: bytes) -> int:
        """Calculate ICMP checksum"""
        if len(data) % 2:
            data += b"\x00"

        checksum = 0
        for i in range(0, len(data), 2):
            word = (data[i] << 8) + data[i + 1]
            checksum += word

        checksum = (checksum >> 16) + (checksum & 0xFFFF)
        checksum = ~checksum & 0xFFFF
        return checksum

    def _discover_active_hosts_icmp(self) -> Set[str]:
        """Discover active hosts in subnet using ICMP echo requests"""
        active_hosts = set()
        hosts_to_ping = []
        for subnet_cidr in self.subnet_cidrs:
            network = ipaddress.ip_network(subnet_cidr, strict=False)
            if len(list(network.hosts())) > 1000:
                self.logger.warning("Rejecting network %s (too big!)", network)
                continue

            # Generate all host IPs in subnet
            hosts_to_ping.extend((str(host) for host in network.hosts()))

        if not self.use_icmp:
            return hosts_to_ping

        try:
            sock = self._create_icmp_socket()
            if sock is None:
                # return all
                return hosts_to_ping

            sock.settimeout(self.ICMP_TIMEOUT)
            icmp_id = os.getpid() & 0xFFFF

            self.logger.info(
                f"Sending ICMP echo requests to {len(hosts_to_ping)} hosts"
            )

            # Send all ICMP echo requests first
            for seq, host in enumerate(hosts_to_ping):
                # we don't care about the result
                flag = False
                while not flag:
                    try:
                        threading.Thread(
                            target=self._send_icmp_echo,
                            # seq can be >= 65535
                            # so it might not fit
                            args=(sock, host, icmp_id, seq % 65535),
                            daemon=True,
                        ).start()
                    except RuntimeError:
                        # too many threads!
                        time.sleep(0.1)
                    else:
                        flag = True

            # Listen for responses
            start_time = time.time()
            while time.time() - start_time < self.ICMP_TIMEOUT * 10:
                ready = select.select([sock], [], [], 0.1)
                if ready[0]:
                    try:
                        packet, addr = sock.recvfrom(1024)
                        ip_header = packet[:20]
                        ip_src = socket.inet_ntoa(ip_header[12:16])

                        # Check if this is an ICMP echo reply
                        if len(packet) >= 28:
                            icmp_header = packet[20:28]
                            icmp_type, code, checksum, pkt_id, seq = struct.unpack(
                                "!BBHHH", icmp_header
                            )

                            if icmp_type == 0 and code == 0 and pkt_id == icmp_id:
                                active_hosts.add(ip_src)
                    except socket.timeout:
                        break
                    except Exception as e:
                        self.logger.debug(f"Error reading ICMP response: {e}")

            sock.close()

        except Exception as e:
            self.logger.error(f"ICMP discovery failed: {e}")
            active_hosts = []

        self.logger.info(f"ICMP discovery found {len(active_hosts)} active hosts")
        return active_hosts

    def _scan_ports_on_active_hosts(
        self, active_hosts: Set[str]
    ) -> List[Tuple[str, int]]:
        """Scan common ports on active hosts discovered via ICMP"""
        discovered_peers = []

        self.logger.info(
            f"Scanning {len(self.ports_to_scan)} ports on {len(active_hosts)} active hosts"
        )

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.MAX_WORKERS
        ) as executor:
            # Submit all port scanning tasks
            future_to_target = {}
            for host in active_hosts:
                for port in self.ports_to_scan:
                    future = executor.submit(self._check_host_port, host, port)
                    future_to_target[future] = (host, port)

            # Collect results
            for future in concurrent.futures.as_completed(future_to_target):
                if not self.running:
                    break

                target = future_to_target[future]
                try:
                    if result := future.result():
                        ip, port = result
                        discovered_peers.append((ip, port))
                        self._add_peer(ip, port)
                except Exception as e:
                    self.logger.debug(f"Port scan failed for {target}: {e}")

        return discovered_peers

    def _check_host_port(self, ip: str, port: int) -> Tuple[str, int]:
        """Check if a specific host:port is a BitTorrent peer"""
        self.scan_attempts += 1

        try:
            # Skip localhost and our own IP
            if ip in ["127.0.0.1", "localhost", "0.0.0.0"]:
                return None

            # Create socket with timeout
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(self.CONNECTION_TIMEOUT)

                # Attempt connection
                result = sock.connect_ex((ip, port))

                if result == 0:
                    # Connection successful - verify it's a BitTorrent peer
                    if self._verify_bittorrent_peer(sock, ip, port):
                        self.logger.info(f"Verified BitTorrent peer: {ip}:{port}")
                        return (ip, port)
                    else:
                        self.logger.debug(f"Port {ip}:{port} open but not BitTorrent")

        except (socket.timeout, ConnectionRefusedError, OSError):
            pass  # Expected for most hosts/ports
        except Exception as e:
            self.logger.debug(f"Error checking {ip}:{port}: {e}")

        return None

    def _verify_bittorrent_peer(self, sock: socket.socket, ip: str, port: int) -> bool:
        """Perform a quick verification to check if this is a BitTorrent peer"""
        try:
            # Send handshake
            handshake = self._create_handshake()
            sock.sendall(handshake)

            # Read response with timeout
            response = sock.recv(68)  # Handshake response length

            if len(response) >= 68:
                # Verify handshake response structure
                return (
                    response[0] == 19
                    and response[1:20] == b"BitTorrent protocol"
                    and response[28:48] == self.torrent.info_hash
                )

        except (socket.timeout, ConnectionResetError, BrokenPipeError):
            pass  # Not a BitTorrent peer or connection failed
        except Exception as e:
            self.logger.debug(f"Handshake failed for {ip}:{port}: {e}")

        return False

    def _create_handshake(self) -> bytes:
        """Create a BitTorrent handshake message"""
        protocol = b"BitTorrent protocol"
        reserved = b"\x00" * 8
        handshake = (
            bytes([len(protocol)])
            + protocol
            + reserved
            + self.torrent.info_hash
            + self.peer_id
        )
        return handshake

    def _perform_scan(self):
        """Perform efficient subnet scan using ICMP discovery first"""
        try:
            self.logger.info(f"Starting efficient subnet scan: {self.subnet_cidrs}")
            start_time = time.time()

            # Phase 1: ICMP host discovery
            active_hosts = self._discover_active_hosts_icmp()

            # Phase 2: Targeted port scanning on active hosts
            discovered_peers = self._scan_ports_on_active_hosts(active_hosts)

            scan_duration = time.time() - start_time
            self.last_scan_time = time.time()
            self.scan_attempts += 1

            self.logger.info(
                f"Efficient scan completed in {scan_duration:.2f}s. "
                f"Found {len(discovered_peers)} new peers from {len(active_hosts)} active hosts"
            )

        except Exception as e:
            self.logger.error(f"Error during subnet scan: {e}")

    def initialize(self) -> bool:
        """Initialize the subnet scanner"""
        # :D
        self.initialized = True
        return True

    def get_peers(self) -> List[Tuple[str, int]]:
        """Get the current list of discovered peers"""
        with self.lock:
            return self.peers.copy()

    def _add_peer(self, ip: str, port: int):
        """Add a discovered peer to the list"""
        with self.lock:
            peer = (ip, port)
            if peer not in self.peers:
                self.peers.append(peer)
                self.successful_discoveries += 1

    def cleanup(self):
        """Clean up scanner resources"""
        self.stop_discovery()
        self.initialized = False
        self.logger.info("SubnetScanner cleaned up")

    def start_discovery(self):
        """Start the subnet scanning process"""
        if not self.initialized:
            self.logger.error("SubnetScanner not initialized")
            return

        if self.running:
            return

        self.running = True
        self.scan_thread = threading.Thread(target=self._scan_loop, daemon=True)
        self.scan_thread.start()
        self.logger.info("SubnetScanner discovery started")

    def stop_discovery(self):
        """Stop the subnet scanning process"""
        self.running = False
        if self.scan_thread:
            self.scan_thread.join(timeout=10)
            self.scan_thread = None
        self.logger.info("SubnetScanner discovery stopped")

    def _scan_loop(self):
        """Main scanning loop"""
        self._perform_scan()
        while self.running:
            time.sleep(self.SCAN_INTERVAL)
            if self.running:
                self._perform_scan()

    def update_stats(self, downloaded: int, uploaded: int, left: int):
        """Update statistics - scanner doesn't use torrent stats"""
        pass

    def get_scan_stats(self) -> dict:
        """Get scanning statistics"""
        return {
            "scan_attempts": self.scan_attempts,
            "successful_discoveries": self.successful_discoveries,
            "last_scan_time": self.last_scan_time,
            "active_hosts_count": len(self.active_hosts),
            "current_peers_count": len(self.peers),
        }

    def set_subnet(self, subnet_cidr: str):
        """Update the subnet to scan"""
        with self.lock:
            self.subnet_cidr = subnet_cidr
            self.logger.info(f"Updated scan subnet to: {subnet_cidr}")

    def add_custom_port(self, port: int):
        """Add a custom port to scan"""
        with self.lock:
            if port not in self.ports_to_scan:
                self.ports_to_scan.append(port)
                self.logger.info(f"Added custom port: {port}")

    def remove_custom_port(self, port: int):
        """Remove a port from scanning"""
        with self.lock:
            if port in self.ports_to_scan:
                self.ports_to_scan.remove(port)
                self.logger.info(f"Removed port: {port}")


@logged
class SmartSubnetScannerDriver(SubnetScannerDriver):
    """Enhanced subnet scanner with intelligent scanning strategies"""

    def __init__(self, torrent, peer_id, port=6881, subnet_cidr: str = None):
        super().__init__(torrent, peer_id, port, subnet_cidr)

        # Smart scanning features
        self.learned_ports: Set[int] = set()  # Ports we've found peers on
        self.active_peers: Set[Tuple[str, int]] = set()  # Recently active peers
        self.failed_hosts: Set[str] = set()  # Hosts that consistently fail
        self.scan_strategy = "aggressive"  # aggressive, conservative, targeted

        # Adaptive scanning
        self.min_scan_interval = 60  # 1 minute minimum
        self.max_scan_interval = 1800  # 30 minutes maximum
        self.current_scan_interval = self.SCAN_INTERVAL

    def _perform_scan(self):
        """Perform smart scanning based on current strategy"""
        if self.scan_strategy == "targeted":
            self._targeted_scan()
        elif self.scan_strategy == "conservative":
            self._conservative_scan()
        else:  # aggressive
            super()._perform_scan()

        self._adjust_scan_strategy()

    def _targeted_scan(self):
        """Scan only known active IP ranges and ports"""
        self.logger.info("Performing targeted scan")

        # Focus on IPs near previously found peers
        target_ips = self._generate_target_ips()
        target_ports = (
            list(self.learned_ports) if self.learned_ports else self.ports_to_scan
        )

        self._scan_targets(target_ips, target_ports)

    def _conservative_scan(self):
        """Slow, careful scanning to avoid network congestion"""
        self.logger.info("Performing conservative scan")

        network = ipaddress.ip_network(self.subnet_cidr, strict=False)
        hosts = list(network.hosts())

        # Sample a subset of hosts
        sample_size = min(50, len(hosts))
        target_ips = [str(host) for host in hosts[:sample_size]]

        # Use only common ports
        target_ports = self.ports_to_scan[:5]  # First 5 common ports

        self._scan_targets(target_ips, target_ports)

    def _generate_target_ips(self) -> List[str]:
        """Generate IP targets based on previously found peers"""
        target_ips = set()

        # Add IPs of known peers
        for ip, port in self.active_peers:
            target_ips.add(ip)

        # Add nearby IPs (same /24 subnet as known peers)
        for ip, port in self.active_peers:
            ip_obj = ipaddress.ip_address(ip)
            network_24 = ipaddress.ip_network(f"{ip}/24", strict=False)
            for host in list(network_24.hosts())[:10]:  # First 10 hosts in /24
                target_ips.add(str(host))

        return list(target_ips)

    def _scan_targets(self, ips: List[str], ports: List[int]):
        """Scan specific IPs and ports"""
        if not ips or not ports:
            return

        self.logger.info(f"Scanning {len(ips)} IPs on {len(ports)} ports")

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(self.MAX_WORKERS, len(ips) * len(ports))
        ) as executor:
            future_to_target = {
                executor.submit(self._check_host_port, ip, port): (ip, port)
                for ip in ips
                for port in ports
                if ip not in self.failed_hosts  # Skip known failed hosts
            }

            for future in concurrent.futures.as_completed(future_to_target):
                if not self.running:
                    break

                target = future_to_target[future]
                try:
                    result = future.result()
                    if result:
                        ip, port = result
                        self._add_peer(ip, port)
                        self.learned_ports.add(port)
                        self.active_peers.add((ip, port))
                except Exception:
                    pass

    def _adjust_scan_strategy(self):
        """Adjust scanning strategy based on results"""
        peer_count = len(self.peers)

        if peer_count == 0:
            # No peers found, be more aggressive
            self.scan_strategy = "aggressive"
            self.current_scan_interval = self.min_scan_interval
        elif peer_count < 5:
            # Few peers, use targeted scanning
            self.scan_strategy = "targeted"
            self.current_scan_interval = self.SCAN_INTERVAL
        else:
            # Many peers, be conservative
            self.scan_strategy = "conservative"
            self.current_scan_interval = self.max_scan_interval

        self.logger.debug(
            f"Adjusted strategy to {self.scan_strategy}, "
            f"interval: {self.current_scan_interval}s"
        )

    def _scan_loop(self):
        """Smart scanning loop with adaptive intervals"""
        while self.running:
            self._perform_scan()

            # Use adaptive interval
            sleep_time = self.current_scan_interval
            self.logger.debug(f"Next scan in {sleep_time}s")

            # Sleep in chunks to allow for quick shutdown
            for _ in range(sleep_time):
                if not self.running:
                    break
                time.sleep(1)

    def set_scan_strategy(self, strategy: str):
        """Manually set scanning strategy"""
        valid_strategies = ["aggressive", "conservative", "targeted"]
        if strategy in valid_strategies:
            self.scan_strategy = strategy
            self.logger.info(f"Scan strategy set to: {strategy}")
        else:
            self.logger.error(f"Invalid scan strategy: {strategy}")

    def get_scan_stats(self) -> dict:
        """Get enhanced scanning statistics"""
        base_stats = super().get_scan_stats()
        base_stats.update(
            {
                "scan_strategy": self.scan_strategy,
                "learned_ports": len(self.learned_ports),
                "active_peers": len(self.active_peers),
                "failed_hosts": len(self.failed_hosts),
                "current_interval": self.current_scan_interval,
            }
        )
        return base_stats


# DRIVERS = [TrackerDriver, LocalPeerDiscoveryDriver, SmartSubnetScannerDriver]
# DRIVERS = [DHTDiscovery,]
# DRIVERS = [LocalPeerDiscoveryDriver,]
DRIVERS = [
    TrackerDriver,
    SmartSubnetScannerDriver,
    # DHTDiscovery,
]
