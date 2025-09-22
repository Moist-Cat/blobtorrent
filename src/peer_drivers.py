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
from typing import List, Tuple, Dict, Set, Optional
import threading
import hashlib

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

    def announce(self, event: str = "") -> List[Tuple[str, int]]:
        """Announce to the tracker with optional event"""
        if not self.initialized:
            self.logger.error("Tracker not initialized")
            return []
            
        if not self.announce_urls:
            self.logger.error("No announce URLs available")
            return []

        url = self.get_current_announce_url()
        self.logger.info(f"Announcing to tracker: {url} (event: {event})")

        parsed = urllib.parse.urlparse(url)
        query_params = {
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
            if parsed.scheme.startswith("http"):
                self.logger.debug(
                    f"Sending tracker request with params: {query_params}"
                )
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

                # Update our peer list
                with self.lock:
                    self.peers = peer_list

                self.logger.info(
                    f"Successfully discovered {len(peer_list)} peers from tracker"
                )
                self.last_announce = time.time()
                return peer_list
            else:
                self.logger.error(f"Unsupported tracker scheme: {parsed.scheme}")
                self.rotate_announce_url()
                return []
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

    def initialize(self) -> bool:
        """Initialize the tracker connection"""
        try:
            # Test the first announce URL to ensure it's valid
            if self.announce_urls:
                test_url = self.announce_urls[0]
                parsed = urllib.parse.urlparse(test_url)
                if not parsed.scheme.startswith("http"):
                    self.logger.warning(f"Unsupported tracker scheme: {parsed.scheme}")
                    return False
                    
                self.initialized = True
                self.logger.info("TrackerDriver initialized successfully")
                return True
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
    LPD_ANNOUNCE_INTERVAL = 300  # 5 minutes as per specification

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
            #self.socket.bind(("", self.LPD_MULTICAST_PORT))
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
    
    def __init__(self, torrent, peer_id, port=6881, dht_port=7882):
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
        self.lock = threading.Lock()
        self.initialized = False
        self.transactions = {}  # Track ongoing transactions

        #self.bootstrap_nodes = bootstrap_nodes or [
        #    ("blobtorrent-node", 6881),
        #]
        self.bootstrap_nodes = [
            ("router.bittorrent.com", 6881),
            ("dht.transmissionbt.com", 6881),
            ("router.utorrent.com", 6881),
            ("dht.aelitis.com", 6881)
        ]
        
    def initialize(self) -> bool:
        """Initialize DHT by creating a UDP socket and joining the DHT network"""
        try:
            # Create UDP socket for DHT
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.socket.bind(("0.0.0.0", self.dht_port))
            self.socket.settimeout(1.0)
            
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
                self.logger.warning("DHT error from %s: %d - %s", addr, error_code, error_msg)
                
        except Exception as e:
            self.logger.error("Error handling DHT error from %s: %s", addr, e)
            
    def _handle_ping_query(self, a: dict, t: bytes, addr: Tuple[str, int]):
        """Handle ping query"""
        try:
            # Send ping response
            response = {
                b"t": t,
                b"y": b"r",
                b"r": {
                    b"id": self.dht_id
                }
            }
            
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
                    b"nodes": self._get_closest_nodes(a.get(b"target", b""))
                }
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
                    b"r": {
                        b"id": self.dht_id,
                        b"values": peers
                    }
                }
            else:
                # No peers, return closest nodes
                response = {
                    b"t": t,
                    b"y": b"r",
                    b"r": {
                        b"id": self.dht_id,
                        b"nodes": self._get_closest_nodes(info_hash),
                        b"token": self._generate_token(addr)
                    }
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
                    self.logger.info("Discovered peer via DHT announce: %s:%d", peer_ip, peer_port)
                    
            # Send response
            response = {
                b"t": t,
                b"y": b"r",
                b"r": {
                    b"id": self.dht_id
                }
            }
            
            self._send_message(response, addr)
            
        except Exception as e:
            self.logger.error("Error handling announce_peer query: %s", e)
            
    def _handle_ping_response(self, message: dict, addr: Tuple[str, int]):
        """Handle ping response"""
        # Add node to our routing table
        self._add_node(addr)
        
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
                                    self.logger.info("Discovered peer via DHT: %s:%d", ip, port)
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
        for node in self.bootstrap_nodes:
            self._send_ping(node)
            
    def _send_ping(self, node: Tuple[str, int]):
        """Send ping to a DHT node"""
        try:
            t = self._generate_transaction_id()
            message = {
                b"t": t,
                b"y": b"q",
                b"q": b"ping",
                b"a": {
                    b"id": self.dht_id
                }
            }
            
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
                b"a": {
                    b"id": self.dht_id,
                    b"target": target
                }
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
                b"a": {
                    b"id": self.dht_id,
                    b"info_hash": info_hash
                }
            }
            
            self.transactions[t] = {"type": "get_peers", "node": node, "info_hash": info_hash}
            self._send_message(message, node)
            
        except Exception as e:
            self.logger.error("Error sending get_peers to %s: %s", node, e)
            
    def _send_announce_peer(self, node: Tuple[str, int], info_hash: bytes, token: bytes):
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
                    b"token": token
                }
            }
            
            self.transactions[t] = {"type": "announce_peer", "node": node, "info_hash": info_hash}
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
            self.nodes.append(addr)
            
    def _add_nodes_from_compact(self, nodes: bytes):
        """Add nodes from compact node info format"""
        try:
            for i in range(0, len(nodes), 26):
                if i + 26 > len(nodes):
                    break
                    
                node_info = nodes[i:i+26]
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
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.connect(("8.8.8.8", 80))
                our_ip = s.getsockname()[0]
                s.close()
                
                peer = socket.inet_aton(our_ip) + self.port.to_bytes(2, "big")
                peers.append(peer)
            except:
                pass
                
        return peers

#DRIVERS = [TrackerDriver, LocalPeerDiscoveryDriver, DHTDiscovery]
DRIVERS = [DHTDiscovery,]
