"""
Drivers for protocols that discover new peers in a network
"""
from abc import ABC, abstractmethod
from typing import List, Tuple, Optional
import threading
import urllib.parse
import socket
import struct
import time
import requests
import binascii

from filesystem import BencodeDecoder
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

@logged
class TrackerDriver(PeerDiscovery):
    def __init__(self, torrent: "TorrentFile", peer_id: bytes, piece_manager: "PieceManager", port: int = 6881):
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
        self.piece_manager = piece_manager

        # Get all announce URLs from the torrent
        self.announce_urls = self._get_announce_urls()
        self.current_announce_url_index = 0

        self.logger.info(
            f"Tracker initialized for {torrent} with {len(self.announce_urls)} announce URLs"
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
            self.logger.info(f"Rotated to announce URL: {self.get_current_announce_url()}")

    def update_stats(self, downloaded: int, uploaded: int, left: int):
        """Update the tracker statistics"""
        with self.lock:
            self.downloaded = downloaded
            self.uploaded = uploaded
            self.left = left

    def announce(self, event: str = "") -> List[Tuple[str, int]]:
        """Announce to the tracker with optional event"""
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

    def start_announcing(self, piece_manager: "PieceManager"):
        """Start periodic announcing to the tracker"""
        if self.announce_thread and self.announce_thread.is_alive():
            self.logger.warning("Announce thread is already running")
            return

        self._stop_announcing = False

        def announce_loop():
            # Initial announce with "started" event
            self.announce("started")

            while not self._stop_announcing:
                try:
                    # Update stats from piece manager
                    downloaded = piece_manager.get_total_downloaded()
                    uploaded = 0  # We're not uploading for now
                    left = self.torrent.total_size - downloaded
                    self.update_stats(downloaded, uploaded, left)

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

    def stop_announcing(self):
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

    def initialize(self) -> bool:
        """Initialize the tracker connection"""
        # For HTTP trackers, initialization is always successful
        return True
        
    def cleanup(self):
        """Clean up tracker resources"""
        self.stop_announcing()
        
    def start_discovery(self):
        """Start the tracker discovery process"""
        self.start_announcing(self.piece_manager)
        
    def stop_discovery(self):
        """Stop the tracker discovery process"""
        self.stop_announcing()

@logged
class LocalPeerDiscovery(PeerDiscovery):
    """Local Peer Discovery implementation for LAN environments"""
    
    # LPD multicast address and port as per BEP 14
    LPD_MULTICAST_ADDR = "239.192.152.143"
    LPD_MULTICAST_PORT = 6771
    LPD_ANNOUNCE_INTERVAL = 300  # 5 minutes as per specification
    
    def __init__(self, torrent, peer_id, piece_manager, port=6881):
        self.torrent = torrent
        self.peer_id = peer_id
        self.port = port
        self.peers: List[Tuple[str, int]] = []
        self.socket = None
        self.running = False
        self.discovery_thread = None
        self.announce_thread = None
        self.piece_manager = piece_manager
        self.lock = threading.Lock()
    
    def initialize(self) -> bool:
        """Initialize LPD by creating a multicast socket"""
        try:
            # Create UDP socket
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            # Bind to the LPD port
            self.socket.bind(('', self.LPD_MULTICAST_PORT))
            
            # Add socket to multicast group
            mreq = struct.pack("4sl", socket.inet_aton(self.LPD_MULTICAST_ADDR), socket.INADDR_ANY)
            self.socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            
            # Set socket timeout to allow for graceful shutdown
            self.socket.settimeout(1.0)
            
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
        self.logger.info("LPD cleaned up")
    
    def start_discovery(self):
        """Start the LPD discovery process"""
        if self.running:
            return
            
        self.running = True
        
        # Start thread for receiving announcements
        self.discovery_thread = threading.Thread(target=self._discovery_loop, daemon=True)
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
            ).encode('utf-8')
            
            self.socket.sendto(message, (self.LPD_MULTICAST_ADDR, self.LPD_MULTICAST_PORT))
            self.logger.debug("Sent LPD announcement")
            
        except Exception as e:
            self.logger.error(f"Failed to send LPD announcement: {e}")
    
    def _process_announcement(self, data: bytes, source_ip: str):
        """Process an incoming LPD announcement"""
        try:
            message = data.decode('utf-8')
            lines = message.split('\r\n')
            
            # Parse the announcement
            port = None
            infohash = None
            
            for line in lines:
                if line.startswith('Port:'):
                    port = int(line.split(':')[1].strip())
                elif line.startswith('Infohash:'):
                    infohash = line.split(':')[1].strip()
            
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

DRIVERS = [TrackerDriver, LocalPeerDiscovery]
