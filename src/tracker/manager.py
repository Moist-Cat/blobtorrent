import binascii
import time
import socket
import threading
import hashlib
from typing import List, Dict, Tuple, Any
import logging
import random

from tracker.protocol import TrackerInfo, TrackerGossipProtocol
from tracker.connection import TrackerGossipConnection  # New import
from network.connection import PortManager
from log import logged

@logged
class TrackerGossipManager:
    """
    Manages a decentralized tracker swarm using gossip protocol.
    """

    def __init__(
        self,
        hostname: str,
        ip: str,
        port: int,
        tracker_id: bytes,
        seed_hostnames: List[str] = None,
        max_trackers: int = 50,
    ):
        """
        Initialize tracker gossip manager.

        Args:
            hostname: This tracker's hostname
            ip: This tracker's IP address
            port: This tracker's port
            tracker_id: Unique identifier for this tracker
            seed_hostnames: List of seed hostnames for bootstrapping
            max_trackers: Maximum number of trackers to maintain
        """
        # Create swarm identifier from a fixed string
        swarm_str = "blobtorrent-tracker-swarm-v1"
        self.swarm_hash = hashlib.sha1(swarm_str.encode()).digest()

        self.tracker_info = TrackerInfo(
            hostname=hostname,
            ip=ip,
            port=port,
            info_hash=self.swarm_hash,
            peer_id=tracker_id,
            version="1.0",
            timestamp=time.time(),
            uptime=0,
            load=0.0,
            capabilities=["gossip", "http", "peer-exchange"],
        )

        self.tracker_id = tracker_id
        self.seed_hostnames = seed_hostnames or []
        self.max_trackers = max_trackers

        # Track known trackers and connections
        self.known_trackers: Dict[bytes, TrackerInfo] = {}  # peer_id -> TrackerInfo
        self.connections: Dict[bytes, TrackerGossipConnection] = {}  # peer_id -> connection
        self.peer_cache: Dict[bytes, List[Tuple[str, int]]] = {}  # info_hash -> peers

        # Statistics
        self.stats = {
            "messages_sent": 0,
            "messages_received": 0,
            "trackers_discovered": 0,
            "peers_propagated": 0,
            "start_time": time.time(),
        }

        # Thread management
        self.running = False
        self.gossip_thread = None
        self.cleanup_thread = None
        self.server_thread = None

        # Server socket for incoming tracker connections
        self.server_socket = None
        self.server_port = port  # Use separate port for tracker-to-tracker

        # Lock for thread safety
        self.lock = threading.RLock()

        self.logger.info(f"TrackerGossipManager initialized for {hostname}:{port} {tracker_id}")

    def start(self):
        """Start the gossip manager."""
        if self.running:
            return

        self.running = True

        # Bootstrap from seed hostnames
        self._bootstrap_from_dns()

        # Start server for incoming tracker connections
        self._start_server()

        # Start gossip thread
        self.gossip_thread = threading.Thread(target=self._gossip_loop, daemon=True)
        self.gossip_thread.start()

        # Start cleanup thread
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()

        self.logger.info("TrackerGossipManager started")

    def stop(self):
        """Stop the gossip manager."""
        self.running = False

        # Close all connections
        with self.lock:
            for connection in self.connections.values():
                connection.stop()
            self.connections.clear()

        # Close server socket
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass

        self.logger.info("TrackerGossipManager stopped")

    def _bootstrap_from_dns(self):
        """Bootstrap tracker list using DNS resolution of seed hostnames."""
        for hostname in self.seed_hostnames:
            self.logger.info(f"Bootstrapping from seed hostname: {hostname}")
            self.add_tracker_hostname(hostname)

    def _start_server(self):
        """Start server for incoming tracker connections."""

        def server_loop():
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind(("0.0.0.0", self.server_port))
            self.server_socket.listen(5)
            self.server_socket.settimeout(1.0)

            self.logger.info(f"Tracker gossip server listening on port {self.server_port}")

            while self.running:
                try:
                    client_socket, client_addr = self.server_socket.accept()
                    threading.Thread(
                        target=self._handle_incoming_connection,
                        args=(client_socket, client_addr),
                        daemon=True,
                    ).start()
                except socket.timeout:
                    continue
                except Exception as e:
                    if self.running:
                        self.logger.error(f"Error accepting connection: {e}")

        self.server_thread = threading.Thread(target=server_loop, daemon=True)
        self.server_thread.start()

    def _handle_incoming_connection(
        self, client_socket: socket.socket, client_addr: Tuple[str, int]
    ):
        """Handle incoming tracker connection."""
        try:
            self.logger.info(f"New tracker connection from {client_addr}")

            # Create gossip protocol instance
            protocol = TrackerGossipProtocol(
                peer=client_addr, info_hash=self.swarm_hash, peer_id=self.tracker_id
            )

            # Handle the connection
            if protocol.handle_incoming_connection(client_socket, client_addr):
                # Create and start connection handler
                self._create_connection(protocol)
            else:
                client_socket.close()

        except Exception as e:
            self.logger.error(f"Error handling incoming connection from {client_addr}: {e}")
            try:
                client_socket.close()
            except:
                pass

    def _handle_connection_closed(self, peer_id: bytes):
        """Handle when a connection is closed."""
        with self.lock:
            if peer_id in self.connections:
                del self.connections[peer_id]
            if peer_id in self.known_trackers:
                del self.known_trackers[peer_id]
        
        self.logger.info(f"Connection closed for peer {peer_id.hex()[:8]}...")

    def _handle_gossip_message(
        self, protocol: TrackerGossipProtocol, message_id: int, payload: bytes
    ):
        """Handle incoming gossip message."""
        try:
            parsed = protocol.parse_gossip_message(message_id, payload)
            if not parsed:
                return

            peer_id = protocol.peer_id

            if message_id == protocol.GOSSIP_HELLO:
                self._handle_gossip_hello(peer_id, parsed)

            elif message_id == protocol.GOSSIP_PEER_LIST:
                self._handle_gossip_peer_list(parsed)

            elif message_id == protocol.GOSSIP_TRACKER_LIST:
                self._handle_gossip_tracker_list(parsed)

            elif message_id == protocol.GOSSIP_STATS:
                self._handle_gossip_stats(parsed)

            elif message_id == protocol.GOSSIP_PING:
                self._handle_gossip_ping(protocol, parsed)

            elif message_id == protocol.GOSSIP_PONG:
                self._handle_gossip_pong(parsed)

        except Exception as e:
            self.logger.error(f"Error handling gossip message {message_id}: {e}")

    def _handle_gossip_hello(
        self, peer_id: bytes, data: Dict[str, Any]
    ):
        """Handle HELLO message from another tracker."""
        try:
            # Create tracker info
            tracker_info = TrackerInfo.from_dict(data)
            tracker_info.timestamp = time.time()  # Update timestamp

            # Update known trackers
            with self.lock:
                self.known_trackers[peer_id] = tracker_info
                self.stats["trackers_discovered"] += 1

            self.logger.info(
                f"Discovered tracker: {tracker_info.hostname} (v{tracker_info.version})"
            )

            # Send our tracker list back
            if peer_id in self.connections:
                self._send_tracker_list(self.connections[peer_id])

            # Send peer lists for torrents we know about
            if peer_id in self.connections:
                self._send_peer_lists(self.connections[peer_id])

        except Exception as e:
            self.logger.error(f"Error handling HELLO message: {e}")

    def _handle_gossip_peer_list(
        self, data: Dict[str, Any]
    ):
        """Handle peer list from another tracker."""
        try:
            info_hash = data["info_hash"]
            peers = data["peers"]

            with self.lock:
                if info_hash not in self.peer_cache:
                    self.peer_cache[info_hash] = []

                # Add new peers, avoiding duplicates
                existing_peers = set(self.peer_cache[info_hash])
                new_peers = [p for p in peers if p not in existing_peers]

                if new_peers:
                    self.peer_cache[info_hash].extend(new_peers)
                    self.stats["peers_propagated"] += len(new_peers)

                    self.logger.debug(
                        f"Received {len(new_peers)} new peers for {binascii.hexlify(info_hash)[:8]}..."
                    )

        except Exception as e:
            self.logger.error(f"Error handling peer list: {e}")

    def _handle_gossip_tracker_list(
        self, data: Dict[str, Any]
    ):
        """Handle tracker list from another tracker."""
        try:
            trackers = data.get("trackers", [])

            with self.lock:
                for tracker_info in trackers:
                    # Skip ourselves
                    if tracker_info.peer_id == self.tracker_id:
                        continue

                    # Update or add tracker
                    self.known_trackers[tracker_info.peer_id] = tracker_info

                    # Connect to new trackers if we have capacity
                    if (
                        len(self.connections) < self.max_trackers
                        and tracker_info.peer_id not in self.connections
                    ):
                        # Eagerly connect to tracker to avoid busy wait
                        self._connect_to_tracker(tracker_info)

            self.logger.info(
                f"Updated tracker list, now know {len(self.known_trackers)} trackers"
            )

        except Exception as e:
            self.logger.error(f"Error handling tracker list: {e}")

    def _handle_gossip_stats(
        self, data: Dict[str, Any]
    ):
        """Handle statistics from another tracker."""
        # Could aggregate stats or use for load balancing
        pass

    def _handle_gossip_ping(
        self, protocol: TrackerGossipProtocol, data: Dict[str, Any]
    ):
        """Handle PING message."""
        try:
            if protocol.peer_id in self.connections:
                self.connections[protocol.remote_peer_id].send_gossip_pong()
        except Exception as e:
            self.logger.error(f"Error handling PING: {e}")

    def _handle_gossip_pong(
        self, data: Dict[str, Any]
    ):
        """Handle PONG response."""
        # Could calculate latency
        pass

    def _send_tracker_list(self, connection: TrackerGossipConnection):
        """Send our known tracker list to another tracker."""
        try:
            with self.lock:
                trackers = list(self.known_trackers.values())

            # Limit list size
            if len(trackers) > 20:
                trackers = random.sample(trackers, 20)

            connection.send_gossip_tracker_list(trackers)
            self.stats["messages_sent"] += 1

        except Exception as e:
            self.logger.error(f"Error sending tracker list: {e}")

    def _send_peer_lists(self, connection: TrackerGossipConnection):
        """Send peer lists for torrents we know about."""
        try:
            with self.lock:
                for info_hash, peers in self.peer_cache.items():
                    if peers:
                        # Send random subset to avoid large messages
                        if len(peers) > 50:
                            peers_to_send = random.sample(peers, 50)
                        else:
                            peers_to_send = peers

                        connection.send_gossip_peer_list(info_hash, peers_to_send)
                        self.stats["messages_sent"] += 1

        except Exception as e:
            self.logger.error(f"Error sending peer lists: {e}")

    def _gossip_loop(self):
        """Main gossip loop - periodically exchange information with other trackers."""
        while self.running:
            try:
                time.sleep(30)  # Gossip interval

                with self.lock:
                    connections = list(self.connections.values())
                    if not connections:
                        continue

                # Select random tracker to gossip with
                target = random.choice(connections)

                # Update our uptime
                self.tracker_info.uptime = time.time() - self.stats["start_time"]
                self.tracker_info.load = len(connections) / self.max_trackers
                self.tracker_info.timestamp = time.time()

                # Send updated HELLO
                target.send_gossip_hello(self.tracker_info)

                # Send peer lists (random subset)
                self._send_peer_lists(target)

                # Occasionally send tracker list
                if random.random() < (1/len(connections))*0.9:  # Guarantees convergence
                    self._send_tracker_list(target)

                self.logger.debug(f"Gossiped with tracker at {target.protocol.peer}")

            except Exception as e:
                self.logger.error(f"Error in gossip loop: {e}")

    def _cleanup_loop(self):
        """Cleanup loop - remove stale trackers and peers."""
        while self.running:
            try:
                time.sleep(60)  # Cleanup interval

                current_time = time.time()

                with self.lock:
                    # Remove stale trackers (not seen for 30 minutes)
                    stale_trackers = []
                    for peer_id, tracker in self.known_trackers.items():
                        if current_time - tracker.timestamp > 1800:  # 30 minutes
                            stale_trackers.append(peer_id)

                    for peer_id in stale_trackers:
                        del self.known_trackers[peer_id]

                    # Remove stale connections
                    stale_connections = []
                    for peer_id, conn in self.connections.items():
                        if not conn.is_active():
                            stale_connections.append(peer_id)

                    for peer_id in stale_connections:
                        if peer_id in self.connections:
                            self.connections[peer_id].stop()
                            del self.connections[peer_id]

                    # Remove old peer cache entries (1 hour)
                    for info_hash, peers in self.peer_cache.items():
                        # Keep only recent peers (implement timestamp tracking in real implementation)
                        if len(peers) > 1000:
                            self.peer_cache[info_hash] = peers[-500:]  # Keep last 500

                    if stale_trackers or stale_connections:
                        self.logger.info(
                            f"Cleaned up {len(stale_trackers)} trackers, {len(stale_connections)} connections"
                        )

            except Exception as e:
                self.logger.error(f"Error in cleanup loop: {e}")

    def add_peer(self, info_hash: bytes, peer: Tuple[str, int]):
        """Add a peer to the cache for propagation."""
        with self.lock:
            if info_hash not in self.peer_cache:
                self.peer_cache[info_hash] = []

            # Avoid duplicates
            if peer not in self.peer_cache[info_hash]:
                self.peer_cache[info_hash].append(peer)

                # Propagate to connected trackers (random subset)
                connections = list(self.connections.values())
                if connections:
                    target = random.choice(connections)
                    target.send_gossip_peer_list(info_hash, [peer])

    def get_peers(self, info_hash: bytes, count: int = 50) -> List[Tuple[str, int]]:
        """Get peers for a torrent from the distributed cache."""
        with self.lock:
            peers = self.peer_cache.get(info_hash, [])
        
        # Return random subset if we have too many
        if len(peers) > count:
            return random.sample(peers, count)
        return peers

    def add_tracker_hostname(self, hostname: str):
        """Add a new hostname for bootstrapping."""
        if hostname not in self.seed_hostnames:
            self.seed_hostnames.append(hostname)

        # Try to resolve and connect
        threading.Thread(
            target=self._resolve_and_connect_hostname, args=(hostname,), daemon=True
        ).start()

    def _create_connection(self, protocol: TrackerGossipProtocol):
        """Create a new TrackerGossipConnection."""
        if protocol.remote_peer_id in self.connections:
            self.logger.info(f"Dropped duplicate connection to tracker {protocol.peer}")
            protocol.close()
            return
        try:
            connection = TrackerGossipConnection(
                protocol=protocol,
                on_message_callback=self._handle_gossip_message,
                on_closed_callback=self._handle_connection_closed,
                peer_id=self.tracker_id
            )
            
            with self.lock:
                self.connections[protocol.remote_peer_id] = connection
            
            # Start the connection
            connection.start()
            
            # Send initial HELLO
            self.tracker_info.uptime = time.time() - self.stats["start_time"]
            connection.send_gossip_hello(self.tracker_info)
            
            self.logger.info(f"Created connection to tracker {protocol.peer}")
            
        except Exception as e:
            self.logger.error(f"Failed to create connection: {e}")



    def _connect_to_tracker(self, tracker_info: TrackerInfo) -> bool:
        """Connect to another tracker."""
        try:
            self.logger.info(
                f"Connecting to tracker {tracker_info.hostname}:{tracker_info.port}"
            )

            # Create gossip protocol instance
            protocol = TrackerGossipProtocol(
                peer=(tracker_info.ip, tracker_info.port),
                info_hash=self.swarm_hash,
                peer_id=self.tracker_id,
            )

            # Connect and perform handshake
            if protocol.connect():
                # Create and start connection handler
                self._create_connection(protocol)
                return True

        except Exception as e:
            self.logger.error(
                f"Failed to connect to tracker {tracker_info.hostname}:{tracker_info.port}: {e}"
            )

        return False

    def _resolve_and_connect_hostname(self, hostname: str):
        """Resolve hostname and attempt to connect."""
        try:
            # Try various resolution strategies
            hostnames_to_try = [hostname]

            for dns_addr in hostnames_to_try:
                try:
                    name, aliaslist, addresslist = socket.gethostbyname_ex(dns_addr)
                    self.logger.info("%s %s", aliaslist, addresslist)
                    for ip in addresslist:
                        # Create tracker info
                        self.logger.info("Trying to connect to %s from %s", ip, name)
                        tracker_info = TrackerInfo(
                            hostname=name,
                            ip=ip,
                            port=self.server_port,
                            info_hash=self.swarm_hash,
                            peer_id=b"unknown",
                            version="unknown",
                            timestamp=time.time(),
                            uptime=0,
                            load=0.0,
                            capabilities=[],
                        )
                        # Try to connect
                        self._connect_to_tracker(tracker_info)

                except socket.gaierror:
                    continue

        except Exception as e:
            self.logger.error(f"Error resolving hostname {hostname}: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """Get gossip manager statistics."""
        with self.lock:
            connection_stats = {}
            for peer_id, conn in self.connections.items():
                connection_stats[peer_id.hex()[:8]] = conn.get_stats()
            
            return {
                "trackers_known": len(self.known_trackers),
                "trackers_connected": len(self.connections),
                "torrents_tracked": len(self.peer_cache),
                "total_peers": sum(len(peers) for peers in self.peer_cache.values()),
                "messages_sent": self.stats["messages_sent"],
                "messages_received": self.stats["messages_received"],
                "uptime": time.time() - self.stats["start_time"],
                "connections": connection_stats
            }
