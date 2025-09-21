import time
import os
import binascii
from enum import Enum, auto
from typing import List, Tuple, Set, Dict, Any, Optional
import random
import socket

from filesystem import TorrentFile
from peer_drivers import DRIVERS
from middleware import PieceManager
from network.connection import ConnectionManager, PeerServer, PortManager, ActiveConnection, PassiveConnection
from stats import Statistics
from log import logged


class BitTorrentClientState(Enum):
    INITIALIZING = auto()
    DISCOVERING_PEERS = auto()
    DOWNLOADING = auto()
    SEEDING = auto()
    PAUSED = auto()
    STOPPED = auto()
    ERROR = auto()


@logged
class BitTorrentClient:
    def __init__(
        self,
        torrent_path: str,
        output_dir: str = ".",
        seed_after_download: bool = True,
        port: Optional[int] = None,
    ):
        self.logger.info(f"Initializing BitTorrentClient with torrent: {torrent_path}")

        # Initialize state
        self.state = BitTorrentClientState.INITIALIZING

        # Load torrent file
        self.torrent = TorrentFile(torrent_path)

        # Generate peer ID
        self.peer_id = self._generate_peer_id()
        self.output_dir = output_dir

        # Initialize port management
        self.port_manager = PortManager()
        self.port = self._allocate_port(port)
        if not self.port:
            self.logger.error("Failed to allocate a port")
            self.state = BitTorrentClientState.ERROR
            return

        # Initialize statistics
        self.stats = Statistics(self.torrent.total_size)

        # Initialize piece manager
        self.piece_manager = PieceManager(self.torrent, output_dir)

        # Initialize connection manager
        self.connection_manager = ConnectionManager()

        # Configuration
        self.seed_after_download = seed_after_download
        self.seeding = False

        # Initialize peer server for incoming connections
        self.peer_server = PeerServer(
            self.port,
            self.torrent,
            self.peer_id,
            self.piece_manager,
            self.connection_manager,
            # will change later
            False,
        )

        # Initialize discovery mechanisms
        self.discovery_mechanisms = []
        for driver in DRIVERS:
            try:
                driver_instance = driver(self.torrent, self.peer_id, self.port)
                if driver_instance.initialize():
                    self.logger.info("Initialized %s", driver.__name__)
                    self.discovery_mechanisms.append(driver_instance)
                else:
                    self.logger.warning("Failed to initialize %s", driver.__name__)
            except Exception as e:
                self.logger.error("Error initializing %s: %s", driver.__name__, e)

        # Update state
        self.state = BitTorrentClientState.DISCOVERING_PEERS
        self.logger.info(f"Client initialized with port {self.port}")
        self.logger.info(f"Client peer ID: {binascii.hexlify(self.peer_id).decode()}")

    def _allocate_port(self, preferred_port: Optional[int] = None) -> Optional[int]:
        """Allocate a port for the client to use"""
        port = self.port_manager.allocate_port(preferred_port)
        if port:
            self.logger.info(f"Allocated port {port}")
        else:
            self.logger.error("No available ports in range")
        return port

    def _generate_peer_id(self) -> bytes:
        peer_id = b"-PY0001-" + os.urandom(12)
        self.logger.debug(f"Generated peer ID: {binascii.hexlify(peer_id).decode()}")
        return peer_id

    def _update_stats(self):
        """Update client statistics"""
        # Update downloaded bytes
        downloaded = self.piece_manager.get_total_downloaded()
        self.stats.update_downloaded(downloaded)

        # Update uploaded bytes (if tracking is implemented)
        # uploaded = self.connection_manager.get_total_uploaded()
        # self.stats.update_uploaded(uploaded)

        # Update peer counts
        connected = self.connection_manager.get_connection_count()
        total = len(self._get_all_peers())
        self.stats.update_peer_counts(connected, total)

        # Log stats periodically
        if int(time.time()) % 10 == 0:  # Log every 10 seconds
            stats = self.stats.get_stats_dict()
            self.logger.info(
                f"Stats: Downloaded: {stats['downloaded']} bytes, "
                f"Speed: {stats['download_speed']/1024:.2f} KB/s, "
                f"Peers: {stats['connected_peers']}/{stats['total_peers']}, "
                f"State: {self.state}"
            )

    def _get_all_peers(self) -> Set[Tuple[str, int]]:
        """Get all known peers from all discovery mechanisms"""
        all_peers = set()
        for discovery in self.discovery_mechanisms:
            try:
                peers = discovery.get_peers()
                all_peers.update(peers)
            except Exception as e:
                self.logger.error(
                    f"Error getting peers from {discovery.__class__.__name__}: {e}"
                )
        return all_peers

    def _start_downloading(self, peers: List[Tuple[str, int]]):
        """Start downloading from discovered peers"""
        self.logger.info(f"Starting downloading with {len(peers)} peers")
        self.state = BitTorrentClientState.DOWNLOADING

        # Create active connections to peers
        for peer in peers:
            connection = ActiveConnection(
                peer, self.torrent, self.piece_manager, self.peer_id, is_seeder=False
            )
            if self.connection_manager.add_connection(connection):
                connection.start()
                self.logger.info(f"Started download connection to {peer}")

    def _start_seeding(self, peers: List[Tuple[str, int]]):
        """Start seeding to discovered peers"""
        self.logger.info(f"Starting seeding with {len(peers)} peers")
        self.state = BitTorrentClientState.SEEDING
        self.seeding = True

        # Update discovery mechanisms to indicate we're a seeder
        for discovery in self.discovery_mechanisms:
            if hasattr(discovery, "update_stats") and callable(
                getattr(discovery, "update_stats")
            ):
                try:
                    discovery.update_stats(
                        downloaded=self.torrent.total_size,
                        uploaded=0,  # We'll track this later if needed
                        left=0,  # We have all pieces
                    )
                except Exception as e:
                    self.logger.error(
                        f"Error updating stats for {discovery.__class__.__name__}: {e}"
                    )

        # Create active connections to peers for seeding
        for peer in peers:
            connection = ActiveConnection(
                peer, self.torrent, self.piece_manager, self.peer_id, is_seeder=True
            )
            if self.connection_manager.add_connection(connection):
                connection.start()
                self.logger.info(f"Started seed connection to {peer}")

    def _transition_to_seeding(self):
        """Transition from downloading to seeding state"""
        if not self.piece_manager.is_complete():
            self.logger.warning("Cannot transition to seeding: download not complete")
            return
        
        self.connection_manager.close_all()
        # less seeding connections
        self.connection_manager.max_connections = 20
        # we could reload but I chose not to
        self.peer_server.stop()
        self.peer_server = PeerServer(
            self.port,
            self.torrent,
            self.peer_id,
            self.piece_manager,
            self.connection_manager,
            # will change later
            True,
        )

        self.logger.info("Download complete, transitioning to seeding")

        # Send "completed" event to trackers that support it
        for discovery in self.discovery_mechanisms:
            if hasattr(discovery, "announce") and callable(
                getattr(discovery, "announce")
            ):
                try:
                    discovery.announce("completed")
                except Exception as e:
                    self.logger.error(
                        f"Error sending completed event to {discovery.__class__.__name__}: {e}"
                    )

        # Get peers for seeding
        peers = list(self._get_all_peers())
        self._start_seeding(peers)

    def start(self):
        """Start the client"""
        if self.state == BitTorrentClientState.ERROR:
            self.logger.error("Cannot start client in error state")
            return

        self.logger.info(f"Starting client in state: {self.state.name}")

        try:
            # Start peer server for incoming connections
            self.peer_server.start()

            # Start all discovery mechanisms
            for discovery in self.discovery_mechanisms:
                try:
                    discovery.start_discovery()
                except Exception as e:
                    self.logger.error(
                        f"Error starting {discovery.__class__.__name__}: {e}"
                    )

            # Main client loop
            while self.state not in {
                BitTorrentClientState.STOPPED,
                BitTorrentClientState.ERROR,
            }:
                # Update statistics
                self._update_stats()

                # State-specific processing
                if self.state == BitTorrentClientState.DISCOVERING_PEERS:
                    self._handle_discovering_state()
                elif self.state == BitTorrentClientState.DOWNLOADING:
                    self._handle_downloading_state()
                elif self.state == BitTorrentClientState.SEEDING:
                    self._handle_seeding_state()
                elif self.state == BitTorrentClientState.PAUSED:
                    self._handle_paused_state()

                # Clean up finished connections
                self.connection_manager.cleanup_finished()

                # Sleep to prevent busy waiting
                time.sleep(1)

        except KeyboardInterrupt:
            self.logger.info("Client interrupted by user")
            self.state = BitTorrentClientState.STOPPED
        except Exception as e:
            self.logger.fatal(f"Error in client: {e}")
            self.state = BitTorrentClientState.ERROR
        finally:
            self._cleanup()

    def _handle_discovering_state(self):
        """Handle the discovering peers state"""
        # Get peers from all discovery mechanisms
        peers = list(self._get_all_peers())

        if peers:
            self.logger.info(f"Discovered {len(peers)} peers, starting download")

            if self.piece_manager.is_complete():
                self._transition_to_seeding()
            else:
                self._start_downloading(peers)
        else:
            self.logger.info("No peers discovered yet, continuing discovery")
            time.sleep(5)

    def _handle_downloading_state(self):
        """Handle the downloading state"""
        # Check if download is complete
        if self.piece_manager.is_complete():
            if self.seed_after_download:
                self._transition_to_seeding()
            else:
                self.logger.info("Download complete, not seeding")
                self.state = BitTorrentClientState.STOPPED
            return

        # Refresh peers periodically
        if int(time.time()) % 30 == 0:  # Every 5 minutes
            peers = list(self._get_all_peers())
            for peer in peers:
                if self.connection_manager.already_connected_to(peer):
                    continue
                self.logger.info(f"Adding {peer} to new peers")
                connection = ActiveConnection(
                    peer,
                    self.torrent,
                    self.piece_manager,
                    self.peer_id,
                    is_seeder=False,
                )
                if self.connection_manager.add_connection(connection):
                    connection.start()
                    self.logger.info(f"Started download connection to {peer}")

    def _handle_seeding_state(self):
        """Handle the seeding state"""
        # Refresh peers periodically
        if int(time.time()) % 30 == 0:  # Every 5 minutes
            peers = list(self._get_all_peers())
            for peer in peers:
                if self.connection_manager.already_connected_to(peer):
                    continue
                self.logger.info(f"Adding {peer} to new peers")
                connection = ActiveConnection(
                    peer,
                    self.torrent,
                    self.piece_manager,
                    self.peer_id,
                    is_seeder=True,
                )
                if self.connection_manager.add_connection(connection):
                    connection.start()
                    self.logger.info(f"Started seed connection to {peer}")

    def _handle_paused_state(self):
        """Handle the paused state"""
        # Just wait in paused state
        time.sleep(1)

    def pause(self):
        # XXX not implemented yet
        """Pause the client"""
        if self.state in [
            BitTorrentClientState.DOWNLOADING,
            BitTorrentClientState.SEEDING,
        ]:
            self.logger.info("Pausing client")
            self.previous_state = self.state
            self.state = BitTorrentClientState.PAUSED

            # Pause all connections
            for connection in self.active_connections:
                connection.pause()

            # Pause discovery mechanisms
            for discovery in self.discovery_mechanisms:
                if hasattr(discovery, "pause") and callable(
                    getattr(discovery, "pause")
                ):
                    try:
                        discovery.pause()
                    except Exception as e:
                        self.logger.error(
                            f"Error pausing {discovery.__class__.__name__}: {e}"
                        )

    def resume(self):
        # XXX not implemented yet
        """Resume the client"""
        if self.state == BitTorrentClientState.PAUSED:
            self.logger.info("Resuming client")
            self.state = self.previous_state

            # Resume all connections
            for connection in self.active_connections:
                connection.resume()

            # Resume discovery mechanisms
            for discovery in self.discovery_mechanisms:
                if hasattr(discovery, "resume") and callable(
                    getattr(discovery, "resume")
                ):
                    try:
                        discovery.resume()
                    except Exception as e:
                        self.logger.error(
                            f"Error resuming {discovery.__class__.__name__}: {e}"
                        )

    def stop(self):
        """Stop the client"""
        self.logger.info("Stopping client")
        self.state = BitTorrentClientState.STOPPED

    def _cleanup(self):
        """Clean up all resources"""
        self.logger.info("Cleaning up client resources")

        # Stop all connections
        self.connection_manager.close_all()

        # Stop peer server
        self.peer_server.stop()

        # Stop all discovery mechanisms
        for discovery in self.discovery_mechanisms:
            try:
                if hasattr(discovery, "stop_discovery") and callable(
                    getattr(discovery, "stop_discovery")
                ):
                    discovery.stop_discovery()
                if hasattr(discovery, "cleanup") and callable(
                    getattr(discovery, "cleanup")
                ):
                    discovery.cleanup()
                if hasattr(discovery, "announce") and callable(
                    getattr(discovery, "announce")
                ):
                    discovery.announce("stopped")
            except Exception as e:
                self.logger.error(
                    f"Error cleaning up {discovery.__class__.__name__}: {e}"
                )

        # Close piece manager
        self.piece_manager.close()

        # Release port
        if self.port:
            self.port_manager.release_port(self.port)

        self.logger.info("Client cleanup complete")

    def get_stats(self) -> Dict[str, Any]:
        """Get current client statistics"""
        return self.stats.get_stats_dict()

    def get_state(self) -> BitTorrentClientState:
        """Get current client state"""
        return self.state
