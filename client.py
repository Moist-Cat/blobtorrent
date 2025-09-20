import random
import time
import os
import binascii
from filesystem import TorrentFile
from peer_drivers import DRIVERS
from network import PieceManager, PeerDownloader

from log import logged

@logged
class BitTorrentClient:
    def __init__(self, torrent_path: str, output_dir: str = "."):
        self.logger.info(f"Initializing BitTorrentClient with torrent: {torrent_path}")
        self.torrent = TorrentFile(torrent_path)
        self.peer_id = self._generate_peer_id()
        self.output_dir = output_dir
        self.piece_manager = PieceManager(self.torrent, output_dir)

        self.discovery_mechanisms = []
        for driver in DRIVERS:
            driver_instance = driver(self.torrent, self.peer_id, self.piece_manager)
            if driver_instance.initialize():
                self.logger.info("Initialized %s", driver.__name__)
                self.discovery_mechanisms.append(driver_instance)

        self.downloaders = set()

        self.logger.info(f"Client peer ID: {binascii.hexlify(self.peer_id).decode()}")

    def _generate_peer_id(self) -> bytes:
        peer_id = b"-PY0001-" + os.urandom(12)
        self.logger.debug(f"Generated peer ID: {binascii.hexlify(peer_id).decode()}")
        return peer_id

    def start_downloaders(self, peer_list):
        random.shuffle(peer_list)
        # Limit concurrent connections to avoid overwhelming the system
        max_connections = min(50, len(peer_list))
        selected_peers = peer_list[:max_connections]

        for peer in selected_peers:
            downloader = PeerDownloader(
                peer, self.torrent, self.piece_manager, self.peer_id
            )
            downloader.start()
            self.downloaders.add(downloader)
            self.logger.info(f"Started downloader for peer: {peer}")

    def start(self):
        if self.piece_manager.is_complete():
            self.logger.info(f"The files are already downloaded. Nothing to do!")
            return

        self.logger.info(f"Starting download for {self.torrent}")
        start_time = time.time()

        try:
            # Start periodic tracker announcing
            for discovery in self.discovery_mechanisms:
                discovery.start_discovery()

            # Get initial peer list
            all_peers = set()
            for discovery in self.discovery_mechanisms:
                peers = discovery.get_peers()
                all_peers.update(peers)
                self.logger.info(f"Discovered {len(peers)} peers via {discovery.__class__.__name__}")

            while not all_peers:
                self.logger.info("No peers discovered yet, waiting...")
                time.sleep(5)
                for discovery in self.discovery_mechanisms:
                    peers = discovery.get_peers()
                    all_peers.update(peers)
                    self.logger.info(f"Discovered {len(peers)} peers via {discovery.__class__.__name__}")

            self.logger.info(f"Discovered {len(all_peers)} peers in total, starting downloaders")

            # Function to create and start downloaders
            # Start initial downloaders
            self.start_downloaders(peers)

            # Monitor progress and manage downloaders
            last_peer_refresh = time.time()
            #peer_refresh_interval = 300  # Refresh peers every 5 minutes
            peer_refresh_interval = 30

            while not self.piece_manager.is_complete():
                # Check if we need to refresh peers
                if (time.time() - last_peer_refresh > peer_refresh_interval) and len(
                    self.downloaders
                ) < 50:
                    self.logger.info("Refreshing peer list")
                    new_peers = set()

                    for discovery in self.discovery_mechanisms:
                        new_peers.update(discovery.get_peers())
                    
                    # Filter out peers we're already connected to
                    current_peer_ips = {(d.peer[0], d.peer[1]) for d in self.downloaders if d.is_alive()}
                    new_peers = [p for p in new_peers if (p[0], p[1]) not in current_peer_ips]

                    if new_peers:
                        self.logger.info(f"Adding {len(new_peers)} new peers")
                        self.start_downloaders(new_peers)

                    last_peer_refresh = time.time()

                # Clean up finished downloaders
                self.downloaders = {d for d in self.downloaders if d.is_alive()}

                # Log progress
                downloaded = sum(self.piece_manager.downloaded_pieces)
                total = self.torrent.num_pieces
                progress = (downloaded / total) * 100

                if downloaded % 5 == 0 or downloaded == total:  # Log every 5 pieces
                    self.logger.info(
                        f"Download progress: {progress:.1f}% ({downloaded}/{total} pieces)"
                    )

                time.sleep(5)  # Check every 5 seconds

            # Send "completed" event to trackers that support it
            for discovery in self.discovery_mechanisms:
                if hasattr(discovery, 'announce') and callable(getattr(discovery, 'announce')):
                    discovery.announce("completed")

            # Wait for all downloaders to finish
            for downloader in self.downloaders:
                downloader.join(timeout=10)

            download_time = time.time() - start_time
            speed = self.torrent.total_size / download_time / 1024 / 1024  # MB/s
            self.logger.info(
                f"Download completed in {download_time:.2f} seconds ({speed:.2f} MB/s)"
            )

        except KeyboardInterrupt:
            self.logger.info("Download interrupted by user")
            # Send "stopped" event to trackers that support it
            for discovery in self.discovery_mechanisms:
                if hasattr(discovery, 'announce') and callable(getattr(discovery, 'announce')):
                    discovery.announce("stopped")
        except Exception as e:
            self.logger.error(f"Error during download: {e}")
            # Send "stopped" event to trackers that support it
            for discovery in self.discovery_mechanisms:
                if hasattr(discovery, 'announce') and callable(getattr(discovery, 'announce')):
                    discovery.announce("stopped")
        finally:
            # Stop all discovery mechanisms
            for discovery in self.discovery_mechanisms:
                discovery.stop_discovery()
                discovery.cleanup()
            self.piece_manager.close()
            self.logger.info("BitTorrent client shutdown complete")
