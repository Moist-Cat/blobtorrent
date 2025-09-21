import time
from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class ClientStats:
    """Container for client statistics"""

    downloaded: int = 0
    uploaded: int = 0
    left: int = 0
    download_speed: float = 0.0  # bytes per second
    upload_speed: float = 0.0  # bytes per second
    connected_peers: int = 0
    total_peers: int = 0
    start_time: float = 0.0
    last_update: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert stats to dictionary for easy serialization"""
        return {
            "downloaded": self.downloaded,
            "uploaded": self.uploaded,
            "left": self.left,
            "download_speed": self.download_speed,
            "upload_speed": self.upload_speed,
            "connected_peers": self.connected_peers,
            "total_peers": self.total_peers,
            "running_time": time.time() - self.start_time,
        }


class Statistics:
    """Collects and manages client statistics"""

    def __init__(self, total_size: int):
        self.stats = ClientStats(left=total_size)
        self.stats.start_time = time.time()
        self.stats.last_update = time.time()
        self.last_downloaded = 0
        self.last_uploaded = 0

    def update_downloaded(self, downloaded: int):
        """Update downloaded bytes and calculate speed"""
        current_time = time.time()
        time_delta = current_time - self.stats.last_update

        if time_delta > 0:
            self.stats.download_speed = (downloaded - self.last_downloaded) / time_delta

        self.stats.downloaded = downloaded
        self.stats.left = self.stats.left - (downloaded - self.last_downloaded)
        self.last_downloaded = downloaded
        self.stats.last_update = current_time

    def update_uploaded(self, uploaded: int):
        """Update uploaded bytes and calculate speed"""
        current_time = time.time()
        time_delta = current_time - self.stats.last_update

        if time_delta > 0:
            self.stats.upload_speed = (uploaded - self.last_uploaded) / time_delta

        self.stats.uploaded = uploaded
        self.last_uploaded = uploaded
        self.stats.last_update = current_time

    def update_peer_counts(self, connected: int, total: int):
        """Update peer counts"""
        self.stats.connected_peers = connected
        self.stats.total_peers = total

    def get_stats(self) -> ClientStats:
        """Get current statistics"""
        return self.stats

    def get_stats_dict(self) -> Dict[str, Any]:
        """Get statistics as dictionary"""
        return self.stats.to_dict()
