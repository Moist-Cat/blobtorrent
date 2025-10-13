from datetime import datetime
import time
from dataclasses import dataclass
from typing import Dict, Any


from dataclasses import dataclass, field
from datetime import datetime
import time
from typing import Dict, Any


@dataclass
class ClientStats:
    """Container for client statistics for a torrent download/upload session"""

    info_hash: bytes
    name: str
    size_bytes: int
    completed_bytes: int = 0
    down_total: int = 0
    up_total: int = 0
    left: int = 0
    down_rate: float = 0.0  # bytes per second
    up_rate: float = 0.0  # bytes per second
    ratio: float = 0.0
    peers_connected: int = 0
    peers_accounted: int = 0
    status: int = 0
    message: str = ""
    added_time: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d"))
    total_peers: int = 0
    start_time: float = field(default_factory=time.time)
    last_update: float = field(default_factory=time.time)

    @property
    def downloaded(self) -> int:
        """Total downloaded bytes"""
        return self.down_total

    @property
    def uploaded(self) -> int:
        """Total uploaded bytes"""
        return self.up_total

    @property
    def download_speed(self) -> float:
        """Current download speed in bytes/sec"""
        return self.down_rate

    @property
    def upload_speed(self) -> float:
        """Current upload speed in bytes/sec"""
        return self.up_rate

    @property
    def connected_peers(self) -> int:
        """Number of currently connected peers"""
        return self.peers_connected

    @property
    def progress(self) -> float:
        """Download progress as percentage (0.0 to 1.0)"""
        if self.size_bytes == 0:
            return 0.0
        return (self.size_bytes - self.left) / self.size_bytes

    @property
    def running_time(self) -> float:
        """Total running time in seconds"""
        return time.time() - self.start_time

    def to_dict(self) -> Dict[str, Any]:
        """Convert stats to dictionary for easy serialization"""
        return {
            "info_hash": self.info_hash,
            "name": self.name,
            "size_bytes": self.size_bytes,
            "progress": self.progress,
            "downloaded": self.downloaded,
            "uploaded": self.uploaded,
            "left": self.left,
            "download_speed": self.download_speed,
            "upload_speed": self.upload_speed,
            "ratio": self.ratio,
            "connected_peers": self.connected_peers,
            "total_peers": self.total_peers,
            "running_time": self.running_time,
            "status": self.status,
            "message": self.message,
            "added_time": self.added_time,
            "last_update": self.last_update,
        }

    def update_timestamp(self) -> None:
        """Update the last_update timestamp to current time"""
        self.last_update = time.time()

    def calculate_ratio(self) -> None:
        """Calculate and update the share ratio"""
        if self.down_total > 0:
            self.ratio = round(self.up_total / self.down_total, 2)
        else:
            self.ratio = 0.0


class Statistics:
    """Collects and manages client statistics"""

    def __init__(self, torrent):
        self.stats = ClientStats(
            info_hash=torrent.info_hash,
            name=torrent.name,
            size_bytes=torrent.total_size,
            left=torrent.total_size,
        )
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
