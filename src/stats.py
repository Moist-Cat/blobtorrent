from datetime import datetime
import time
from dataclasses import dataclass
from typing import Dict, Any


from dataclasses import dataclass, field
from datetime import datetime
import time
from typing import Dict, Any

import time
from dataclasses import dataclass, field
from typing import Dict, Any
from datetime import datetime

@dataclass
class ClientStats:
    """Container for client statistics compatible with Flood expectations"""

    # Basic torrent info
    info_hash: bytes
    name: str
    size_bytes: int

    # Progress and transfer stats
    completed_bytes: int = 0
    down_total: int = 0
    up_total: int = 0
    left_bytes: int = 0
    down_rate: float = 0.0
    up_rate: float = 0.0
    ratio: float = 0.0

    # Peer information
    peers_connected: int = 0
    peers_accounted: int = 0
    peers_not_connected: int = 0
    peers_complete: int = 0  # seeders

    # State and status
    state: int = 0  # rTorrent state code
    message: str = ""
    is_active: bool = False
    is_complete: bool = False
    is_hash_checking: bool = False

    # File information
    size_chunks: int = 0
    completed_chunks: int = 0
    chunk_size: int = 16384

    # Additional fields
    download_path: str = ""
    base_path: str = ""
    base_filename: str = ""
    directory: str = ""
    creation_date: float = field(default_factory=time.time)
    added_time: float = field(default_factory=time.time)
    last_activity: float = field(default_factory=time.time)

    # Trackers
    tracker_count: int = 0

    # Speed calculation
    last_downloaded: int = 0
    last_uploaded: int = 0
    last_update: float = field(default_factory=time.time)
    start_time: float = field(default_factory=time.time)

    @property
    def progress(self) -> float:
        """Download progress as percentage (0.0 to 1.0)"""
        if self.size_bytes == 0:
            return 0.0
        return min(1.0, self.completed_bytes / self.size_bytes)

    @property
    def downloaded(self) -> int:
        return self.down_total

    @property
    def uploaded(self) -> int:
        return self.up_total

    @property
    def download_speed(self) -> float:
        return self.down_rate

    @property
    def upload_speed(self) -> float:
        return self.up_rate

    @property
    def connected_peers(self) -> int:
        return self.peers_connected

    @property
    def total_peers(self) -> int:
        return self.peers_accounted

    @property
    def running_time(self) -> float:
        return time.time() - self.start_time

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API consumption"""
        return {
            # Basic info
            "info_hash": self.info_hash.hex() if isinstance(self.info_hash, bytes) else self.info_hash,
            "name": self.name,
            "size_bytes": self.size_bytes,

            # Progress
            "completed_bytes": self.completed_bytes,
            "down_total": self.down_total,
            "up_total": self.up_total,
            "left_bytes": self.left_bytes,
            "progress": self.progress,

            # Speed and ratio
            "down_rate": self.down_rate,
            "up_rate": self.up_rate,
            "ratio": self.ratio,

            # Peers
            "peers_connected": self.peers_connected,
            "peers_accounted": self.peers_accounted,
            "peers_not_connected": self.peers_not_connected,
            "peers_complete": self.peers_complete,

            # State
            "state": self._get_state_string(),
            "state_code": self.state,
            "message": self.message,
            "is_active": self.is_active,
            "is_complete": self.is_complete,
            "is_hash_checking": self.is_hash_checking,

            # File info
            "size_chunks": self.size_chunks,
            "completed_chunks": self.completed_chunks,
            "chunk_size": self.chunk_size,

            # Paths
            "download_path": self.download_path,
            "base_path": self.base_path,
            "base_filename": self.base_filename,
            "directory": self.directory,

            # Timestamps
            "creation_date": self.creation_date,
            "added_time": self.added_time,
            "last_activity": self.last_activity,
            "start_time": self.start_time,
            "last_update": self.last_update,

            # Additional
            "tracker_count": self.tracker_count,
        }

    def _get_state_string(self) -> str:
        """Convert state code to string"""
        state_map = {
            0: "stopped",
            1: "downloading",
            2: "seeding",
            4: "error",
            8: "checking"
        }
        return state_map.get(self.state, "stopped")

    def update_timestamp(self):
        self.last_update = time.time()

    def calculate_ratio(self):
        if self.down_total > 0:
            self.ratio = round(self.up_total / self.down_total, 2)
        else:
            self.ratio = 0.0

    def update_speeds(self, current_time: float):
        """Calculate current download/upload speeds"""
        time_delta = current_time - self.last_update
        if time_delta > 0:
            # Download speed
            download_delta = self.down_total - self.last_downloaded
            self.down_rate = download_delta / time_delta

            # Upload speed
            upload_delta = self.up_total - self.last_uploaded
            self.up_rate = upload_delta / time_delta

            # Update last values
            self.last_downloaded = self.down_total
            self.last_uploaded = self.up_total

class Statistics:
    """Enhanced statistics manager with proper speed calculation"""

    def __init__(self, torrent, download_path: str = ""):
        self.stats = ClientStats(
            info_hash=torrent.info_hash,
            name=torrent.name,
            size_bytes=torrent.total_size,
            left_bytes=torrent.total_size,
            size_chunks=len(torrent.piece_hashes),
            chunk_size=torrent.piece_length,
            download_path=download_path,
            base_path=download_path,
            base_filename=torrent.name,
            directory=download_path,
        )
        self.last_downloaded = 0
        self.last_uploaded = 0

    def update_downloaded(self, downloaded: int):
        """Update downloaded bytes"""
        current_time = time.time()
        self.stats.update_speeds(current_time)

        self.stats.down_total = downloaded
        self.stats.completed_bytes = downloaded
        self.stats.left_bytes = max(0, self.stats.size_bytes - downloaded)
        self.stats.completed_chunks = int((downloaded / self.stats.size_bytes) * self.stats.size_chunks)

        # Update speeds
        self.stats.update_speeds(current_time)
        self.stats.update_timestamp()

        # Update completion status
        self.stats.is_complete = downloaded >= self.stats.size_bytes
        if self.stats.is_complete:
            self.stats.state = 2  # Seeding
        else:
            self.stats.state = 1  # Downloading

    def update_uploaded(self, uploaded: int):
        """Update uploaded bytes"""
        current_time = time.time()

        self.stats.up_total = uploaded
        self.stats.update_speeds(current_time)
        self.stats.calculate_ratio()
        self.stats.update_timestamp()

    def update_peer_counts(self, connected: int, total: int, seeders: int = 0):
        """Update peer counts"""
        self.stats.peers_connected = connected
        self.stats.peers_accounted = total
        self.stats.peers_not_connected = max(0, total - connected)
        self.stats.peers_complete = seeders
        self.stats.update_timestamp()

    def update_state(self, state: int, message: str = ""):
        """Update client state"""
        self.stats.state = state
        self.stats.message = message
        self.stats.is_active = state in [1, 2, 8]  # downloading, seeding, checking
        self.stats.is_hash_checking = state == 8
        self.stats.update_timestamp()

    def update_activity(self):
        """Update last activity timestamp"""
        self.stats.last_activity = time.time()

    def get_stats(self) -> ClientStats:
        return self.stats

    def get_stats_dict(self) -> Dict[str, Any]:
        return self.stats.to_dict()
