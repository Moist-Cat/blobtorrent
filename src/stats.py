import math
import time
from typing import Dict, Any, List
from dataclasses import dataclass, field
from collections import deque

from peer_drivers import TrackerDriver

def calculate_speed(
    last_update,
    current_time: float,
    dq,
    total,
    max_size=9,
    slice_size=10,
):
    time_delta = (current_time // slice_size) - (last_update // slice_size)
    if time_delta > 0:
        dq.append(total)

    if len(dq) <= 1:
        return 0
    # at least two elements

    if len(dq) > max_size:
        dq.popleft()

    deltas = []
    for i in range(len(dq) - 1):
        before, after = dq[i], dq[i + 1]
        deltas.append(after - before)
    return sum(deltas) / (len(deltas) * slice_size)


@dataclass
class File:
    path: str
    size_bytes: int
    size_chunks: int
    completed_chunks: int

    def to_dict(self):
        return {
            "path": str(self.path),
            "size_bytes": self.size_bytes,
            "size_chunks": self.size_chunks,
            "completed_chunks": self.completed_chunks,
        }


@dataclass
class Tracker:
    url: str

    def to_dict(self):
        return {"url": self.url}


@dataclass
class Peer:
    ip: str
    port: int
    is_incoming: bool = False

    last_downloaded: deque = field(default_factory=deque)
    last_uploaded: deque = field(default_factory=deque)

    last_update: float = field(default_factory=time.time)

    up_rate: int = 0
    down_rate: int = 0

    @property
    def address(self):
        return f"{self.ip}:{self.port}"

    def to_dict(self):
        return {
            "address": self.address,
            "up_rate": self.up_rate,
            "down_rate": self.down_rate,
            "is_incoming": self.is_incoming,
        }

    def update_speeds(self, down_total, up_total, current_time):

        self.down_rate = calculate_speed(
            self.last_update, current_time, self.last_downloaded, down_total,
        )
        self.up_rate = calculate_speed(
            self.last_update, current_time, self.last_uploaded, up_total,
        )
        self.last_update = current_time


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
    start_time: float = field(default_factory=time.time)

    # Trackers
    tracker_count: int = 0

    # Other structures
    files: List[File] = field(default_factory=list)
    trackers: List[Tracker] = field(default_factory=list)
    peers: List[Peer] = field(default_factory=list)

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
            "info_hash": self.info_hash.hex()
            if isinstance(self.info_hash, bytes)
            else self.info_hash,
            "name": self.name,
            "size_bytes": self.size_bytes,
            # Progress
            "completed_bytes": self.completed_bytes,
            "down_total": self.down_total,
            "up_total": self.up_total,
            "left_bytes": self.left_bytes,
            "progress": self.progress,
            # Speed and ratio
            "down_rate": sum(peer.down_rate for peer in self.peers),
            "up_rate": sum(peer.up_rate for peer in self.peers),
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
            # Additional
            "tracker_count": self.tracker_count,
            "files": [file.to_dict() for file in self.files],
            "trackers": [tracker.to_dict() for tracker in self.trackers],
            "peers": [peer.to_dict() for peer in self.peers],
        }

    def _get_state_string(self) -> str:
        """Convert state code to string"""
        state_map = {
            0: "stopped",
            1: "downloading",
            2: "seeding",
            4: "error",
            8: "checking",
        }
        return state_map.get(self.state, "stopped")

    def calculate_ratio(self):
        if self.down_total > 0:
            self.ratio = round(self.up_total / self.down_total, 2)
        else:
            self.ratio = 0.0

class Statistics:
    """Enhanced statistics manager with proper speed calculation"""

    def __init__(self, client, download_path: str = ""):
        self.client = client
        self.piece_manager = client.piece_manager
        self.connection_manager = client.connection_manager
        self.torrent = client.torrent

        self.stats = ClientStats(
            info_hash=self.torrent.info_hash,
            name=self.torrent.name,
            size_bytes=self.torrent.total_size,
            left_bytes=max(
                0, self.torrent.total_size - self.piece_manager.get_total_downloaded()
            ),
            size_chunks=len(self.torrent.piece_hashes),
            chunk_size=self.torrent.piece_length,
            download_path=download_path,
            base_path=download_path,
            base_filename=self.torrent.name,
            directory=download_path,
        )
        for driver in self.client.discovery_mechanisms:
            if isinstance(driver, TrackerDriver):
                for url in driver.announce_urls:
                    self.stats.trackers.append(Tracker(url=url))
        self._update_stats()

        self.last_downloaded = 0
        self.last_uploaded = 0

    def _update_stats(self):
        """Update client statistics with real data"""
        current_time = time.time()

        downloaded = self.piece_manager.get_total_downloaded()
        total_uploaded = {}
        total_downloaded = {}
        peers_dict = {
            (peer.ip, peer.port): idx for idx, peer in enumerate(self.stats.peers)
        }
        for connection in self.connection_manager.active_connections:
            down_total = connection.protocol.total_downloaded
            up_total = connection.protocol.total_uploaded

            total_downloaded[
                connection.protocol.peer
            ] = down_total
            total_uploaded[
                connection.protocol.peer
            ] = up_total

            ip, port = connection.protocol.peer
            if (ip, port) in peers_dict:
                self.stats.peers[peers_dict[(ip, port)]].update_speeds(
                    down_total,
                    up_total,
                    current_time
                )
            else:
                new_peer = Peer(
                    ip=ip,
                    port=port,
                    last_update=0,
                    is_incoming=connection.protocol.is_incoming,
                )
                new_peer.update_speeds(
                    down_total,
                    up_total,
                    current_time,
                )
                self.stats.peers.append(
                    new_peer,
                )

        for file_idx, file_data in enumerate(self.piece_manager.file_offsets):
            completed_chunks = math.ceil(
                self.piece_manager.file_progress[file_idx] / self.stats.chunk_size
            )
            size_chunks = math.ceil(file_data["length"] / self.stats.chunk_size)
            new_file = File(
                path=file_data["path"],
                size_bytes=file_data["length"],
                size_chunks=size_chunks,
                completed_chunks=completed_chunks
            )
            if len(self.stats.files) <= file_idx:
                self.stats.files.append(new_file)
            else:
                self.stats.files[file_idx].completed_chunks = completed_chunks

        self.update_downloaded(downloaded)
        #self.update_uploaded(uploaded)

        # Update peer counts
        connected_peers = self.connection_manager.get_connection_count()
        total_peers = len(self.client._get_all_peers())
        self.update_peer_counts(connected_peers, total_peers, 0)

        # Update state based on client state
        state_mapping = {
            self.client.state_enum.INITIALIZING: 0,
            self.client.state_enum.DISCOVERING_PEERS: 0,
            self.client.state_enum.DOWNLOADING: 1,
            self.client.state_enum.SEEDING: 2,
            self.client.state_enum.PAUSED: 0,
            self.client.state_enum.STOPPED: 0,
            self.client.state_enum.ERROR: 4,
        }
        state_code = state_mapping[self.client.state]
        self.update_state(state_code)

        # Update activity
        self.update_activity()

    def update_downloaded(self, downloaded: int):
        """Update downloaded bytes"""
        self.stats.down_total = downloaded
        self.stats.completed_bytes = downloaded
        self.stats.left_bytes = max(0, self.stats.size_bytes - downloaded)
        self.stats.completed_chunks = int(
            (downloaded / self.stats.size_bytes) * self.stats.size_chunks
        )

        # Update completion status
        self.stats.is_complete = downloaded >= self.stats.size_bytes
        if self.stats.is_complete:
            self.stats.state = 2  # Seeding
        else:
            self.stats.state = 1  # Downloading

    def update_uploaded(self, uploaded: int):
        """Update uploaded bytes"""
        self.stats.up_total = uploaded
        self.stats.calculate_ratio()

    def update_peer_counts(self, connected: int, total: int, seeders: int = 0):
        """Update peer counts"""
        self.stats.peers_connected = connected
        self.stats.peers_accounted = total
        self.stats.peers_not_connected = max(0, total - connected)
        self.stats.peers_complete = seeders

    def update_state(self, state: int, message: str = ""):
        """Update client state"""
        self.stats.state = state
        self.stats.message = message
        self.stats.is_active = state in [1, 2, 8]  # downloading, seeding, checking
        self.stats.is_hash_checking = state == 8

    def update_activity(self):
        """Update last activity timestamp"""
        self.stats.last_activity = time.time()

    def get_stats(self) -> ClientStats:
        self._update_stats()
        return self.stats

    def get_stats_dict(self) -> Dict[str, Any]:
        self._update_stats()
        return self.stats.to_dict()
