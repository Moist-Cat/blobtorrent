from log import logged
import socket
import struct
import hashlib
from pathlib import Path
import threading
from typing import Tuple, Any
import time
import random
import math

from collections import Counter
from filesystem import FileCache, TorrentFile
from log import logged

@logged
class PeerWireProtocol:
    CHOKE = 0
    UNCHOKE = 1
    INTERESTED = 2
    NOT_INTERESTED = 3
    HAVE = 4
    BITFIELD = 5
    REQUEST = 6
    PIECE = 7
    CANCEL = 8

    MESSAGE_NAMES = {
        0: "CHOKE",
        1: "UNCHOKE",
        2: "INTERESTED",
        3: "NOT_INTERESTED",
        4: "HAVE",
        5: "BITFIELD",
        6: "REQUEST",
        7: "PIECE",
        8: "CANCEL",
    }

    def __init__(self, peer: Tuple[str, int], info_hash: bytes, peer_id: bytes):
        self.peer = peer
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connected = False
        self.bitfield = None
        self.handshake_done = False
        self.logger.debug(f"PeerWireProtocol initialized for {peer}")

    def __hash__(self):
        return hash(self.peer[0], self.peer[1], hash(self.torrent))

    def __eq__(self, other):
        return (self.peer[0] == other.peer[0]) and (self.peer[1] == other.peer[1])

    def connect(self) -> bool:
        try:
            self.logger.info(f"Connecting to peer {self.peer}")
            self.socket.settimeout(10)
            self.socket.connect(self.peer)
            self.logger.info(f"Connected to peer {self.peer}")
            self._handshake()
            self.connected = True
            return True
        except socket.timeout:
            self.logger.warning(f"Connection timeout to peer {self.peer}")
            return False
        except ConnectionRefusedError:
            self.logger.warning(f"Connection refused by peer {self.peer}")
            return False
        except Exception as e:
            self.logger.error(f"Failed to connect to peer {self.peer}: {e}")
            return False

    def _handshake(self):
        try:
            handshake_msg = (
                b"\x13BitTorrent protocol" + b"\x00" * 8 + self.info_hash + self.peer_id
            )
            self.logger.debug(f"Sending handshake to {self.peer}")
            self.socket.send(handshake_msg)
            response = self.socket.recv(68)
            self.logger.debug(f"Received handshake response of length {len(response)}")

            if (
                len(response) != 68
                or response[0:1] != b"\x13"
                or response[1:20] != b"BitTorrent protocol"
                or response[28:48] != self.info_hash
            ):
                self.logger.error(f"Invalid handshake response from {self.peer}")
                raise ConnectionError("Invalid handshake response")

            self.handshake_done = True
            self.logger.info(f"Handshake successful with {self.peer}")
        except Exception as e:
            self.logger.error(f"Handshake failed with {self.peer}: {e}")
            raise

    def _recv_exact(self, n: int) -> bytes:
        """Read exactly n bytes from the socket, handling partial reads."""
        data = b""
        while len(data) < n:
            try:
                chunk = self.socket.recv(n - len(data))
                if not chunk:
                    raise ConnectionError("Connection closed")
                data += chunk
            except socket.timeout:
                raise
            except Exception as e:
                raise ConnectionError(f"Error receiving data: {e}")
        return data

    def receive_message(self) -> Tuple[int, bytes]:
        try:
            # Read exactly 4 bytes for the length prefix
            length_prefix = self._recv_exact(4)
            length = struct.unpack(">I", length_prefix)[0]
            if length == 0:
                self.logger.debug("Received keep-alive message")
                return None, None

            # Read exactly 'length' bytes for the message body
            message_body = self._recv_exact(length)
            message_id = message_body[0]  # first byte is message ID
            payload = message_body[1:]  # rest is payload

            message_name = self.MESSAGE_NAMES.get(message_id, f"UNKNOWN({message_id})")
            self.logger.debug(
                f"Received message: {message_name}, length: {length}, payload: {len(payload)} bytes"
            )

            return message_id, payload
        except socket.timeout:
            self.logger.warning(f"Timeout while receiving message from {self.peer}")
            return None, None
        except ConnectionError as e:
            self.logger.error(f"Connection error from {self.peer}: {e}")
            return None, None
        except Exception as e:
            self.logger.error(f"Error receiving message from {self.peer}: {e}")
            return None, None

    def send_interested(self):
        try:
            msg = struct.pack(">Ib", 1, self.INTERESTED)
            # no difference
            # msg = struct.pack("!IB", 1, self.INTERESTED)
            self.logger.debug("Sending INTERESTED message...")
            self.socket.send(msg)
            self.logger.debug("Sent INTERESTED message")
        except Exception as e:
            self.logger.error(f"Failed to send INTERESTED message: {e}")

    def send_request(self, index: int, begin: int, length: int):
        try:
            msg = struct.pack(">IbIII", 13, self.REQUEST, index, begin, length)
            self.socket.send(msg)
            self.logger.debug(
                f"Sent REQUEST for piece {index}, offset {begin}, length {length}"
            )
        except Exception as e:
            self.logger.error(f"Failed to send REQUEST message: {e}")

    def send_unchoke(self):
        try:
            msg = struct.pack(">Ib", 1, self.UNCHOKE)
            self.socket.send(msg)
            self.logger.debug("Sent UNCHOKE message")
        except Exception as e:
            self.logger.error(f"Failed to send UNCHOKE message: {e}")

    def close(self):
        try:
            self.socket.close()
            self.connected = False
            self.logger.info(f"Closed connection to peer {self.peer}")
        except Exception as e:
            self.logger.error(f"Error closing connection to {self.peer}: {e}")


@logged
class PieceManager:
    def __init__(self, torrent: TorrentFile, output_dir: str = "."):
        self.torrent = torrent
        self.pieces = [None] * torrent.num_pieces
        self.downloaded_pieces = [False] * torrent.num_pieces
        self.downloading_pieces = [False] * torrent.num_pieces
        self.piece_availability = [0] * torrent.num_pieces
        self.lock = threading.Lock()
        self.output_dir = Path(output_dir)
        self.file_cache = FileCache(max_files=512)

        # Initialize file structure and check for existing data
        self.file_offsets = self._initialize_files()

        # Track file completion status for stochastic selection
        self.file_progress = {i: 0 for i in range(len(self.file_offsets))}
        self.piece_to_file = self._map_pieces_to_files()

        # Verify existing pieces
        self._verify_existing_pieces()

        self.logger.info(f"Initialized {len(self.file_offsets)} files")
        self.logger.info(
            f"Already downloaded: {sum(self.downloaded_pieces)}/{self.torrent.num_pieces} pieces"
        )

    def get_total_downloaded(self):
        # read-only
        # no need for a lock here
        return Counter(self.downloaded_pieces)[True]

    def _initialize_files(self):
        file_offsets = []
        current_offset = 0

        if b"files" in self.torrent.info:
            # Multi-file torrent
            root_path = self.output_dir / self.torrent.info[b"name"].decode()
            root_path.mkdir(parents=True, exist_ok=True)

            for file_info in self.torrent.info[b"files"]:
                file_path = root_path / Path(*[p.decode() for p in file_info[b"path"]])
                file_path.parent.mkdir(parents=True, exist_ok=True)

                file_size = file_info[b"length"]
                file_offsets.append(
                    {
                        "path": file_path,
                        "offset": current_offset,
                        "length": file_size,
                        "completed": False,
                    }
                )

                # Check if file exists and has correct size
                if file_path.exists() and file_path.stat().st_size == file_size:
                    self.logger.info(f"File already exists: {file_path}")
                else:
                    # Create or truncate file to correct size
                    with open(file_path, "wb") as f:
                        f.truncate(file_size)
                    self.logger.info(f"Created file: {file_path}")

                current_offset += file_size
        else:
            # Single-file torrent
            file_path = self.output_dir / self.torrent.info[b"name"].decode()
            file_size = self.torrent.info[b"length"]

            file_offsets.append(
                {
                    "path": file_path,
                    "offset": 0,
                    "length": file_size,
                    "completed": False,
                }
            )

            # Check if file exists and has correct size
            if file_path.exists() and file_path.stat().st_size == file_size:
                self.logger.info(f"File already exists: {file_path}")
            else:
                # Create or truncate file to correct size
                with open(file_path, "wb") as f:
                    f.truncate(file_size)
                self.logger.info(f"Created file: {file_path}")

        return file_offsets

    def _verify_existing_pieces(self):
        """Check existing files for already downloaded pieces"""
        self.logger.info("Verifying existing pieces...")

        for piece_index in range(self.torrent.num_pieces):
            # Read piece data from files
            piece_data = self._read_piece_from_files(piece_index)
            if piece_data is None:
                continue  # Piece doesn't exist or error reading

            # Verify piece hash
            expected_hash = self.torrent.piece_hashes[piece_index]
            actual_hash = hashlib.sha1(piece_data).digest()

            if expected_hash == actual_hash:
                self.downloaded_pieces[piece_index] = True
                self.logger.debug(f"Verified existing piece {piece_index}")

                # Update file progress
                piece_start = piece_index * self.torrent.piece_length
                piece_end = min(piece_start + len(piece_data), self.torrent.total_size)
                self._update_file_progress(piece_start, piece_end)
            else:
                self.logger.warning(f"Corrupted piece {piece_index} found, will redownload")

        self.logger.info(
            f"Verification complete. Found {sum(self.downloaded_pieces)} valid pieces"
        )

    def _read_piece_from_files(self, piece_index: int) -> bytes:
        """Read a piece from existing files"""
        piece_start = piece_index * self.torrent.piece_length
        piece_length = self.torrent.piece_length

        # Handle last piece which might be shorter
        if piece_index == self.torrent.num_pieces - 1:
            piece_length = self.torrent.total_size - piece_start

        data = b""
        current_offset = piece_start

        try:
            while len(data) < piece_length:
                # Find the file that contains current_offset
                file_info = None
                for f in self.file_offsets:
                    if f["offset"] <= current_offset < f["offset"] + f["length"]:
                        file_info = f
                        break

                if file_info is None:
                    break  # Should not happen if torrent metadata is correct

                # Calculate how much to read from this file
                file_read_start = current_offset - file_info["offset"]
                file_read_end = min(
                    file_info["length"], file_read_start + (piece_length - len(data))
                )
                bytes_to_read = file_read_end - file_read_start

                # Read from file
                file_obj = self.file_cache.get_file(file_info["path"])
                file_obj.seek(file_read_start)
                chunk = file_obj.read(bytes_to_read)

                if not chunk:
                    break  # EOF or error

                data += chunk
                current_offset += len(chunk)

            return data if len(data) == piece_length else None

        except Exception as e:
            self.logger.error(f"Error reading piece {piece_index}: {e}")
            return None

    def _update_file_progress(self, start_offset: int, end_offset: int):
        """Update progress tracking for files affected by a piece"""
        for file_idx, file_info in enumerate(self.file_offsets):
            file_start = file_info["offset"]
            file_end = file_info["offset"] + file_info["length"]

            # Check if piece overlaps with this file
            if start_offset < file_end and end_offset > file_start:
                # Calculate overlap
                overlap_start = max(start_offset, file_start)
                overlap_end = min(end_offset, file_end)
                bytes_in_file = overlap_end - overlap_start

                self.file_progress[file_idx] += bytes_in_file

    def _map_pieces_to_files(self):
        """Map each piece to the files it affects"""
        piece_to_file = {}

        for piece_index in range(self.torrent.num_pieces):
            piece_start = piece_index * self.torrent.piece_length
            piece_end = piece_start + min(
                self.torrent.piece_length, self.torrent.total_size - piece_start
            )

            affected_files = []
            for file_idx, file_info in enumerate(self.file_offsets):
                file_start = file_info["offset"]
                file_end = file_info["offset"] + file_info["length"]

                # Check if piece overlaps with this file
                if piece_start < file_end and piece_end > file_start:
                    affected_files.append(file_idx)

            piece_to_file[piece_index] = affected_files

        return piece_to_file

    def update_availability(self, bitfield: bytes):
        with self.lock:
            self.logger.debug(
                f"Updating piece availability from bitfield of length {len(bitfield)}"
            )
            for i in range(self.torrent.num_pieces):
                byte_index = i // 8
                bit_index = i % 8
                if (
                    byte_index < len(bitfield)
                    and bitfield[byte_index] >> (7 - bit_index) & 1
                ):
                    self.piece_availability[i] += 1
            self.logger.debug(
                f"Piece availability updated. Available pieces: {sum(1 for x in self.piece_availability if x > 0)}/{self.torrent.num_pieces}"
            )

    def mark_downloading(self, piece_index: int) -> bool:
        """Mark a piece as being downloaded. Returns True if successful, False if already downloading."""
        with self.lock:
            if (
                self.downloading_pieces[piece_index]
                or self.downloaded_pieces[piece_index]
            ):
                return False
            self.downloading_pieces[piece_index] = True
            self.logger.debug(f"Marked piece {piece_index} as downloading")
            return True

    def unmark_downloading(self, piece_index: int):
        """Unmark a piece that is no longer being downloaded."""
        with self.lock:
            self.downloading_pieces[piece_index] = False
            self.logger.debug(f"Unmarked piece {piece_index} as downloading")

    def get_rarest_piece(self) -> int:
        """Stochastic piece selection based on rarity and file completion"""
        with self.lock:
            available_pieces = [
                i
                for i in range(self.torrent.num_pieces)
                if not self.downloaded_pieces[i]
                and not self.downloading_pieces[i]
                and self.piece_availability[i] > 0
            ]

            if not available_pieces:
                self.logger.debug("No available pieces to download")
                return -1

            # Calculate weights for each piece
            weights = []
            for piece_index in available_pieces:
                # Base weight: inverse of availability (rarer pieces have higher weight)
                if self.piece_availability[piece_index] == 0:
                    # Too rare!
                    weights.append(0)
                rarity_weight = 1.0 / (self.piece_availability[piece_index])

                # File completion bonus: pieces from less completed files get higher weight
                file_bonus = 0
                for file_idx in self.piece_to_file[piece_index]:
                    file_completion = (
                        self.file_progress[file_idx]
                        / self.file_offsets[file_idx]["length"]
                    )
                    file_bonus += (
                        1 - file_completion
                    ) / (10)  # Higher bonus for less complete files

                # Combine weights
                weight = rarity_weight * (1 + file_bonus)
                weights.append(weight)

            # Normalize weights to probabilities
            total_weight = sum(weights)
            if total_weight == 0:
                # We don't have anything!?
                # Fallback to uniform distribution if all weights are zero
                weights = [1] * len(weights)
                total_weight = len(weights)

            probabilities = [w / total_weight for w in weights]

            # Select a piece based on probabilities
            selected_index = random.choices(
                available_pieces, weights=probabilities, k=1
            )[0]
            self.logger.debug(f"Selected piece {selected_index} using stochastic selection")
            return selected_index

    def save_piece(self, piece_index: int, data: bytes):
        with self.lock:
            if self.downloaded_pieces[piece_index]:
                self.logger.warning(f"Piece {piece_index} already downloaded, skipping")
                return

            # Verify piece hash
            expected_hash = self.torrent.piece_hashes[piece_index]
            actual_hash = hashlib.sha1(data).digest()

            if expected_hash != actual_hash:
                self.logger.error(
                    f"Piece {piece_index} hash mismatch! Expected: {binascii.hexlify(expected_hash).decode()}, Got: {binascii.hexlify(actual_hash).decode()}"
                )
                return

            # Calculate piece boundaries
            piece_start = piece_index * self.torrent.piece_length
            piece_end = piece_start + len(data)

            # Write to affected files
            for file_info in self.file_offsets:
                file_start = file_info["offset"]
                file_end = file_info["offset"] + file_info["length"]

                # Check if piece overlaps with this file
                if piece_start < file_end and piece_end > file_start:
                    # Calculate overlap boundaries
                    write_start = max(piece_start, file_start)
                    write_end = min(piece_end, file_end)

                    # Calculate file-specific offset and data slice
                    file_offset = write_start - file_start
                    data_start = write_start - piece_start
                    data_end = data_start + (write_end - write_start)

                    # Write to file using cache
                    file_obj = self.file_cache.get_file(file_info["path"])
                    file_obj.seek(file_offset)
                    file_obj.write(data[data_start:data_end])

                    # Update file progress
                    bytes_written = write_end - write_start
                    self.file_progress[
                        self.file_offsets.index(file_info)
                    ] += bytes_written

            self.downloaded_pieces[piece_index] = True
            self.downloading_pieces[piece_index] = False

            downloaded_count = sum(self.downloaded_pieces)
            progress = (downloaded_count / self.torrent.num_pieces) * 100
            self.logger.info(
                f"Piece {piece_index} downloaded and verified successfully. Progress: {progress:.1f}% ({downloaded_count}/{self.torrent.num_pieces} pieces)"
            )

    def is_complete(self) -> bool:
        with self.lock:
            complete = all(self.downloaded_pieces)
            if complete:
                self.logger.info("All pieces downloaded successfully!")
                self.file_cache.close_all()
        return complete

    def close(self):
        self.file_cache.close_all()
        self.logger.info("All files closed successfully")


@logged
class PeerDownloader(threading.Thread):
    def __init__(
        self,
        peer: Tuple[str, int],
        torrent: TorrentFile,
        piece_manager: PieceManager,
        peer_id: bytes,
    ):
        super().__init__()
        self.peer = peer
        self.torrent = torrent
        self.piece_manager = piece_manager
        self.peer_id = peer_id
        self.protocol = PeerWireProtocol(peer, torrent.info_hash, peer_id)
        self.unchoked = False
        self.bitfield = None
        self.daemon = True
        self.current_piece = None
        self.alive = True
        self.logger.info(f"PeerDownloader initialized for {peer}")

    def is_alive(self):
        return self.alive

    def run(self):
        self.logger.info(f"Starting downloader for peer {self.peer}")
        if not self.protocol.connect():
            self.logger.warning(f"Failed to connect to peer {self.peer}, stopping")
            self.alive = False
            return

        try:
            self._receive_bitfield()
            if self.bitfield is None:
                self.logger.warning(f"No bitfield received from {self.peer}, stopping")
                self.alive = False
                return

            self.piece_manager.update_availability(self.bitfield)
            self.protocol.send_interested()

            # Wait for unchoke
            self._wait_for_unchoke()

            self.logger.info(f"Starting download loop with {self.peer}")
            while not self.piece_manager.is_complete():
                if not self.unchoked:
                    self._wait_for_unchoke()
                    continue

                # Get a piece to download
                piece_index = self.piece_manager.get_rarest_piece()
                if piece_index == -1:
                    self.logger.info(f"No more pieces to download from {self.peer}")
                    break

                # Try to reserve the piece
                if not self.piece_manager.mark_downloading(piece_index):
                    self.logger.debug(
                        f"Piece {piece_index} already being downloaded, trying another"
                    )
                    time.sleep(0.1)
                    continue

                self.current_piece = piece_index
                self.logger.info(f"Downloading piece {piece_index} from {self.peer}")

                # Download the piece
                success = self._download_piece(piece_index)

                # Unreserve the piece if download failed
                if not success:
                    self.piece_manager.unmark_downloading(piece_index)
                    self.current_piece = None

        except Exception as e:
            self.logger.error(f"Error in downloader for {self.peer}: {e}")
            if self.current_piece is not None:
                self.piece_manager.unmark_downloading(self.current_piece)
        finally:
            self.protocol.close()
            self.logger.info(f"Downloader for {self.peer} finished")
        self.alive = False

    def _wait_for_unchoke(self):
        """Wait for unchoke message with timeout"""
        start_time = time.time()
        while time.time() - start_time < 30:  # 30 second timeout
            msg_id, payload = self.protocol.receive_message()
            if msg_id is None:
                time.sleep(0.1)
                continue

            if msg_id == PeerWireProtocol.UNCHOKE:
                self.unchoked = True
                self.logger.info(f"Received UNCHOKE from {self.peer}")
                return
            elif msg_id == PeerWireProtocol.CHOKE:
                self.unchoked = False
                self.logger.info(f"Received CHOKE from {self.peer}")
            elif msg_id == PeerWireProtocol.HAVE:
                piece_index = struct.unpack(">I", payload)[0]
                self.logger.debug(f"Received HAVE for piece {piece_index}")
                # Update availability for this piece
                with self.piece_manager.lock:
                    if piece_index < len(self.piece_manager.piece_availability):
                        self.piece_manager.piece_availability[piece_index] += 1

        self.logger.warning(f"Timeout waiting for unchoke from {self.peer}")

    def _receive_bitfield(self):
        self.logger.info(f"Waiting for bitfield from {self.peer}")
        start_time = time.time()
        while time.time() - start_time < 30:  # 30 second timeout
            msg_id, payload = self.protocol.receive_message()
            if msg_id is None:
                time.sleep(0.1)
                continue

            if msg_id == PeerWireProtocol.BITFIELD:
                self.bitfield = payload
                self.logger.info(
                    f"Received bitfield from {self.peer}, length: {len(payload)} bytes"
                )
                return
            elif msg_id == PeerWireProtocol.UNCHOKE:
                self.unchoked = True
                self.logger.info(f"Received UNCHOKE from {self.peer}")
            elif msg_id == PeerWireProtocol.CHOKE:
                self.unchoked = False
                self.logger.info(f"Received CHOKE from {self.peer}")
            elif msg_id == PeerWireProtocol.HAVE:
                piece_index = struct.unpack(">I", payload)[0]
                self.logger.debug(f"Received HAVE for piece {piece_index}")

        self.logger.warning(f"Timeout waiting for bitfield from {self.peer}")

    def _download_piece(self, piece_index: int) -> bool:
        piece_length = self.torrent.piece_length
        if piece_index == self.torrent.num_pieces - 1:
            piece_length = (
                self.torrent.total_size - piece_index * self.torrent.piece_length
            )

        block_size = 2**14
        num_blocks = math.ceil(piece_length / block_size)
        data = b""

        self.logger.debug(f"Downloading piece {piece_index} with {num_blocks} blocks")

        for block_index in range(num_blocks):
            begin = block_index * block_size
            block_length = min(block_size, piece_length - begin)

            self.protocol.send_request(piece_index, begin, block_length)
            block_start_time = time.time()
            block_received = False

            while time.time() - block_start_time < 30:  # 30 second timeout per block
                msg_id, payload = self.protocol.receive_message()

                if msg_id is None:
                    time.sleep(0.1)
                    continue

                if msg_id == PeerWireProtocol.PIECE:
                    index, begin_offset = struct.unpack(">II", payload[:8])
                    block_data = payload[8:]

                    # Verify block index and offset
                    if index != piece_index or begin_offset != begin:
                        self.logger.warning(
                            f"Received wrong block: expected {piece_index}:{begin}, got {index}:{begin_offset}"
                        )
                        continue  # Not the block we're waiting for

                    # Critical: Check block length
                    if len(block_data) != block_length:
                        self.logger.warning(
                            f"Incorrect block length: expected {block_length}, got {len(block_data)}"
                        )
                        continue

                    data += block_data
                    self.logger.debug(
                        f"Received block {block_index+1}/{num_blocks} for piece {piece_index}"
                    )
                    block_received = True
                    break
                elif msg_id == PeerWireProtocol.CHOKE:
                    self.unchoked = False
                    self.logger.info(f"Choked by {self.peer} during download")
                    return False
                elif msg_id == PeerWireProtocol.UNCHOKE:
                    self.unchoked = True
                    self.logger.info(f"Unchoked by {self.peer}")
                elif msg_id == PeerWireProtocol.HAVE:
                    piece_idx = struct.unpack(">I", payload)[0]
                    self.logger.debug(f"Received HAVE for piece {piece_idx}")

            if not block_received:
                self.logger.warning(
                    f"Timeout downloading block {block_index} of piece {piece_index} from {self.peer}"
                )
                return False

        # Verify piece hash before saving
        computed_hash = hashlib.sha1(data).digest()
        expected_hash = self.torrent.piece_hashes[piece_index]
        if computed_hash != expected_hash:
            self.logger.error(
                f"Hash mismatch for piece {piece_index}. Expected {expected_hash.hex()}, got {computed_hash.hex()}"
            )
            return False

        self.piece_manager.save_piece(piece_index, data)
        self.logger.info(
            f"Successfully downloaded and verified piece {piece_index} from {self.peer}"
        )
        return True


