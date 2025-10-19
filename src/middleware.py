import binascii
import hashlib
from pathlib import Path
import threading
from typing import Tuple, Any
import random

from collections import Counter
from filesystem import FileCache, TorrentFile
from log import logged


@logged
class PieceManager:
    def __init__(self, torrent: TorrentFile, output_dir: str = "."):
        self.torrent = torrent
        self.pieces = [None] * torrent.num_pieces
        self.downloaded_pieces = [False] * torrent.num_pieces
        self.downloading_pieces = [False] * torrent.num_pieces
        # self.piece_availability <- property
        self.peer_availability = {}
        self.lock = threading.Lock()
        self.output_dir = Path(output_dir)
        self.file_cache = FileCache(max_files=512)

        # Initialize file structure and check for existing data
        self.file_offsets = self._initialize_files()

        # Track file completion status for stochastic selection
        self.file_progress = {i: 0 for i in range(len(self.file_offsets))}
        self.piece_to_file = self._map_pieces_to_files()

        self.check_interval = 30

        # Verify existing pieces
        self._verify_existing_pieces()
        self.check_thread = threading.Thread(target=self.check_pieces, daemon=True)

        self.logger.info(f"Initialized {len(self.file_offsets)} files")
        self.logger.info(
            f"Already downloaded: {sum(self.downloaded_pieces)}/{self.torrent.num_pieces} pieces"
        )

    def check_pieces(self):
        """
        Thread to check the integrity of the files
        """
        while True:
            time.sleep(self.check_interval)
            self._verify_existing_pieces()

    def get_total_downloaded(self):
        # read-only
        # no need for a lock here
        return min(
            Counter(self.downloaded_pieces)[True] * self.torrent.piece_length,
            self.torrent.total_size,
        )

    def get_bitfield(self) -> bytes:
        """Generate a bitfield representing which pieces we have"""
        with self.lock:
            # Calculate the number of bytes needed for the bitfield
            num_bytes = (self.torrent.num_pieces + 7) // 8
            bitfield = bytearray(num_bytes)

            # Set bits for pieces we have
            for i in range(self.torrent.num_pieces):
                if self.downloaded_pieces[i]:
                    byte_index = i // 8
                    bit_index = i % 8
                    bitfield[byte_index] |= 1 << (7 - bit_index)

            return bytes(bitfield)

    def has_piece(self, piece_index: int) -> bool:
        """Check if we have a specific piece"""
        with self.lock:
            if 0 <= piece_index < len(self.downloaded_pieces):
                return self.downloaded_pieces[piece_index]
            return False

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
            piece_data = self.read_piece(piece_index)
            if piece_data is None:
                self.downloaded_pieces[piece_index] = False
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
                self.downloaded_pieces[piece_index] = False
                # self.logger.warning(
                #    f"Corrupted piece {piece_index} found, will redownload"
                # )

        self.logger.info(
            f"Verification complete. Found {sum(self.downloaded_pieces)} valid pieces"
        )

    def read_piece(self, piece_index: int) -> bytes:
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

    def update_peer_availability(self, peer: tuple, index: int):
        if peer not in self.peer_availability:
            self.peer_availability[peer] = [0 for _ in range(self.torrent.num_pieces)]
        self.peer_availability[peer][index] = 1

    @property
    def piece_availability(self):
        total_avl = [0 for _ in range(self.torrent.num_pieces)]
        for avl in self.peer_availability.values():
            for i, is_available in enumerate(avl):
                total_avl[i] += is_available
        return total_avl

    def update_availability(self, bitfield: bytes, peer: tuple):
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
                    self.update_peer_availability(peer, i)
            avl_percent = (
                self.torrent.num_pieces - Counter(self.piece_availability)[0]
            ) / self.torrent.num_pieces
            self.logger.debug(
                f"Piece availability updated. Available pieces: %s%%",
                str(avl_percent * 100),
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

    def get_rarest_piece(self, peer) -> int:
        """Stochastic piece selection based on rarity and file completion"""
        # never ask for pieces the peer doesn't have
        full_avl = self.piece_availability.copy()
        for i, avl in enumerate(full_avl):
            if not self.peer_availability[peer][i]:
                full_avl[i] = 0
        with self.lock:
            available_pieces = [
                i
                for i in range(self.torrent.num_pieces)
                if not self.downloaded_pieces[i]
                and not self.downloading_pieces[i]
                and full_avl[i] > 0
            ]

            if not available_pieces:
                self.logger.debug("No available pieces to download")
                return -1

            # Calculate weights for each piece
            weights = []
            for piece_index in available_pieces:
                # Base weight: inverse of availability (rarer pieces have higher weight)
                rarity_weight = 1.0 / (full_avl[piece_index])

                # File completion bonus: pieces more less completed files get higher weight
                file_bonus = 0
                for file_idx in self.piece_to_file[piece_index]:
                    file_completion = (
                        self.file_progress[file_idx]
                        / self.file_offsets[file_idx]["length"]
                    )
                    file_bonus += (file_completion) / (
                        10
                    )  # Higher bonus for more complete files

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
            self.logger.debug(
                f"Selected piece {selected_index} using stochastic selection"
            )
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
