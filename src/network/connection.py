import binascii
from collections import deque
import queue
from pathlib import Path
import socket
import struct
import hashlib
import threading
from typing import Tuple, Any, Optional, Set
import time
import math

from filesystem import TorrentFile
from network.protocol import PeerWireProtocol
from middleware import PieceManager
from log import logged


@logged
class PortManager:
    """Manages port allocation and verification"""

    # DEFAULT_PORT_RANGE = (6881, 6889)  # Default BitTorrent port range
    DEFAULT_PORT_RANGE = (6881, 60000)

    def __init__(self, port_range: tuple = DEFAULT_PORT_RANGE):
        self.port_range = port_range
        self.allocated_ports = set()

    def is_port_available(self, port: int) -> bool:
        """Check if a port is available for use"""
        try:
            # Try to create a socket and bind to the port
            test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            test_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            test_socket.bind(("0.0.0.0", port))
            test_socket.close()
            return True
        except socket.error:
            return False

    def allocate_port(self, preferred_port: Optional[int] = None) -> Optional[int]:
        """Allocate an available port, trying preferred port first, then the range"""
        # Try preferred port if specified
        if (
            preferred_port
            and self.is_port_available(preferred_port)
            and preferred_port not in self.allocated_ports
        ):
            self.allocated_ports.add(preferred_port)
            return preferred_port

        # Try ports in the specified range
        for port in range(self.port_range[0], self.port_range[1] + 1):
            if port not in self.allocated_ports and self.is_port_available(port):
                self.allocated_ports.add(port)
                return port

        # No available ports found
        return None

    def release_port(self, port: int):
        """Release a previously allocated port"""
        if port in self.allocated_ports:
            self.allocated_ports.remove(port)

    def get_allocated_ports(self):
        """Get all currently allocated ports"""
        return self.allocated_ports.copy()


class PeerConnection(threading.Thread):
    """Base class for both active and passive peer connections"""

    def __init__(self, protocol: PeerWireProtocol, piece_manager: PieceManager):
        # first to avoid errors when threading.Thread calls ref
        self.protocol = protocol

        super().__init__()
        self.piece_manager = piece_manager
        self.running = True
        self.daemon = True
        self.handler = None

    def __hash__(self):
        return hash(self.protocol)

    def __eq__(self, other):
        return self.protocol == other.protocol

    def run(self):
        """Main connection loop - to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement run()")

    def stop(self):
        """Stop the connection"""
        self.running = False

    def pause(self):
        if not self.handler:
            return
        self.handler.paused = True

    def resume(self):
        if not self.handler:
            return
        self.handler.paused = False

    def is_alive(self):
        """Check if the connection is still active"""
        # return self.running and self.protocol.connected
        return self.running

    def close(self):
        """Close the connection"""
        self.stop()
        self.protocol.close()


@logged
class ActiveConnection(PeerConnection):
    """Handles active (outgoing) connections to peers"""

    def __init__(
        self,
        peer: Tuple[str, int],
        torrent: TorrentFile,
        piece_manager: PieceManager,
        peer_id: bytes,
        is_seeder: bool = False,
    ):
        protocol = PeerWireProtocol(peer, torrent.info_hash, peer_id)
        super().__init__(protocol, piece_manager)
        self.peer = peer
        self.torrent = torrent
        self.is_seeder = is_seeder
        self.logger.info(
            f"ActiveConnection initialized for {peer} (seeder: {is_seeder})"
        )

    def run(self):
        """Establish an active connection and hand off to appropriate handler"""
        self.logger.info(f"Establishing active connection to {self.peer}")

        # Attempt to connect
        if not self.protocol.connect():
            self.logger.warning(f"Failed to connect to peer {self.peer}")
            self.running = False
            return

        # Hand off to appropriate handler based on our role
        if self.is_seeder:
            self.handler = SeedHandler(self.protocol, self.piece_manager)
        else:
            self.handler = DownloadHandler(
                self.protocol, self.piece_manager, self.torrent
            )

        # Run the handler
        self.handler.run()

        self.logger.info(f"Active connection to {self.peer} finished")
        self.running = False


@logged
class PassiveConnection(PeerConnection):
    """Handles passive (incoming) connections from peers"""

    def __init__(
        self,
        socket: socket.socket,
        peer: Tuple[str, int],
        torrent: TorrentFile,
        piece_manager: PieceManager,
        peer_id: bytes,
        is_seeder: bool = False,
    ):
        protocol = PeerWireProtocol(peer, torrent.info_hash, peer_id)
        super().__init__(protocol, piece_manager)
        self.socket = socket
        self.peer = peer
        self.torrent = torrent
        self.is_seeder = is_seeder
        self.logger.info(
            f"PassiveConnection initialized for {peer} (seeder: {is_seeder})"
        )

    def run(self):
        """Handle an incoming connection and hand off to appropriate handler"""
        self.logger.info(f"Handling passive connection from {self.peer}")

        # Handle the incoming connection
        if not self.protocol.handle_incoming_connection(self.socket, self.peer):
            self.logger.warning(
                f"Failed to handle incoming connection from {self.peer}"
            )
            self.running = False
            return

        # Hand off to appropriate handler based on our role
        if self.is_seeder:
            self.handler = SeedHandler(self.protocol, self.piece_manager)
        else:
            self.handler = DownloadHandler(
                self.protocol, self.piece_manager, self.torrent
            )

        # Run the handler
        self.handler.run()

        self.logger.info(f"Passive connection from {self.peer} finished")
        self.running = False


@logged
class ConnectionManager:
    def __init__(self, max_connections=50):
        self.max_connections = max_connections
        self.active_connections = set()
        self.lock = threading.RLock()

    def add_connection(self, connection: PeerConnection) -> bool:
        with self.lock:
            if len(self.active_connections) < self.max_connections:
                self.active_connections.add(connection)
                return True
            return False

    def remove_connection(self, connection: PeerConnection) -> bool:
        with self.lock:
            if connection in self.active_connections:
                self.active_connections.remove(connection)
                connection.close()
                return True
            return False

    def get_connection_count(self) -> int:
        with self.lock:
            return len(self.active_connections)

    def close_all(self):
        with self.lock:
            for connection in self.active_connections:
                connection.close()
            self.active_connections = set()

    def cleanup_finished(self):
        """Remove finished connections from the list"""
        with self.lock:
            self.active_connections = {
                conn for conn in self.active_connections if conn.is_alive()
            }
            return len(self.active_connections)

    def already_connected_to(self, peer: Tuple[str, int]):
        with self.lock:
            for connection in self.active_connections:
                if connection.peer == peer:
                    return True
            return False

    def get_connection_count(self) -> int:
        """Get number of active connections"""
        return len([c for c in self.active_connections if c.is_alive()])

    def __len__(self):
        return len(self.active_connections)


BLOCK_SIZE = 2**14
BASE_MAX_REQUESTS = 10


def get_piece_length(torrent, piece_index):
    piece_length = torrent.piece_length
    if piece_index == torrent.num_pieces - 1:
        piece_length = torrent.total_size - piece_index * torrent.piece_length
    return piece_length


def get_total_blocks(piece_length, block_size):
    return math.ceil(piece_length / block_size)


def get_start_block(begin, block_size):
    return math.ceil(begin / block_size)


@logged
class DownloadHandler:
    """Asynchronous download handler with message buffering and pipelining"""

    def __init__(
        self,
        protocol,
        piece_manager,
        torrent,
    ):
        self.protocol = protocol
        self.piece_manager = piece_manager
        self.torrent = torrent
        self.unchoked = False
        self.bitfield = None
        self.paused = False
        self.wait_timeout = 90
        self.sleep_time = 0.1

        # Async messaging
        self.message_queue = queue.Queue()
        self.expected_blocks: set = set()  # block_index
        self.received_blocks: dict(int, bytes) = {}  # begin -> block_data
        self.current_piece: int = None
        self.reader_thread = None
        self.shutdown = False

        # Cache for partial pieces
        ih = binascii.hexlify(torrent.info_hash).decode()
        self.cache_dir = Path(f"/tmp/blobcache/{ih}/")
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # aim for 10 pieces (160kb/s)
        self.last_piece = time.time()
        self.current_max_requests = BASE_MAX_REQUESTS
        self.request_upper_bound = 64
        self.request_lower_bound = 1
        self.piece_count = 0

    def penalize_max_requests(self):
        new = self.current_max_requests // 2
        self.current_max_requests = max(
            self.request_lower_bound,
            new,
        )
        self.logger.debug("Modified max requests to %d", self.current_max_requests)

    def reward_max_requests(self):
        self.current_max_requests = min(
            self.request_upper_bound, self.current_max_requests + 1
        )
        self.logger.debug("Modified max requests to %d", self.current_max_requests)

    def load_piece(self, piece_index: int) -> bytes:
        """Load partially downloaded piece from cache"""
        piece_file = self.cache_dir / str(piece_index)
        if not piece_file.exists():
            self.save_piece(piece_index, b"")
        with open(piece_file, "rb") as file:
            return file.read()

    def save_piece(self, piece_index: int, data: bytes):
        """Save piece progress to cache"""
        piece_file = self.cache_dir / str(piece_index)
        with open(piece_file, "wb") as file:
            file.write(data)

    def invalidate_piece_cache(self, piece_index: int):
        """Remove piece from cache"""
        piece_file = self.cache_dir / str(piece_index)
        if piece_file.exists():
            piece_file.unlink()
        else:
            self.logger.warning(
                "Tried to invalidate unexisting cache file for %s", str(piece_index)
            )

    def run(self):
        """Execute the asynchronous downloading protocol"""
        self.logger.info(f"Starting async download handler for {self.protocol.peer}")

        try:
            # Start the message reader thread
            self.reader_thread = threading.Thread(
                target=self._message_reader, daemon=True
            )
            self.reader_thread.start()

            # Receive bitfield and update availability
            if not self._receive_bitfield_async():
                self.logger.warning(f"No bitfield received from {self.protocol.peer}")
                return

            self.piece_manager.update_availability(self.bitfield, self.protocol.peer)
            self.protocol.send_interested()

            # Wait for unchoke
            if not self._wait_for_unchoke_async():
                self.logger.warning(
                    f"Timeout waiting for unchoke from {self.protocol.peer}"
                )
                return

            # Main download loop
            while (
                not self.piece_manager.is_complete()
                and self.protocol.connected
                and not self.shutdown
            ):
                if self.paused:
                    self.logger.debug("Paused...")
                    time.sleep(5)
                    continue

                if not self.unchoked:
                    if not self._wait_for_unchoke_async():
                        self.logger.warning(f"Lost unchoke from {self.protocol.peer}")
                        break

                # Process any pending messages
                self._process_queued_messages()

                # Request new piece if needed as well
                self._request_piece()

                # Process received blocks and check for completion
                self._process_received_blocks()

                time.sleep(self.sleep_time)

        except Exception as e:
            self.logger.error(
                f"Error in async download handler for {self.protocol.peer}: {e}"
            )
        finally:
            self.shutdown = True
            # Clean up active pieces
            self.logger.info(
                f"Async download handler for {self.protocol.peer} finished"
            )

    def _message_reader(self):
        """Dedicated thread to read all incoming messages"""
        self.logger.debug(f"Starting message reader for {self.protocol.peer}")

        while self.protocol.connected and not self.shutdown:
            try:
                msg_id, payload = self.protocol.receive_message()
                if msg_id is not None:
                    self.message_queue.put((msg_id, payload))
                else:
                    time.sleep(0.01)  # Small sleep when no message
            except Exception as e:
                self.logger.error(
                    f"Error in message reader for {self.protocol.peer}: {e}"
                )
                break

    def _process_queued_messages(self):
        """Process all available messages in the queue"""
        processed_count = 0
        while not self.message_queue.empty() and processed_count < 50:
            try:
                msg_id, payload = self.message_queue.get_nowait()
                self._handle_message(msg_id, payload)
                processed_count += 1
            except queue.Empty:
                break

    def _handle_message(self, msg_id: int, payload: bytes):
        """Handle a single message"""
        if msg_id == PeerWireProtocol.PIECE:
            self._handle_piece_message(payload)
        elif msg_id == PeerWireProtocol.UNCHOKE:
            self.unchoked = True
            self.logger.info(f"Received UNCHOKE from {self.protocol.peer}")
        elif msg_id == PeerWireProtocol.CHOKE:
            self.unchoked = False
            self.penalize_max_requests()
            self.logger.info(f"Received CHOKE from {self.protocol.peer}")
            # Cancel all pending requests when choked
            self.expected_blocks.clear()
        elif msg_id == PeerWireProtocol.HAVE:
            piece_index = struct.unpack(">I", payload)[0]
            self.logger.debug(f"Received HAVE for piece {piece_index} (ignoring)")
            self.piece_manager.update_peer_availability(self.protocol.peer, piece_index)
            # Do not update availability
            # Since when were you under the impression you could suggest pieces?
        elif msg_id == PeerWireProtocol.BITFIELD:
            self.logger.debug("Received BITFIELD")
            self.bitfield = payload

    def _handle_piece_message(self, payload: bytes):
        """Handle a PIECE message"""
        if len(payload) < 8:
            self.logger.warning(f"Invalid PIECE message length: {len(payload)}")
            return

        index, begin = struct.unpack(">II", payload[:8])
        block_data = payload[8:]

        # Check if this is a block we're expecting
        if index == self.current_piece and begin in self.expected_blocks:
            block_type = "expected"
            self.received_blocks[begin] = block_data
            self.save_contiguous_block(begin)
            self.expected_blocks.remove(begin)
            self.reward_max_requests()
        else:
            block_type = "orphan"
            self.save_contiguous_orphan(index, begin, block_data)

        self.logger.debug(
            "Received %s block %d:%d (%d bytes)",
            block_type,
            index,
            begin,
            len(block_data),
        )

    def save_contiguous_block(self, begin):
        """
        Save data while keeping the cache contiguous
        """
        data = self.load_piece(self.current_piece)
        while begin == len(data) and begin in self.received_blocks:
            block_data = self.received_blocks.pop(begin)
            data += block_data
            begin += len(block_data)
            self.save_piece(self.current_piece, data)

    def save_contiguous_orphan(self, index, begin, block_data):
        if (
            not self.piece_manager.mark_downloading(index)
            and index != self.current_index
        ):
            self.logger.warning("Couldn't adopt orphan %d:%d", index, begin)
            return
        data = self.load_piece(index)
        if begin == len(data):
            data += block_data
            self.save_piece(index, data)
        else:
            self.logger.warning(
                "Couldn't adopt orphan %d:%d (non-contiguous block)", index, begin
            )

        self.piece_manager.unmark_downloading(index)

    def _receive_bitfield_async(self) -> bool:
        """Wait for bitfield message asynchronously"""
        self.logger.info(f"Waiting for bitfield from {self.protocol.peer}")
        start_time = time.time()

        while time.time() - start_time < self.wait_timeout:
            if self.shutdown or not self.protocol.connected:
                return False

            self._process_queued_messages()

            if self.bitfield is not None:
                return True

            time.sleep(self.sleep_time)

        self.logger.warning(f"Timeout waiting for bitfield from {self.protocol.peer}")
        return False

    def _wait_for_unchoke_async(self) -> bool:
        """Wait for unchoke message asynchronously"""
        start_time = time.time()

        while time.time() - start_time < self.wait_timeout:
            if self.shutdown or not self.protocol.connected:
                return False

            self._process_queued_messages()

            if self.unchoked:
                return True

            time.sleep(self.sleep_time)

        return False

    def _request_piece(self):
        """Request a piece to download"""
        if not self.current_piece:
            piece_index = self.piece_manager.get_rarest_piece(self.protocol.peer)
            if piece_index == -1:
                # we are (supposedly) done
                # wait for the others
                time.sleep(10)
                return
            # Try to reserve the piece
            if not self.piece_manager.mark_downloading(piece_index):
                self.logger.debug(f"Piece {piece_index} already being downloaded")
                return
            self.current_piece = piece_index

        self.logger.info(
            "Downloading piece %s from %s",
            self.current_piece,
            self.protocol.peer,
        )

        # Initialize piece data and request first blocks
        self._initialize_piece_download(self.current_piece)

    def _initialize_piece_download(self, piece_index: int):
        """Initialize downloading of a piece and request initial blocks"""
        # Load existing data
        data = self.load_piece(piece_index)
        if not self.expected_blocks:
            begin = len(data)
        else:
            begin = max(self.expected_blocks) + BLOCK_SIZE
        start_block = get_start_block(begin, BLOCK_SIZE)

        # Calculate blocks needed
        piece_length = get_piece_length(self.torrent, piece_index)

        total_blocks = get_total_blocks(piece_length, BLOCK_SIZE)

        self.logger.debug(
            f"Initializing piece %s with %s blocks "
            f"(already have %s bytes (requested: %s blocks))",
            piece_index,
            total_blocks,
            len(data),
            len(self.expected_blocks),
        )
        max_requests = self.current_max_requests - len(self.expected_blocks)
        if max_requests == 0 or total_blocks - start_block == 0:
            self.logger.debug("Download queue is full")
            time.sleep(1)
            return

        requested_count = 0
        for block_index in range(start_block, total_blocks):
            if requested_count >= max_requests:
                break

            begin = block_index * BLOCK_SIZE
            block_length = min(BLOCK_SIZE, piece_length - begin)

            # Skip if already requested or received
            if (
                block_index in self.expected_blocks
                or block_index in self.received_blocks
            ):
                self.logger.warning("Received non-contiguous block %s", block_index)
                continue

            self.protocol.send_request(piece_index, begin, block_length)
            self.expected_blocks.add(begin)
            requested_count += 1

    def _process_received_blocks(self):
        """Process received blocks and check for piece completion"""
        if self.received_blocks or self.expected_blocks:
            # We verify the piece so all of these
            # are ours
            return

        if self._is_piece_complete(self.current_piece):
            self._finalize_piece(self.current_piece)

    def _is_piece_complete(self, piece_index: int) -> bool:
        """Check if a piece is complete based on received blocks"""
        piece_length = get_piece_length(self.torrent, piece_index)

        # Load current data
        data = self.load_piece(piece_index)

        return len(data) == piece_length

    def _finalize_piece(self, piece_index: int):
        """Finalize a completed piece - verify and save"""
        # Assemble final piece data
        data = self.load_piece(piece_index)

        # Verify piece hash
        computed_hash = hashlib.sha1(data).digest()
        expected_hash = self.torrent.piece_hashes[piece_index]

        if computed_hash != expected_hash:
            self.logger.error(f"Hash mismatch for piece {piece_index}")
            self._cleanup_piece()
            return

        # Save to piece manager
        self.piece_manager.save_piece(piece_index, data)
        self.logger.info(
            f"Successfully downloaded piece {piece_index} from {self.protocol.peer}"
        )

        # Clean up
        self._cleanup_piece()

    def _cleanup_piece(self):
        """Clean up resources for a piece"""
        # sanity check
        self.expected_blocks.clear()
        self.received_blocks.clear()

        # Unmark in piece manager
        if self.current_piece:
            self.piece_manager.unmark_downloading(self.current_piece)

        # Clean cache
        self.invalidate_piece_cache(self.current_piece)

        # Remove from active pieces
        self.current_piece = None

    def stop(self):
        """Stop the download handler"""
        self.shutdown = True
        if self.reader_thread and self.reader_thread.is_alive():
            self.reader_thread.join(timeout=5.0)


@logged
class SeedHandler:
    """Handles the seeding phase of a connection with improved protocol compliance"""

    def __init__(self, protocol: PeerWireProtocol, piece_manager: PieceManager):
        self.protocol = protocol
        self.piece_manager = piece_manager
        self.unchoked = False
        self.peer_interested = False
        self.peer_choking = True  # We start by choking the peer
        self.last_keep_alive = time.time()
        self.keep_alive_interval = 120  # 2 minutes
        self.max_idle_time = 10  # 5 minutes
        self.last_activity = time.time()
        self.running = True
        self.paused = False

    def run(self):
        """Execute the seeding protocol with proper connection management"""
        self.logger.info(f"Starting seed handler for {self.protocol.peer}")

        try:
            # Send our bitfield to show what pieces we have
            bitfield = self.piece_manager.get_bitfield()
            if not self.protocol.send_bitfield(bitfield):
                self.logger.error(f"Failed to send bitfield to {self.protocol.peer}")
                return

            # Main seeding loop
            while self.running and self.protocol.connected:
                if self.paused:
                    self.logger.debug("Paused...")
                    time.sleep(5)
                # Check for inactivity timeout
                if time.time() - self.last_activity > self.max_idle_time:
                    self.logger.info(
                        f"Closing idle connection with {self.protocol.peer}"
                    )
                    break

                # Send keep-alive if needed
                if time.time() - self.last_keep_alive > self.keep_alive_interval:
                    if not self.protocol.send_keep_alive():
                        break
                    self.last_keep_alive = time.time()

                # Receive message
                msg_id, payload = self.protocol.receive_message()
                if msg_id is None:
                    # No message received, continue
                    time.sleep(0.1)
                    continue

                self.last_activity = time.time()

                # Handle message
                if msg_id == PeerWireProtocol.CHOKE:
                    self._handle_choke()
                elif msg_id == PeerWireProtocol.UNCHOKE:
                    self._handle_unchoke()
                elif msg_id == PeerWireProtocol.INTERESTED:
                    self._handle_interested()
                elif msg_id == PeerWireProtocol.NOT_INTERESTED:
                    self._handle_not_interested()
                elif msg_id == PeerWireProtocol.REQUEST:
                    self._handle_request(payload)
                elif msg_id == PeerWireProtocol.HAVE:
                    self._handle_have(payload)
                elif msg_id == PeerWireProtocol.BITFIELD:
                    self._handle_bitfield(payload)
                elif msg_id == PeerWireProtocol.CANCEL:
                    self._handle_cancel(payload)
                elif msg_id == PeerWireProtocol.PORT:
                    self._handle_port(payload)
                elif msg_id == PeerWireProtocol.EXTENDED:
                    self._handle_extended(payload)
                else:
                    self.logger.warning(
                        f"Received unknown message type {msg_id} from {self.protocol.peer}"
                    )

        except Exception as e:
            self.logger.error(f"Error in seed handler for {self.protocol.peer}: {e}")
        finally:
            self.logger.info(f"Seed handler for {self.protocol.peer} finished")
            self.protocol.close()

    def stop(self):
        """Stop the seed handler"""
        self.running = False

    def _handle_choke(self):
        """Handle CHOKE message"""
        self.logger.debug(f"Received CHOKE from {self.protocol.peer}")
        self.peer_choking = True

    def _handle_unchoke(self):
        """Handle UNCHOKE message"""
        self.logger.debug(f"Received UNCHOKE from {self.protocol.peer}")
        self.peer_choking = False

    def _handle_interested(self):
        """Handle INTERESTED message"""
        self.logger.debug(f"Received INTERESTED from {self.protocol.peer}")
        self.peer_interested = True

        # Unchoke the peer if they're interested and we're not already unchoking
        if not self.unchoked:
            self.protocol.send_unchoke()
            self.unchoked = True

    def _handle_not_interested(self):
        """Handle NOT_INTERESTED message"""
        self.logger.debug(f"Received NOT_INTERESTED from {self.protocol.peer}")
        self.peer_interested = False

        # Choke the peer if they're not interested and we're not already choking
        if self.unchoked:
            self.protocol.send_choke()
            self.unchoked = False

    def _handle_request(self, payload: bytes):
        """Handle REQUEST message"""
        if not self.unchoked or not self.peer_interested:
            self.logger.debug(
                f"Ignoring request from {self.protocol.peer} (unchoked: {self.unchoked}, interested: {self.peer_interested})"
            )
            return

        if len(payload) != 12:
            self.logger.warning(
                f"Invalid request message length from {self.protocol.peer}: {len(payload)}"
            )
            return

        # Parse the request
        try:
            index, begin, length = struct.unpack(">III", payload)
            self.logger.debug(
                f"Received request for piece {index}, begin {begin}, length {length} from {self.protocol.peer}"
            )

            # Validate request parameters
            if length > 2**14:  # 16KB max block size
                self.logger.warning(f"Requested block size too large: {length}")
                return

            # Check if we have the requested piece
            if not self.piece_manager.has_piece(index):
                self.logger.warning(f"Peer requested piece {index} which we don't have")
                return

            # Read the piece from disk
            piece_data = self.piece_manager.read_piece(index)
            if piece_data is None:
                self.logger.error(
                    f"Failed to read piece {index} for {self.protocol.peer}"
                )
                return

            # Validate request boundaries
            if begin + length > len(piece_data):
                self.logger.warning(
                    f"Invalid request: begin {begin} + length {length} > piece length {len(piece_data)}"
                )
                return

            # Send the requested block
            block = piece_data[begin : begin + length]
            if not self.protocol.send_piece(index, begin, block):
                self.logger.error(
                    f"Failed to send piece {index} to {self.protocol.peer}"
                )

        except Exception as e:
            self.logger.error(f"Error handling request from {self.protocol.peer}: {e}")

    def _handle_have(self, payload: bytes):
        """Handle HAVE message"""
        # As a seeder, we don't need to track what pieces the peer has
        self.logger.debug(f"Received HAVE from {self.protocol.peer}")

    def _handle_bitfield(self, payload: bytes):
        """Handle BITFIELD message"""
        # As a seeder, we don't need to track the peer's bitfield
        self.logger.debug(f"Received BITFIELD from {self.protocol.peer}")

    def _handle_cancel(self, payload: bytes):
        """Handle CANCEL message"""
        # We don't need to handle cancel requests as we serve requests immediately
        self.logger.debug(f"Received CANCEL from {self.protocol.peer}")

    def _handle_port(self, payload: bytes):
        """Handle PORT message (for DHT)"""
        self.logger.debug(f"Received PORT from {self.protocol.peer}")

    def _handle_extended(self, payload: bytes):
        """Handle EXTENDED message"""
        # We don't support extended messages yet
        self.logger.debug(f"Received EXTENDED from {self.protocol.peer}")


@logged
class PeerServer(threading.Thread):
    """Server that listens for incoming peer connections"""

    def __init__(
        self,
        port: int,
        torrent: TorrentFile,
        peer_id: bytes,
        piece_manager: PieceManager,
        connection_manager: ConnectionManager,
        is_seeder: bool = False,
    ):
        super().__init__()
        self.port = port
        self.torrent = torrent
        self.peer_id = peer_id
        self.piece_manager = piece_manager
        self.connection_manager = connection_manager
        self.is_seeder = is_seeder
        self.socket = None
        self.running = False
        self.daemon = True
        self.logger.info(f"PeerServer initialized on port {port}")

    def run(self):
        """Start the server and listen for incoming connections"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.bind(("0.0.0.0", self.port))
            self.socket.listen(5)
            self.running = True

            self.logger.info(f"Peer server listening on port {self.port}")

            while self.running:
                try:
                    client_socket, addr = self.socket.accept()
                    self.logger.info(f"Incoming connection from {addr}")

                    # Create a passive connection
                    connection = PassiveConnection(
                        client_socket,
                        addr,
                        self.torrent,
                        self.piece_manager,
                        self.peer_id,
                        self.is_seeder,
                    )

                    # Add to connection manager
                    if self.connection_manager.add_connection(connection):
                        connection.start()
                    else:
                        self.logger.warning(
                            f"Max connections reached, rejecting connection from {addr}"
                        )
                        client_socket.close()

                except socket.timeout:
                    continue
                except Exception as e:
                    if self.running:  # Only log if we're still running
                        self.logger.error(f"Error accepting connection: {e}")

        except Exception as e:
            self.logger.error(f"Peer server error: {e}")
        finally:
            if self.socket:
                self.socket.close()
            self.logger.info("Peer server stopped")

    def stop(self):
        """Stop the server"""
        self.running = False
        if self.socket:
            self.socket.close()

    def restart(self):
        self.stop()
        self.run()
