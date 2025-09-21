import socket
import struct
import hashlib
import threading
from typing import Tuple, Any, Optional
import time
import math

from filesystem import TorrentFile
from network.protocol import PeerWireProtocol
from middleware import PieceManager
from log import logged


@logged
class PortManager:
    """Manages port allocation and verification"""

    #DEFAULT_PORT_RANGE = (6881, 6889)  # Default BitTorrent port range
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
            test_socket.bind(('0.0.0.0', port))
            test_socket.close()
            return True
        except socket.error:
            return False

    def allocate_port(self, preferred_port: Optional[int] = None) -> Optional[int]:
        """Allocate an available port, trying preferred port first, then the range"""
        # Try preferred port if specified
        if preferred_port and self.is_port_available(preferred_port) and preferred_port not in self.allocated_ports:
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
    
    def is_alive(self):
        """Check if the connection is still active"""
        return self.running and self.protocol.connected
    
    def close(self):
        """Close the connection"""
        self.stop()
        self.protocol.close()

@logged
class ActiveConnection(PeerConnection):
    """Handles active (outgoing) connections to peers"""

    def __init__(self, peer: Tuple[str, int], torrent: TorrentFile,
                 piece_manager: PieceManager, peer_id: bytes, is_seeder: bool = False):
        protocol = PeerWireProtocol(peer, torrent.info_hash, peer_id)
        super().__init__(protocol, piece_manager)
        self.peer = peer
        self.torrent = torrent
        self.is_seeder = is_seeder
        self.logger.info(f"ActiveConnection initialized for {peer} (seeder: {is_seeder})")

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
            self.handler = DownloadHandler(self.protocol, self.piece_manager, self.torrent)

        # Run the handler
        self.handler.run()

        self.logger.info(f"Active connection to {self.peer} finished")
        self.running = False


@logged
class PassiveConnection(PeerConnection):
    """Handles passive (incoming) connections from peers"""

    def __init__(self, socket: socket.socket, peer: Tuple[str, int], torrent: TorrentFile,
                 piece_manager: PieceManager, peer_id: bytes, is_seeder: bool = False):
        protocol = PeerWireProtocol(peer, torrent.info_hash, peer_id)
        super().__init__(protocol, piece_manager)
        self.socket = socket
        self.peer = peer
        self.torrent = torrent
        self.is_seeder = is_seeder
        self.logger.info(f"PassiveConnection initialized for {peer} (seeder: {is_seeder})")

    def run(self):
        """Handle an incoming connection and hand off to appropriate handler"""
        self.logger.info(f"Handling passive connection from {self.peer}")

        # Handle the incoming connection
        if not self.protocol.handle_incoming_connection(self.socket, self.peer):
            self.logger.warning(f"Failed to handle incoming connection from {self.peer}")
            self.running = False
            return

        # Hand off to appropriate handler based on our role
        if self.is_seeder:
            self.handler = SeedHandler(self.protocol, self.piece_manager)
        else:
            self.handler = DownloadHandler(self.protocol, self.piece_manager, self.torrent)

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
            self.active_connections = {conn for conn in self.active_connections if conn.is_alive()}
            return len(self.active_connections)

    def already_connected_to(self, peer: Tuple[str, int]):
        with self.lock:
            for connection in self.active_connections:
                if connection.peer == peer:
                    return True
            return False

@logged
class DownloadHandler:
    """Handles the downloading phase of a connection"""
    
    def __init__(self, protocol: PeerWireProtocol, piece_manager: PieceManager, torrent: TorrentFile):
        self.protocol = protocol
        self.piece_manager = piece_manager
        self.torrent = torrent
        self.unchoked = False
        self.bitfield = None
        self.current_piece = None
    
    def run(self):
        """Execute the downloading protocol"""
        self.logger.info(f"Starting download handler for {self.protocol.peer}")
        
        try:
            # Receive bitfield and update availability
            self._receive_bitfield()
            if self.bitfield is None:
                self.logger.warning(f"No bitfield received from {self.protocol.peer}")
                return
                
            self.piece_manager.update_availability(self.bitfield)
            self.protocol.send_interested()

            # Wait for unchoke
            self._wait_for_unchoke()

            # Download pieces until complete
            while not self.piece_manager.is_complete():
                if not self.unchoked:
                    self._wait_for_unchoke()
                    continue

                # Get a piece to download
                piece_index = self.piece_manager.get_rarest_piece()
                if piece_index == -1:
                    self.logger.info(f"No more pieces to download from {self.protocol.peer}")
                    break

                # Try to reserve the piece
                if not self.piece_manager.mark_downloading(piece_index):
                    self.logger.debug(f"Piece {piece_index} already being downloaded, trying another")
                    time.sleep(0.1)
                    continue

                self.current_piece = piece_index
                self.logger.info(f"Downloading piece {piece_index} from {self.protocol.peer}")

                # Download the piece
                success = self._download_piece(piece_index)

                # Unreserve the piece if download failed
                if not success:
                    self.piece_manager.unmark_downloading(piece_index)
                    self.current_piece = None
                    
        except Exception as e:
            self.logger.error(f"Error in download handler for {self.protocol.peer}: {e}")
            if self.current_piece is not None:
                self.piece_manager.unmark_downloading(self.current_piece)
        finally:
            self.logger.info(f"Download handler for {self.protocol.peer} finished")
    
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
                self.logger.info(f"Received UNCHOKE from {self.protocol.peer}")
                return
            elif msg_id == PeerWireProtocol.CHOKE:
                self.unchoked = False
                self.logger.info(f"Received CHOKE from {self.protocol.peer}")
            elif msg_id == PeerWireProtocol.HAVE:
                piece_index = struct.unpack(">I", payload)[0]
                self.logger.debug(f"Received HAVE for piece {piece_index}")
                # Update availability for this piece
                with self.piece_manager.lock:
                    if piece_index < len(self.piece_manager.piece_availability):
                        self.piece_manager.piece_availability[piece_index] += 1

        self.logger.warning(f"Timeout waiting for unchoke from {self.protocol.peer}")

    def _receive_bitfield(self):
        self.logger.info(f"Waiting for bitfield from {self.protocol.peer}")
        start_time = time.time()
        while time.time() - start_time < 30:  # 30 second timeout
            msg_id, payload = self.protocol.receive_message()
            if msg_id is None:
                time.sleep(0.1)
                continue

            if msg_id == PeerWireProtocol.BITFIELD:
                self.bitfield = payload
                self.logger.info(f"Received bitfield from {self.protocol.peer}, length: {len(payload)} bytes")
                return
            elif msg_id == PeerWireProtocol.UNCHOKE:
                self.unchoked = True
                self.logger.info(f"Received UNCHOKE from {self.protocol.peer}")
            elif msg_id == PeerWireProtocol.CHOKE:
                self.unchoked = False
                self.logger.info(f"Received CHOKE from {self.protocol.peer}")
            elif msg_id == PeerWireProtocol.HAVE:
                piece_index = struct.unpack(">I", payload)[0]
                self.logger.debug(f"Received HAVE for piece {piece_index}")

        self.logger.warning(f"Timeout waiting for bitfield from {self.protocol.peer}")

    def _download_piece(self, piece_index: int) -> bool:
        piece_length = self.torrent.piece_length
        if piece_index == self.torrent.num_pieces - 1:
            piece_length = self.torrent.total_size - piece_index * self.torrent.piece_length

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
                        self.logger.warning(f"Received wrong block: expected {piece_index}:{begin}, got {index}:{begin_offset}")
                        continue  # Not the block we're waiting for

                    # Critical: Check block length
                    if len(block_data) != block_length:
                        self.logger.warning(f"Incorrect block length: expected {block_length}, got {len(block_data)}")
                        continue

                    data += block_data
                    self.logger.debug(f"Received block {block_index+1}/{num_blocks} for piece {piece_index}")
                    block_received = True
                    break
                elif msg_id == PeerWireProtocol.CHOKE:
                    self.unchoked = False
                    self.logger.info(f"Choked by {self.protocol.peer} during download")
                    return False
                elif msg_id == PeerWireProtocol.UNCHOKE:
                    self.unchoked = True
                    self.logger.info(f"Unchoked by {self.protocol.peer}")
                elif msg_id == PeerWireProtocol.HAVE:
                    piece_idx = struct.unpack(">I", payload)[0]
                    self.logger.debug(f"Received HAVE for piece {piece_idx}")

            if not block_received:
                self.logger.warning(f"Timeout downloading block {block_index} of piece {piece_index} from {self.protocol.peer}")
                return False

        # Verify piece hash before saving
        computed_hash = hashlib.sha1(data).digest()
        expected_hash = self.torrent.piece_hashes[piece_index]
        if computed_hash != expected_hash:
            self.logger.error(f"Hash mismatch for piece {piece_index}. Expected {expected_hash.hex()}, got {computed_hash.hex()}")
            return False

        self.piece_manager.save_piece(piece_index, data)
        self.logger.info(f"Successfully downloaded and verified piece {piece_index} from {self.protocol.peer}")
        return True


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
                # Check for inactivity timeout
                if time.time() - self.last_activity > self.max_idle_time:
                    self.logger.info(f"Closing idle connection with {self.protocol.peer}")
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
                    self.logger.warning(f"Received unknown message type {msg_id} from {self.protocol.peer}")
                
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
            self.logger.debug(f"Ignoring request from {self.protocol.peer} (unchoked: {self.unchoked}, interested: {self.peer_interested})")
            return
        
        if len(payload) != 12:
            self.logger.warning(f"Invalid request message length from {self.protocol.peer}: {len(payload)}")
            return
        
        # Parse the request
        try:
            index, begin, length = struct.unpack(">III", payload)
            self.logger.debug(f"Received request for piece {index}, begin {begin}, length {length} from {self.protocol.peer}")
            
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
                self.logger.error(f"Failed to read piece {index} for {self.protocol.peer}")
                return
            
            # Validate request boundaries
            if begin + length > len(piece_data):
                self.logger.warning(f"Invalid request: begin {begin} + length {length} > piece length {len(piece_data)}")
                return
            
            # Send the requested block
            block = piece_data[begin:begin+length]
            if not self.protocol.send_piece(index, begin, block):
                self.logger.error(f"Failed to send piece {index} to {self.protocol.peer}")
            
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

    def __init__(self, port: int, torrent: TorrentFile, peer_id: bytes, piece_manager: PieceManager,
                 connection_manager: ConnectionManager, is_seeder: bool = False):
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
            self.socket.bind(('0.0.0.0', self.port))
            self.socket.listen(5)
            self.running = True

            self.logger.info(f"Peer server listening on port {self.port}")

            while self.running:
                try:
                    client_socket, addr = self.socket.accept()
                    self.logger.info(f"Incoming connection from {addr}")

                    # Create a passive connection
                    connection = PassiveConnection(
                        client_socket, addr, self.torrent, self.piece_manager,
                        self.peer_id, self.is_seeder
                    )

                    # Add to connection manager
                    if self.connection_manager.add_connection(connection):
                        connection.start()
                    else:
                        self.logger.warning(f"Max connections reached, rejecting connection from {addr}")
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
