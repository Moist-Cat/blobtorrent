import argparse
import binascii
import collections
import hashlib
import json
import logging
import math
import os
import random
import socket
import struct
import threading
import time
import urllib.parse
import requests
from typing import Any, Dict, List, Tuple

# Set up logging with more detailed format
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("bittorrent_client.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class BencodeDecoder:
    @staticmethod
    def decode(data: bytes) -> Tuple[Any, int]:
        try:
            if data.startswith(b"i"):
                end_pos = data.find(b"e", 1)
                number = int(data[1:end_pos])
                logger.debug(f"Decoded integer: {number}")
                return number, end_pos + 1
            elif data.startswith(b"l"):
                list_data = []
                pos = 1
                while data[pos : pos + 1] != b"e":
                    item, length = BencodeDecoder.decode(data[pos:])
                    list_data.append(item)
                    pos += length
                logger.debug(f"Decoded list with {len(list_data)} items")
                return list_data, pos + 1
            elif data.startswith(b"d"):
                dict_data = {}
                pos = 1
                while data[pos : pos + 1] != b"e":
                    key, key_len = BencodeDecoder.decode(data[pos:])
                    pos += key_len
                    value, value_len = BencodeDecoder.decode(data[pos:])
                    pos += value_len
                    dict_data[key] = value
                logger.debug(f"Decoded dictionary with {len(dict_data)} keys")
                return dict_data, pos + 1
            elif data[0:1] in b"0123456789":
                colon_pos = data.find(b":")
                length = int(data[:colon_pos])
                start = colon_pos + 1
                end = start + length
                string_data = data[start:end]
                logger.debug(f"Decoded string of length {length}")
                return string_data, end
            else:
                raise ValueError("Invalid bencoded data")
        except Exception as e:
            logger.error(f"Error in BencodeDecoder.decode: {e}")
            raise

    @staticmethod
    def decode_full(data: bytes) -> Any:
        logger.debug(f"Decoding bencoded data of length {len(data)}")
        result, length = BencodeDecoder.decode(data)
        logger.debug(f"Successfully decoded data, result type: {type(result)}")
        return result


class BencodeEncoder:
    @staticmethod
    def encode(data: Any) -> bytes:
        try:
            if isinstance(data, int):
                logger.debug(f"Encoding integer: {data}")
                return f"i{data}e".encode()
            elif isinstance(data, bytes):
                logger.debug(f"Encoding bytes of length {len(data)}")
                return f"{len(data)}:".encode() + data
            elif isinstance(data, str):
                logger.debug(f"Encoding string: {data}")
                return BencodeEncoder.encode(data.encode())
            elif isinstance(data, list):
                logger.debug(f"Encoding list with {len(data)} items")
                encoded_elements = b"".join(BencodeEncoder.encode(item) for item in data)
                return b"l" + encoded_elements + b"e"
            elif isinstance(data, dict):
                logger.debug(f"Encoding dictionary with {len(data)} keys")
                encoded_dict = b""
                for key in sorted(data.keys()):
                    if not isinstance(key, bytes):
                        raise TypeError("Dictionary keys must be bytes")
                    encoded_dict += BencodeEncoder.encode(key) + BencodeEncoder.encode(
                        data[key]
                    )
                return b"d" + encoded_dict + b"e"
            else:
                raise TypeError(f"Type {type(data)} not supported")
        except Exception as e:
            logger.error(f"Error in BencodeEncoder.encode: {e}")
            raise


class TorrentFile:
    def __init__(self, filepath: str):
        self.filepath = filepath
        logger.info(f"Loading torrent file: {filepath}")
        try:
            with open(filepath, "rb") as f:
                data = f.read()
            logger.info(f"Torrent file size: {len(data)} bytes")
            
            self.metadata = BencodeDecoder.decode_full(data)
            self.info = self.metadata[b"info"]
            self.info_hash = hashlib.sha1(BencodeEncoder.encode(self.info)).digest()
            logger.info(f"Info hash: {binascii.hexlify(self.info_hash).decode()}")
            
            self.piece_hashes = [
                self.info[b"pieces"][i : i + 20]
                for i in range(0, len(self.info[b"pieces"]), 20)
            ]
            self.piece_length = self.info[b"piece length"]
            
            if b"files" in self.info:
                self.total_size = self.info.get(
                    b"length", sum(f[b"length"] for f in self.info[b"files"])
                )
                logger.info(f"Multi-file torrent with {len(self.info[b'files'])} files")
            else:
                self.total_size = self.info[b"length"]
                logger.info(f"Single file torrent")
            
            self.num_pieces = math.ceil(self.total_size / self.piece_length)
            self.announce = self.metadata.get(b"announce").decode()
            
            logger.info(f"Total size: {self.total_size} bytes")
            logger.info(f"Piece length: {self.piece_length} bytes")
            logger.info(f"Number of pieces: {self.num_pieces}")
            logger.info(f"Announce URL: {self.announce}")
            
        except Exception as e:
            logger.error(f"Failed to load torrent file {filepath}: {e}")
            raise

    def __str__(self):
        return f"Torrent: {self.info.get(b'name', b'unknown').decode()}"


class Tracker:
    def __init__(self, torrent: TorrentFile, peer_id: bytes, port: int = 6881):
        self.torrent = torrent
        self.peer_id = peer_id
        self.port = port
        self.uploaded = 0
        self.downloaded = 0
        self.left = torrent.total_size
        logger.info(f"Tracker initialized for {torrent}")

    def announce(self) -> List[Tuple[str, int]]:
        url = self.torrent.announce
        logger.info(f"Announcing to tracker: {url}")
        parsed = urllib.parse.urlparse(url)
        query_params = {
            "info_hash": self.torrent.info_hash,
            "peer_id": self.peer_id,
            "port": self.port,
            "uploaded": self.uploaded,
            "downloaded": self.downloaded,
            "left": self.left,
            "compact": 1,
        }
        headers = {
            "User-Agent": "BlobTorrent/0.0.1",
        }
        try:
            if parsed.scheme.startswith("http"):
                logger.debug(f"Sending tracker request with params: {query_params}")
                response = requests.get(url, params=query_params, headers=headers, timeout=30)
                
                if not response.ok:
                    logger.error(f"Tracker responded with error: {response.status_code} - {response.reason}")
                    return []
                
                response_data = response.content
                logger.debug(f"Tracker response size: {len(response_data)} bytes")
                decoded_response = BencodeDecoder.decode_full(response_data)
                logger.info(f"Tracker response: {decoded_response.keys()}")
                
                peers = decoded_response.get(b"peers", b"")
                peer_list = []
                
                if isinstance(peers, bytes):
                    logger.info(f"Received compact peer list with {len(peers)//6} peers")
                    for i in range(0, len(peers), 6):
                        ip = socket.inet_ntoa(peers[i : i + 4])
                        port = struct.unpack("!H", peers[i + 4 : i + 6])[0]
                        peer_list.append((ip, port))
                        logger.debug(f"Discovered peer: {ip}:{port}")
                
                elif isinstance(peers, list):
                    logger.info(f"Received non-compact peer list with {len(peers)} peers")
                    for peer in peers:
                        ip, port = peer[b"ip"], peer[b"port"]
                        peer_list.append((ip, port))
                        logger.debug(f"Discovered peer: {ip}:{port}")
                else:
                    logger.warning(f"Unknown peer format: {type(peers)}")
                    raise Exception(f"Unknown peer format: {type(peers)}")
                
                logger.info(f"Successfully discovered {len(peer_list)} peers from tracker")
                return peer_list
            else:
                logger.error(f"Unsupported tracker scheme: {parsed.scheme}")
                return []
        except requests.exceptions.Timeout:
            logger.error(f"Tracker request timed out: {url}")
            return []
        except requests.exceptions.RequestException as e:
            logger.error(f"Tracker request failed: {e}")
            return []
        except Exception as e:
            logger.error(f"Error during tracker announce: {e}")
            return []


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
        8: "CANCEL"
    }

    def __init__(self, peer: Tuple[str, int], info_hash: bytes, peer_id: bytes):
        self.peer = peer
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connected = False
        self.bitfield = None
        self.handshake_done = False
        logger.debug(f"PeerWireProtocol initialized for {peer}")

    def connect(self) -> bool:
        try:
            logger.info(f"Connecting to peer {self.peer}")
            self.socket.settimeout(10)
            self.socket.connect(self.peer)
            logger.info(f"Connected to peer {self.peer}")
            self._handshake()
            self.connected = True
            return True
        except socket.timeout:
            logger.warning(f"Connection timeout to peer {self.peer}")
            return False
        except ConnectionRefusedError:
            logger.warning(f"Connection refused by peer {self.peer}")
            return False
        except Exception as e:
            logger.error(f"Failed to connect to peer {self.peer}: {e}")
            return False

    def _handshake(self):
        try:
            handshake_msg = (
                b"\x13BitTorrent protocol" + b"\x00" * 8 + self.info_hash + self.peer_id
            )
            logger.debug(f"Sending handshake to {self.peer}")
            self.socket.send(handshake_msg)
            response = self.socket.recv(68)
            logger.debug(f"Received handshake response of length {len(response)}")
            
            if (
                len(response) != 68
                or response[0:1] != b"\x13"
                or response[1:20] != b"BitTorrent protocol"
                or response[28:48] != self.info_hash
            ):
                logger.error(f"Invalid handshake response from {self.peer}")
                raise ConnectionError("Invalid handshake response")
            
            self.handshake_done = True
            logger.info(f"Handshake successful with {self.peer}")
        except Exception as e:
            logger.error(f"Handshake failed with {self.peer}: {e}")
            raise

    def _recv_exact(self, n: int) -> bytes:
        """Read exactly n bytes from the socket, handling partial reads."""
        data = b''
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
                logger.debug("Received keep-alive message")
                return None, None

            # Read exactly 'length' bytes for the message body
            message_body = self._recv_exact(length)
            message_id = message_body[0]  # first byte is message ID
            payload = message_body[1:]    # rest is payload

            message_name = self.MESSAGE_NAMES.get(message_id, f"UNKNOWN({message_id})")
            logger.debug(f"Received message: {message_name}, length: {length}, payload: {len(payload)} bytes")

            return message_id, payload
        except socket.timeout:
            logger.warning(f"Timeout while receiving message from {self.peer}")
            return None, None
        except ConnectionError as e:
            logger.error(f"Connection error from {self.peer}: {e}")
            return None, None
        except Exception as e:
            logger.error(f"Error receiving message from {self.peer}: {e}")
            return None, None

    def send_interested(self):
        try:
            msg = struct.pack(">Ib", 1, self.INTERESTED)
            logger.debug("Sending INTERESTED message...")
            #msg = struct.pack("!IB", 1, self.INTERESTED)
            self.socket.send(msg)
            logger.debug("Sent INTERESTED message")
        except Exception as e:
            logger.error(f"Failed to send INTERESTED message: {e}")

    def send_request(self, index: int, begin: int, length: int):
        try:
            msg = struct.pack(">IbIII", 13, self.REQUEST, index, begin, length)
            self.socket.send(msg)
            logger.debug(f"Sent REQUEST for piece {index}, offset {begin}, length {length}")
        except Exception as e:
            logger.error(f"Failed to send REQUEST message: {e}")

    def send_unchoke(self):
        try:
            msg = struct.pack(">Ib", 1, self.UNCHOKE)
            self.socket.send(msg)
            logger.debug("Sent UNCHOKE message")
        except Exception as e:
            logger.error(f"Failed to send UNCHOKE message: {e}")

    def close(self):
        try:
            self.socket.close()
            self.connected = False
            logger.info(f"Closed connection to peer {self.peer}")
        except Exception as e:
            logger.error(f"Error closing connection to {self.peer}: {e}")


class PieceManager:
    def __init__(self, torrent: TorrentFile, output_dir: str = "."):
        self.torrent = torrent
        self.pieces = [None] * torrent.num_pieces
        self.downloaded_pieces = [False] * torrent.num_pieces
        self.piece_availability = [0] * torrent.num_pieces
        self.lock = threading.Lock()
        self.output_dir = output_dir
        
        output_filename = os.path.join(output_dir, torrent.info[b"name"].decode())
        logger.info(f"Creating output file: {output_filename}")
        
        try:
            os.makedirs(output_dir, exist_ok=True)
            self.file = open(output_filename, "wb")
            self.file.truncate(torrent.total_size)
            logger.info(f"Output file initialized with size {torrent.total_size} bytes")
        except Exception as e:
            logger.error(f"Failed to initialize output file: {e}")
            raise

    def update_availability(self, bitfield: bytes):
        with self.lock:
            logger.debug(f"Updating piece availability from bitfield of length {len(bitfield)}")
            for i in range(self.torrent.num_pieces):
                byte_index = i // 8
                bit_index = i % 8
                if (
                    byte_index < len(bitfield)
                    and bitfield[byte_index] >> (7 - bit_index) & 1
                ):
                    self.piece_availability[i] += 1
            logger.debug(f"Piece availability updated. Available pieces: {sum(1 for x in self.piece_availability if x > 0)}/{self.torrent.num_pieces}")

    def get_rarest_piece(self) -> int:
        with self.lock:
            available_pieces = [
                i
                for i in range(self.torrent.num_pieces)
                if not self.downloaded_pieces[i] and self.piece_availability[i] > 0
            ]
            if not available_pieces:
                logger.debug("No available pieces to download")
                return -1
            rarest = min(available_pieces, key=lambda i: self.piece_availability[i])
            logger.debug(f"Selected rarest piece: {rarest} (availability: {self.piece_availability[rarest]})")
            return rarest

    def mark_downloading(self, piece_index: int):
        with self.lock:
            logger.debug(f"Marking piece {piece_index} as downloading")

    def save_piece(self, piece_index: int, data: bytes):
        with self.lock:
            if self.downloaded_pieces[piece_index]:
                logger.warning(f"Piece {piece_index} already downloaded, skipping")
                return
            
            logger.info(f"Verifying piece {piece_index} (size: {len(data)} bytes)")
            expected_hash = self.torrent.piece_hashes[piece_index]
            actual_hash = hashlib.sha1(data).digest()
            
            if expected_hash != actual_hash:
                logger.error(f"Piece {piece_index} hash mismatch! Expected: {binascii.hexlify(expected_hash).decode()}, Got: {binascii.hexlify(actual_hash).decode()}")
                return
            
            try:
                offset = piece_index * self.torrent.piece_length
                self.file.seek(offset)
                self.file.write(data)
                self.file.flush()
                self.downloaded_pieces[piece_index] = True
                
                downloaded_count = sum(self.downloaded_pieces)
                progress = (downloaded_count / self.torrent.num_pieces) * 100
                logger.info(f"Piece {piece_index} downloaded and verified successfully. Progress: {progress:.1f}% ({downloaded_count}/{self.torrent.num_pieces} pieces)")
            except Exception as e:
                logger.error(f"Failed to save piece {piece_index}: {e}")

    def is_complete(self) -> bool:
        with self.lock:
            complete = all(self.downloaded_pieces)
            if complete:
                logger.info("All pieces downloaded successfully!")
        return complete

    def close(self):
        try:
            self.file.close()
            logger.info("Output file closed successfully")
        except Exception as e:
            logger.error(f"Error closing output file: {e}")


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
        logger.info(f"PeerDownloader initialized for {peer}")

    def run(self):
        logger.info(f"Starting downloader for peer {self.peer}")
        if not self.protocol.connect():
            logger.warning(f"Failed to connect to peer {self.peer}, stopping")
            return
        
        try:
            self._receive_bitfield()
            if self.bitfield is None:
                logger.warning(f"No bitfield received from {self.peer}, stopping")
                return
                
            self.piece_manager.update_availability(self.bitfield)
            self.protocol.send_interested()


            # unchoke
            self._receive_bitfield()
            
            logger.info(f"Starting download loop with {self.peer}")
            while not self.piece_manager.is_complete():
                if not self.unchoked:
                    self._receive_bitfield()
                    logger.debug(f"Waiting for unchoke from {self.peer}")
                    time.sleep(1)
                    continue
                
                piece_index = self.piece_manager.get_rarest_piece()
                if piece_index == -1:
                    logger.info(f"No more pieces to download from {self.peer}")
                    break
                
                logger.info(f"Downloading piece {piece_index} from {self.peer}")
                self._download_piece(piece_index)
                
        except Exception as e:
            logger.error(f"Error in downloader for {self.peer}: {e}")
        finally:
            self.protocol.close()
            logger.info(f"Downloader for {self.peer} finished")

    def _receive_bitfield(self):
        logger.info(f"Waiting for bitfield from {self.peer}")
        start_time = time.time()
        while time.time() - start_time < 30:  # 30 second timeout
            msg_id, payload = self.protocol.receive_message()
            if msg_id is None:
                time.sleep(0.1)
                continue
                
            if msg_id == PeerWireProtocol.BITFIELD:
                self.bitfield = payload
                logger.info(f"Received bitfield from {self.peer}, length: {len(payload)} bytes")
                return
            elif msg_id == PeerWireProtocol.UNCHOKE:
                self.unchoked = True
                logger.info(f"Received UNCHOKE from {self.peer}")
            elif msg_id == PeerWireProtocol.CHOKE:
                self.unchoked = False
                logger.info(f"Received CHOKE from {self.peer}")
            elif msg_id == PeerWireProtocol.HAVE:
                logger.debug(f"Received HAVE message from {self.peer}")
        
        logger.warning(f"Timeout waiting for bitfield from {self.peer}")

    def _download_piece(self, piece_index: int):
        piece_length = self.torrent.piece_length
        if piece_index == self.torrent.num_pieces - 1:
            piece_length = (
                self.torrent.total_size - piece_index * self.torrent.piece_length
            )
        
        block_size = 2**14
        num_blocks = math.ceil(piece_length / block_size)
        data = b""
        
        logger.debug(f"Downloading piece {piece_index} with {num_blocks} blocks")
        
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
                        continue  # Not the block we're waiting for
                    
                    # Critical: Check block length
                    if len(block_data) != block_length:
                        logger.warning(f"Incorrect block length: expected {block_length}, got {len(block_data)}")
                        continue
                    
                    data += block_data
                    logger.debug(f"Received block {block_index+1}/{num_blocks} for piece {piece_index}")
                    block_received = True
                    break
                elif msg_id == PeerWireProtocol.CHOKE:
                    self.unchoked = False
                    logger.info(f"Choked by {self.peer} during download")
                elif msg_id == PeerWireProtocol.UNCHOKE:
                    self.unchoked = True
                    logger.info(f"Unchoked by {self.peer}")
                elif msg_id == PeerWireProtocol.HAVE:
                    logger.debug(f"Received HAVE during download")
            
            if not block_received:
                logger.warning(f"Timeout downloading block {block_index} of piece {piece_index} from {self.peer}")
                # Consider retrying the block instead of failing the entire piece
                return
        
        # Verify piece hash before saving
        computed_hash = hashlib.sha1(data).digest()
        expected_hash = self.torrent.piece_hashes[piece_index]
        if computed_hash != expected_hash:
            logger.error(f"Hash mismatch for piece {piece_index}. Expected {expected_hash.hex()}, got {computed_hash.hex()}")
            return  # Or implement retry logic for the entire piece
        
        self.piece_manager.save_piece(piece_index, data)
        logger.info(f"Successfully downloaded and verified piece {piece_index}")

class BitTorrentClient:
    def __init__(self, torrent_path: str, output_dir: str = "."):
        logger.info(f"Initializing BitTorrentClient with torrent: {torrent_path}")
        self.torrent = TorrentFile(torrent_path)
        self.peer_id = self._generate_peer_id()
        self.output_dir = output_dir
        self.piece_manager = PieceManager(self.torrent, output_dir)
        self.downloaders = []
        logger.info(f"Client peer ID: {binascii.hexlify(self.peer_id).decode()}")

    def _generate_peer_id(self) -> bytes:
        peer_id = b"-PY0001-" + os.urandom(12)
        logger.debug(f"Generated peer ID: {binascii.hexlify(peer_id).decode()}")
        return peer_id

    def start(self):
        logger.info(f"Starting download for {self.torrent}")
        start_time = time.time()
        
        try:
            tracker = Tracker(self.torrent, self.peer_id)
            peers = tracker.announce()
            
            if not peers:
                logger.error("No peers discovered from tracker")
                return
            
            logger.info(f"Discovered {len(peers)} peers, starting downloaders")
            
            random.shuffle(peers)
            # Limit concurrent connections to avoid overwhelming the system
            max_connections = min(10, len(peers))
            #max_connections = min(1, len(peers))
            selected_peers = peers[:max_connections]
            
            for peer in selected_peers:
                downloader = PeerDownloader(
                    peer, self.torrent, self.piece_manager, self.peer_id
                )
                downloader.start()
                self.downloaders.append(downloader)
                logger.info(f"Started downloader for peer: {peer}")
            
            # Monitor progress
            while not self.piece_manager.is_complete():
                downloaded = sum(self.piece_manager.downloaded_pieces)
                total = self.torrent.num_pieces
                progress = (downloaded / total) * 100
                
                if downloaded % 5 == 0 or downloaded == total:  # Log every 5 pieces
                    logger.info(f"Download progress: {progress:.1f}% ({downloaded}/{total} pieces)")
                
                time.sleep(5)  # Check every 5 seconds
            
            # Wait for all downloaders to finish
            for downloader in self.downloaders:
                downloader.join(timeout=10)
            
            download_time = time.time() - start_time
            speed = self.torrent.total_size / download_time / 1024 / 1024  # MB/s
            logger.info(f"Download completed in {download_time:.2f} seconds ({speed:.2f} MB/s)")
            
        except KeyboardInterrupt:
            logger.info("Download interrupted by user")
        except Exception as e:
            logger.error(f"Error during download: {e}")
        finally:
            self.piece_manager.close()
            logger.info("BitTorrent client shutdown complete")


def main():
    parser = argparse.ArgumentParser(description="BitTorrent Client")
    parser.add_argument("torrent", help="Path to the torrent file")
    parser.add_argument(
        "--output-dir", default=".", help="Output directory for downloaded file"
    )
    parser.add_argument(
        "--log-level", default="DEBUG", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Set the logging level"
    )
    
    args = parser.parse_args()
    
    # Set log level
    logger.setLevel(getattr(logging, args.log_level.upper()))
    
    try:
        client = BitTorrentClient(args.torrent, args.output_dir)
        client.start()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise


if __name__ == "__main__":
    main()
