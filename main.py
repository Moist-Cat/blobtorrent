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

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class BencodeDecoder:
    @staticmethod
    def decode(data: bytes) -> Tuple[Any, int]:
        if data.startswith(b"i"):
            end_pos = data.find(b"e", 1)
            number = int(data[1:end_pos])
            return number, end_pos + 1
        elif data.startswith(b"l"):
            list_data = []
            pos = 1
            while data[pos : pos + 1] != b"e":
                item, length = BencodeDecoder.decode(data[pos:])
                list_data.append(item)
                pos += length
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
            return dict_data, pos + 1
        elif data[0:1] in b"0123456789":
            colon_pos = data.find(b":")
            length = int(data[:colon_pos])
            start = colon_pos + 1
            end = start + length
            string_data = data[start:end]
            return string_data, end
        else:
            raise ValueError("Invalid bencoded data")

    @staticmethod
    def decode_full(data: bytes) -> Any:
        result, length = BencodeDecoder.decode(data)
        return result


class BencodeEncoder:
    @staticmethod
    def encode(data: Any) -> bytes:
        if isinstance(data, int):
            return f"i{data}e".encode()
        elif isinstance(data, bytes):
            return f"{len(data)}:".encode() + data
        elif isinstance(data, str):
            return BencodeEncoder.encode(data.encode())
        elif isinstance(data, list):
            encoded_elements = b"".join(BencodeEncoder.encode(item) for item in data)
            return b"l" + encoded_elements + b"e"
        elif isinstance(data, dict):
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


class TorrentFile:
    def __init__(self, filepath: str):
        self.filepath = filepath
        with open(filepath, "rb") as f:
            data = f.read()
        self.metadata = BencodeDecoder.decode_full(data)
        self.info = self.metadata[b"info"]
        self.info_hash = hashlib.sha1(BencodeEncoder.encode(self.info)).digest()
        self.piece_hashes = [
            self.info[b"pieces"][i : i + 20]
            for i in range(0, len(self.info[b"pieces"]), 20)
        ]
        self.piece_length = self.info[b"piece length"]
        if b"files" in self.info:
            self.total_size = self.info.get(
                b"length", sum(f[b"length"] for f in self.info[b"files"])
            )
        else:
            self.total_size = self.info[b"length"]
        self.num_pieces = math.ceil(self.total_size / self.piece_length)
        self.announce = self.metadata.get(b"announce").decode()

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

    def announce(self) -> List[Tuple[str, int]]:
        url = self.torrent.announce
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
                response = requests.get(url, params=query_params, headers=headers)
                if not response.ok:
                    logger.error(f"Tracker response: {response}")
                    return []
                response_data = response.content
                decoded_response = BencodeDecoder.decode_full(response_data)
                peers = decoded_response.get(b"peers", b"")
                peer_list = []
                if isinstance(peers, bytes):
                    for i in range(0, len(peers), 6):
                        ip = socket.inet_ntoa(peers[i : i + 4])
                        port = struct.unpack("!H", peers[i + 4 : i + 6])[0]
                        peer_list.append((ip, port))
                elif isinstance(peers, list):
                    for peer in peers:
                        ip, port = peer[b"ip"], peer[b"port"]
                        peer_list.append((ip, port))
                else:
                    raise Exception(peers)
                return peer_list
            else:
                print(parsed)
                logger.error("Only HTTP trackers are supported")
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

    def __init__(self, peer: Tuple[str, int], info_hash: bytes, peer_id: bytes):
        self.peer = peer
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connected = False
        self.bitfield = None
        self.handshake_done = False

    def connect(self) -> bool:
        try:
            self.socket.settimeout(10)
            self.socket.connect(self.peer)
            self._handshake()
            self.connected = True
            return True
        except Exception as e:
            logger.error(f"Failed to connect to peer {self.peer}: {e}")
            return False

    def _handshake(self):
        handshake_msg = (
            b"\x13BitTorrent protocol" + b"\x00" * 8 + self.info_hash + self.peer_id
        )
        self.socket.send(handshake_msg)
        response = self.socket.recv(68)
        print(response)
        if (
            len(response) != 68
            or response[0:1] != b"\x13"
            or response[1:20] != b"BitTorrent protocol"
            or response[28:48] != self.info_hash
        ):
            raise ConnectionError("Invalid handshake response")
        self.handshake_done = True

    def receive_message(self) -> Tuple[int, bytes]:
        length_prefix = self.socket.recv(4)
        if not length_prefix:
            return None, None
        length = struct.unpack(">I", length_prefix)[0]
        if length == 0:
            return None, None
        message_id = self.socket.recv(1)[0]
        payload = self.socket.recv(length - 1) if length > 1 else b""
        return message_id, payload

    def send_interested(self):
        msg = struct.pack(">Ib", 1, self.INTERESTED)
        self.socket.send(msg)

    def send_request(self, index: int, begin: int, length: int):
        msg = struct.pack(">IbIII", 13, self.REQUEST, index, begin, length)
        self.socket.send(msg)

    def send_unchoke(self):
        msg = struct.pack(">Ib", 1, self.UNCHOKE)
        self.socket.send(msg)

    def close(self):
        self.socket.close()
        self.connected = False


class PieceManager:
    def __init__(self, torrent: TorrentFile, output_dir: str = "."):
        self.torrent = torrent
        self.pieces = [None] * torrent.num_pieces
        self.downloaded_pieces = [False] * torrent.num_pieces
        self.piece_availability = [0] * torrent.num_pieces
        self.lock = threading.Lock()
        self.output_dir = output_dir
        self.file = open(os.path.join(output_dir, torrent.info[b"name"].decode()), "wb")
        self.file.truncate(torrent.total_size)

    def update_availability(self, bitfield: bytes):
        with self.lock:
            for i in range(self.torrent.num_pieces):
                byte_index = i // 8
                bit_index = i % 8
                if (
                    byte_index < len(bitfield)
                    and bitfield[byte_index] >> (7 - bit_index) & 1
                ):
                    self.piece_availability[i] += 1

    def get_rarest_piece(self) -> int:
        with self.lock:
            available_pieces = [
                i
                for i in range(self.torrent.num_pieces)
                if not self.downloaded_pieces[i] and self.piece_availability[i] > 0
            ]
            if not available_pieces:
                return -1
            rarest = min(available_pieces, key=lambda i: self.piece_availability[i])
            return rarest

    def mark_downloading(self, piece_index: int):
        with self.lock:
            pass  # Could track currently downloading pieces

    def save_piece(self, piece_index: int, data: bytes):
        with self.lock:
            if self.downloaded_pieces[piece_index]:
                return
            expected_hash = self.torrent.piece_hashes[piece_index]
            actual_hash = hashlib.sha1(data).digest()
            if expected_hash != actual_hash:
                logger.error(f"Piece {piece_index} hash mismatch")
                return
            offset = piece_index * self.torrent.piece_length
            self.file.seek(offset)
            self.file.write(data)
            self.file.flush()
            self.downloaded_pieces[piece_index] = True
            logger.info(f"Piece {piece_index} downloaded and verified")

    def is_complete(self) -> bool:
        with self.lock:
            return all(self.downloaded_pieces)

    def close(self):
        self.file.close()


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

    def run(self):
        if not self.protocol.connect():
            return
        logger.info(f"Connected to peer {self.peer}")
        self._receive_bitfield()
        self.piece_manager.update_availability(self.bitfield)
        self.protocol.send_interested()
        while not self.piece_manager.is_complete():
            if not self.unchoked:
                time.sleep(1)
                continue
            piece_index = self.piece_manager.get_rarest_piece()
            if piece_index == -1:
                break
            self._download_piece(piece_index)
        self.protocol.close()

    def _receive_bitfield(self):
        while True:
            msg_id, payload = self.protocol.receive_message()
            if msg_id is None:
                continue
            if msg_id == PeerWireProtocol.BITFIELD:
                self.bitfield = payload
                break
            elif msg_id == PeerWireProtocol.UNCHOKE:
                self.unchoked = True
            elif msg_id == PeerWireProtocol.CHOKE:
                self.unchoked = False

    def _download_piece(self, piece_index: int):
        piece_length = self.torrent.piece_length
        if piece_index == self.torrent.num_pieces - 1:
            piece_length = (
                self.torrent.total_size - piece_index * self.torrent.piece_length
            )
        block_size = 2**14
        num_blocks = math.ceil(piece_length / block_size)
        data = b""
        for block_index in range(num_blocks):
            begin = block_index * block_size
            block_length = min(block_size, piece_length - begin)
            self.protocol.send_request(piece_index, begin, block_length)
            while True:
                msg_id, payload = self.protocol.receive_message()
                if msg_id == PeerWireProtocol.PIECE:
                    index, begin_offset = struct.unpack(">II", payload[:8])
                    block_data = payload[8:]
                    if index == piece_index and begin_offset == begin:
                        data += block_data
                        break
                elif msg_id == PeerWireProtocol.CHOKE:
                    self.unchoked = False
                elif msg_id == PeerWireProtocol.UNCHOKE:
                    self.unchoked = True
        self.piece_manager.save_piece(piece_index, data)


class BitTorrentClient:
    def __init__(self, torrent_path: str, output_dir: str = "."):
        self.torrent = TorrentFile(torrent_path)
        self.peer_id = self._generate_peer_id()
        self.output_dir = output_dir
        self.piece_manager = PieceManager(self.torrent, output_dir)
        self.downloaders = []

    def _generate_peer_id(self) -> bytes:
        return b"-PY0001-" + os.urandom(12)

    def start(self):
        logger.info(f"Starting download for {self.torrent}")
        tracker = Tracker(self.torrent, self.peer_id)
        peers = tracker.announce()
        logger.info(f"Discovered {len(peers)} peers")
        for peer in peers:
            downloader = PeerDownloader(
                peer, self.torrent, self.piece_manager, self.peer_id
            )
            downloader.start()
            self.downloaders.append(downloader)
        for downloader in self.downloaders:
            downloader.join()
        self.piece_manager.close()
        logger.info("Download complete")


def main():
    parser = argparse.ArgumentParser(description="BitTorrent Client")
    parser.add_argument("torrent", help="Path to the torrent file")
    parser.add_argument(
        "--output-dir", default=".", help="Output directory for downloaded file"
    )
    args = parser.parse_args()
    client = BitTorrentClient(args.torrent, args.output_dir)
    client.start()


if __name__ == "__main__":
    main()
