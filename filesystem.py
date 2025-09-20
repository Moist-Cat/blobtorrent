from typing import Tuple, Any
import hashlib
import math
from pathlib import Path
import binascii
import threading
from collections import OrderedDict

from log import logged, master as logger


class BencodeDecoder:
    @staticmethod
    def decode(data: bytes) -> Tuple[Any, int]:
        try:
            if data.startswith(b"i"):
                end_pos = data.find(b"e", 1)
                number = int(data[1:end_pos])
                #logger.debug(f"Decoded integer: {number}")
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
                #logger.debug(f"Decoded dictionary with {len(dict_data)} keys")
                return dict_data, pos + 1
            elif data[0:1] in b"0123456789":
                colon_pos = data.find(b":")
                length = int(data[:colon_pos])
                start = colon_pos + 1
                end = start + length
                string_data = data[start:end]
                #logger.debug(f"Decoded string of length {length}")
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
                encoded_elements = b"".join(
                    BencodeEncoder.encode(item) for item in data
                )
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


@logged
class FileCache:
    def __init__(self, max_files: int = 512):
        self.max_files = max_files
        self.cache = OrderedDict()
        self.lock = threading.Lock()

    def get_file(self, filepath: Path):
        with self.lock:
            if filepath in self.cache:
                # Move to end to mark as recently used
                self.cache.move_to_end(filepath)
                return self.cache[filepath]
            else:
                # If cache is full, remove least recently used
                if len(self.cache) >= self.max_files:
                    oldest_path, oldest_file = self.cache.popitem(last=False)
                    oldest_file.close()

                # Open new file and add to cache
                file_obj = open(filepath, "r+b")
                self.cache[filepath] = file_obj
                return file_obj

    def close_all(self):
        with self.lock:
            for file_obj in self.cache.values():
                file_obj.close()
            self.cache.clear()

    def __del__(self):
        self.close_all()


@logged
class TorrentFile:
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.logger.info(f"Loading torrent file: {filepath}")
        try:
            with open(filepath, "rb") as f:
                data = f.read()
            self.logger.info(f"Torrent file size: {len(data)} bytes")

            self.metadata = BencodeDecoder.decode_full(data)
            self.info = self.metadata[b"info"]
            self.info_hash = hashlib.sha1(BencodeEncoder.encode(self.info)).digest()
            self.logger.info(f"Info hash: {binascii.hexlify(self.info_hash).decode()}")

            self.piece_hashes = [
                self.info[b"pieces"][i : i + 20]
                for i in range(0, len(self.info[b"pieces"]), 20)
            ]
            self.piece_length = self.info[b"piece length"]

            if b"files" in self.info:
                self.total_size = self.info.get(
                    b"length", sum(f[b"length"] for f in self.info[b"files"])
                )
                self.logger.info(
                    f"Multi-file torrent with {len(self.info[b'files'])} files"
                )
            else:
                self.total_size = self.info[b"length"]
                self.logger.info(f"Single file torrent")

            self.num_pieces = math.ceil(self.total_size / self.piece_length)
            self.announce = self.metadata.get(b"announce").decode()

            self.logger.info(f"Total size: {self.total_size} bytes")
            self.logger.info(f"Piece length: {self.piece_length} bytes")
            self.logger.info(f"Number of pieces: {self.num_pieces}")
            self.logger.info(f"Announce URL: {self.announce}")

        except Exception as e:
            self.logger.error(f"Failed to load torrent file {filepath}: {e}")
            raise

    def __str__(self):
        return f"Torrent: {self.info.get(b'name', b'unknown').decode()}"
