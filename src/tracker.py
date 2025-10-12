#!/usr/bin/env python3
"""
Custom tracker service for Docker Swarm environment.
Provides HTTP-based tracker functionality with DNS-based peer discovery.
"""

import binascii
import asyncio
import aiohttp
from aiohttp import web
import time
import logging
import socket
from typing import Dict, List, Tuple, Set
import json
from urllib.parse import unquote, unquote_to_bytes

from filesystem import BencodeEncoder

# Configure logging
logger = logging.getLogger("blobtorrent-tracker")


class TrackerService:
    def __init__(self, host: str = "0.0.0.0", port: int = 6969):
        self.host = host
        self.port = port
        self.peers: Dict[
            bytes, Set[Tuple[str, int]]
        ] = {}  # info_hash -> set of (ip, port)
        self.last_announce: Dict[
            Tuple[bytes, str, int], float
        ] = {}  # (info_hash, ip, port) -> timestamp
        self.cleanup_interval = 300  # Clean up every 5 minutes
        self.peer_timeout = 1800  # Remove peers after 30 minutes of inactivity

    async def start(self):
        """Start the tracker service"""
        # Start cleanup task
        asyncio.create_task(self._cleanup_task())

        # Create web application
        app = web.Application()
        app.router.add_get("/announce", self.handle_announce)
        app.router.add_get("/health", self.handle_health)
        app.router.add_get("/stats", self.handle_stats)

        # Start server
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        await site.start()

        logger.info(f"Tracker service started on {self.host}:{self.port}")

        # Keep the server running
        await asyncio.Future()

    async def handle_announce(self, request):
        """Handle announce requests from clients"""
        try:
            # Parse query parameters - handle info_hash as raw bytes
            query_string = request.query_string

            # Extract info_hash from query string manually
            info_hash = None
            peer_id = None
            port = 6881
            uploaded = 0
            downloaded = 0
            left = 0
            event = ""
            compact = 1

            # Parse query string manually to handle URL-encoded binary data
            for part in query_string.split("&"):
                if "=" in part:
                    key, value = part.split("=", 1)
                    if key == "info_hash":
                        # URL-decode the value and convert to bytes
                        info_hash = unquote_to_bytes(value)
                    elif key == "peer_id":
                        peer_id = unquote(value)
                    elif key == "port":
                        port = int(value)
                    elif key == "uploaded":
                        uploaded = int(value)
                    elif key == "downloaded":
                        downloaded = int(value)
                    elif key == "left":
                        left = int(value)
                    elif key == "event":
                        event = value
                    elif key == "compact":
                        compact = int(value)

            if info_hash is None:
                return web.Response(status=400, text="Missing info_hash parameter")

            # Get client IP (handle proxy headers if needed)
            client_ip = request.remote
            if "X-Forwarded-For" in request.headers:
                client_ip = request.headers["X-Forwarded-For"].split(",")[0].strip()

            logger.info(
                f"Announce from {client_ip}:{port} for info_hash {info_hash.hex()}"
            )

            # Update peer information
            if info_hash not in self.peers:
                self.peers[info_hash] = set()

            peer_key = (info_hash, client_ip, port)
            self.last_announce[peer_key] = time.time()

            # Add peer if it's a new announcement or "started" event
            if event in ("started", "", "completed", None):
                self.peers[info_hash].add((client_ip, port))
                logger.info(
                    f"Added peer {client_ip}:{port} for info_hash {info_hash.hex()}"
                )
            elif event == "stopped":
                self.peers[info_hash].discard((client_ip, port))
                logger.info(
                    f"Removed peer {client_ip}:{port} for info_hash {info_hash.hex()}"
                )

            # Get peers for response (exclude the requesting peer)
            peers_list = list(self.peers.get(info_hash, set()))
            peers_list = [p for p in peers_list if p != (client_ip, port)]

            # Prepare response
            response_data = {
                # b'interval': 1800,  # 30 minutes
                b"interval": 30,  # 30 minutes
                b"complete": len(
                    [p for p in peers_list if p[1] == 100]
                ),  # Fake seeder count
                b"incomplete": len(peers_list),  # Leecher count
                b"peers": self._format_peers(peers_list, compact),
            }

            return web.Response(
                body=BencodeEncoder.encode(response_data),
                content_type="application/octet-stream",
            )

        except Exception as e:
            logger.error(f"Error handling announce: {e}")
            return web.Response(status=400, text=str(e))

    def _format_peers(self, peers: List[Tuple[str, int]], compact: int) -> bytes:
        """Format peers list in compact or dictionary format"""
        if compact:
            # Compact format: <ip (4 bytes)><port (2 bytes)>
            compact_peers = b""
            for ip, port in peers:
                try:
                    compact_peers += socket.inet_aton(ip) + port.to_bytes(2, "big")
                except (socket.error, OSError):
                    continue  # Skip invalid IP addresses
            return compact_peers
        else:
            # Dictionary format
            return [{"ip": ip, "port": port} for ip, port in peers]

    async def handle_health(self, request):
        """Health check endpoint"""
        return web.json_response(
            {
                "status": "healthy",
                "timestamp": time.time(),
                "tracked_torrents": len(self.peers),
                "total_peers": sum(len(peers) for peers in self.peers.values()),
            }
        )

    async def handle_stats(self, request):
        """Statistics endpoint"""
        stats = {"torrents": {}, "total_peers": 0, "total_torrents": len(self.peers)}

        for info_hash, peers in self.peers.items():
            stats["torrents"][info_hash.hex()] = {
                "peers": len(peers),
                "peer_list": [f"{ip}:{port}" for ip, port in peers],
            }
            stats["total_peers"] += len(peers)

        return web.json_response(stats)

    async def _cleanup_task(self):
        """Periodically clean up stale peers"""
        while True:
            await asyncio.sleep(self.cleanup_interval)
            try:
                current_time = time.time()
                stale_peers = []

                for (info_hash, ip, port), last_seen in self.last_announce.items():
                    if current_time - last_seen > self.peer_timeout:
                        stale_peers.append((info_hash, ip, port))

                for info_hash, ip, port in stale_peers:
                    if info_hash in self.peers:
                        self.peers[info_hash].discard((ip, port))
                        if not self.peers[info_hash]:
                            del self.peers[info_hash]
                    del self.last_announce[(info_hash, ip, port)]

                logger.info(f"Cleaned up {len(stale_peers)} stale peers")

            except Exception as e:
                logger.error(f"Error in cleanup task: {e}")


async def main():
    """Main function to start the tracker service"""
    tracker = TrackerService()
    await tracker.start()


if __name__ == "__main__":
    asyncio.run(main())
