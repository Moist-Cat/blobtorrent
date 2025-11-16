#!/usr/bin/env python3
"""
REST API for BlobTorrent client - Provides rTorrent-compatible interface
"""

import binascii
import asyncio
import aiohttp
from aiohttp import web
import json
import logging
import base64
import tempfile
import os
import hashlib
from typing import Dict, List, Optional, Any
from pathlib import Path
import time
import base64

from client import TorrentManager

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("blobtorrent-api")


class TorrentAPIServer:
    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8080,
        data_directory: str = "/out",
        client_manager=None,
    ):
        self.host = host
        self.port = port
        self.data_directory = Path(data_directory)
        self.client_manager = client_manager
        self.active_torrents: Dict[
            str, Dict
        ] = self.client_manager.get_torrents()  # info_hash -> torrent data

    async def start(self):
        """Start the REST API server"""
        app = web.Application()

        # Torrent management endpoints
        app.router.add_get("/api/torrents", self.list_torrents)
        app.router.add_post("/api/torrents", self.add_torrent)
        app.router.add_get("/api/torrents/{info_hash}", self.get_torrent)
        app.router.add_post("/api/torrents/{info_hash}/start", self.start_torrent)
        app.router.add_post("/api/torrents/{info_hash}/pause", self.pause_torrent)
        app.router.add_post("/api/torrents/{info_hash}/stop", self.stop_torrent)
        app.router.add_post("/api/torrents/{info_hash}/remove", self.remove_torrent)
        app.router.add_post("/api/torrents/{info_hash}/check", self.check_torrent)

        # File operations
        app.router.add_post(
            "/api/torrents/{info_hash}/files/{file_index}/priority",
            self.set_file_priority,
        )

        # Start server
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        await site.start()

        logger.info(f"REST API server started on {self.host}:{self.port}")

        # Keep the server running
        await asyncio.Future()

    # Torrent Management Endpoints
    async def list_torrents(self, request):
        """List all active torrents"""
        torrents = []
        for info_hash, data in self.client_manager.get_torrents().items():
            torrent_info = await self._get_torrent_info(info_hash, data)
            torrents.append(torrent_info)

        return web.json_response({"torrents": torrents, "count": len(torrents)})

    async def add_torrent(self, request):
        """Add a new torrent via magnet link or base64 .torrent file"""
        try:
            data = await request.json()
            magnet_link = data.get("magnet")
            torrent_b64 = data.get("torrent")
            download_dir = data.get("download_dir", str(self.data_directory))

            if magnet_link:
                info_hash = await self._add_magnet_torrent(magnet_link, download_dir)
            elif torrent_b64:
                info_hash = await self._add_torrent_file(torrent_b64, download_dir)
            else:
                return web.Response(
                    status=400, text="Either magnet or torrent must be provided"
                )

            return web.json_response(
                {"info_hash": info_hash, "status": "added"}, status=201
            )

        except Exception as e:
            logger.error(f"Error adding torrent: {e}")
            return web.Response(status=400, text=str(e))

    async def get_torrent(self, request):
        """Get detailed information about a specific torrent"""
        info_hash = request.match_info["info_hash"]

        active_torrents = self.client_manager.get_torrents()

        if info_hash not in active_torrents:
            return web.Response(status=404, text="Torrent not found")

        torrent_info = await self._get_torrent_info(
            info_hash, active_torrents[info_hash]
        )
        return web.json_response(torrent_info)

    async def start_torrent(self, request):
        """Start a torrent"""
        info_hash = request.match_info["info_hash"]
        active_torrents = self.client_manager.get_torrents()

        if info_hash not in active_torrents:
            return web.Response(status=404, text="Torrent not found")

        # Start the torrent using client manager
        if self.client_manager:
            self.client_manager.perform_action("resume", info_hash)

        return web.json_response({"status": "started"})

    async def pause_torrent(self, request):
        """Pause a torrent"""
        info_hash = request.match_info["info_hash"]
        active_torrents = self.client_manager.get_torrents()

        if info_hash not in active_torrents:
            return web.Response(status=404, text="Torrent not found")

        if self.client_manager:
            self.client_manager.perform_action("pause", info_hash)

        return web.json_response({"status": "paused"})

    async def stop_torrent(self, request):
        """Stop a torrent"""
        info_hash = request.match_info["info_hash"]
        active_torrents = self.client_manager.get_torrents()

        if info_hash not in active_torrents:
            return web.Response(status=404, text="Torrent not found")

        if self.client_manager:
            self.client_manager.perform_action("stop", info_hash)

        return web.json_response({"status": "stopped"})

    async def remove_torrent(self, request):
        """Remove a torrent"""
        info_hash = request.match_info["info_hash"]
        data = await request.json()
        delete_files = data.get("delete_files", False)
        active_torrents = self.client_manager.get_torrents()

        if info_hash not in active_torrents:
            return web.Response(status=404, text="Torrent not found")

        if self.client_manager:
            self.client_manager.remove_torrent(info_hash, delete_files)

        return web.json_response({"status": "removed"})

    async def check_torrent(self, request):
        """Check torrent hash"""
        info_hash = request.match_info["info_hash"]
        active_torrents = self.client_manager.get_torrents()

        if info_hash not in active_torrents:
            return web.Response(status=404, text="Torrent not found")

        if self.client_manager:
            self.client_manager.perform_action("check_hash", info_hash)

        return web.json_response({"status": "checking"})

    # File Operations
    async def set_file_priority(self, request):
        """Set file priority"""
        info_hash = request.match_info["info_hash"]
        file_index = int(request.match_info["file_index"])

        data = await request.json()
        priority = data.get("priority", 1)  # 0=skip, 1=normal, 2=high

        if info_hash not in self.active_torrents:
            return web.Response(status=404, text="Torrent not found")

        if self.client_manager:
            await self.client_manager.set_file_priority(info_hash, file_index, priority)

        return web.json_response({"status": "priority_set"})

    # Helper methods
    async def _add_magnet_torrent(self, magnet_link: str, download_dir: str) -> str:
        """Add a magnet link torrent"""
        # Extract info hash from magnet link
        # magnet:?xt=urn:btih:INFO_HASH
        if not magnet_link.startswith("magnet:"):
            raise ValueError("Invalid magnet link")

        # Parse magnet link to extract info hash
        import urllib.parse

        parsed = urllib.parse.urlparse(magnet_link)
        params = urllib.parse.parse_qs(parsed.query)

        xt = params.get("xt", [])
        if not xt:
            raise ValueError("No xt parameter in magnet link")

        info_hash = xt[0].split(":")[-1].lower()

        # Add to active torrents
        self.active_torrents[info_hash] = {
            "magnet_link": magnet_link,
            "download_path": download_dir,
            "added_time": time.time(),
            "status": "stopped",
            "name": params.get("dn", ["Unknown"])[0],
        }

        return info_hash

    async def _add_torrent_file(self, torrent_b64: str, download_dir: str) -> str:
        torrent = base64.b64decode(torrent_b64)
        return self.client_manager.add_torrent(torrent, download_dir)

    async def _get_torrent_info(
        self, info_hash: str, torrent_data: Dict
    ) -> Dict[str, Any]:
        """Get enhanced torrent information for web UI"""
        # Convert bytes info_hash to string if needed
        if isinstance(info_hash, bytes):
            info_hash_str = info_hash.hex()
        else:
            info_hash_str = info_hash

        torrent_data["info_hash"] = info_hash_str

        return torrent_data


async def main():
    """Main function to start the API server"""
    api_server = TorrentAPIServer(
        port=8000, data_directory=os.getenv("OUTPUT_DIR", "/out"), client_manager=TorrentManager()
    )
    await api_server.start()


if __name__ == "__main__":
    asyncio.run(main())
