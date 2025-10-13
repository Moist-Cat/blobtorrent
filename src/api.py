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
        app.router.add_get("/api/torrents/{info_hash}/files", self.list_files)
        app.router.add_post(
            "/api/torrents/{info_hash}/files/{file_index}/priority",
            self.set_file_priority,
        )

        # Tracker operations
        app.router.add_get("/api/torrents/{info_hash}/trackers", self.list_trackers)
        app.router.add_post(
            "/api/torrents/{info_hash}/trackers/{tracker_url}/enable",
            self.enable_tracker,
        )
        app.router.add_post(
            "/api/torrents/{info_hash}/trackers/{tracker_url}/disable",
            self.disable_tracker,
        )

        # System information
        app.router.add_get("/api/system/info", self.system_info)
        app.router.add_get("/api/system/stats", self.system_stats)

        # File system operations
        app.router.add_get("/api/fs/list", self.list_directory)
        app.router.add_delete("/api/fs/delete", self.delete_file)

        # Start server
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        await site.start()

        logger.info(f"REST API server started on {self.host}:{self.port}")
        await self._load_existing_torrents()

        # Keep the server running
        await asyncio.Future()

    async def _load_existing_torrents(self):
        """Load existing torrents from data directory"""
        torrent_files = list(self.data_directory.glob("**/*.torrent"))
        for torrent_file in torrent_files:
            try:
                # Extract info_hash from filename or parse torrent
                info_hash = await self._get_torrent_info_hash(torrent_file)
                if info_hash:
                    self.active_torrents[info_hash] = {
                        "file_path": str(torrent_file),
                        "name": torrent_file.stem,
                        "added_time": time.time(),
                        "status": "stopped",
                    }
            except Exception as e:
                logger.error(f"Error loading torrent {torrent_file}: {e}")

    async def _get_torrent_info_hash(self, torrent_path: Path) -> Optional[str]:
        """Extract info hash from torrent file"""
        try:
            # This would use your existing TorrentFile class
            # For now, using a simple hash of the file path
            return hashlib.sha1(str(torrent_path).encode()).hexdigest()[:20]
        except:
            return None

    # Torrent Management Endpoints
    async def list_torrents(self, request):
        """List all active torrents"""
        torrents = []
        for info_hash, data in self.active_torrents.items():
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

        if info_hash not in self.active_torrents:
            return web.Response(status=404, text="Torrent not found")

        torrent_info = await self._get_torrent_info(
            info_hash, self.active_torrents[info_hash]
        )
        return web.json_response(torrent_info)

    async def start_torrent(self, request):
        """Start a torrent"""
        info_hash = request.match_info["info_hash"]

        if info_hash not in self.active_torrents:
            return web.Response(status=404, text="Torrent not found")

        # Start the torrent using client manager
        if self.client_manager:
            await self.client_manager.start_torrent(info_hash)

        self.active_torrents[info_hash]["status"] = "downloading"

        return web.json_response({"status": "started"})

    async def pause_torrent(self, request):
        """Pause a torrent"""
        info_hash = request.match_info["info_hash"]

        if info_hash not in self.active_torrents:
            return web.Response(status=404, text="Torrent not found")

        if self.client_manager:
            await self.client_manager.pause_torrent(info_hash)

        self.active_torrents[info_hash]["status"] = "paused"

        return web.json_response({"status": "paused"})

    async def stop_torrent(self, request):
        """Stop a torrent"""
        info_hash = request.match_info["info_hash"]

        if info_hash not in self.active_torrents:
            return web.Response(status=404, text="Torrent not found")

        if self.client_manager:
            await self.client_manager.stop_torrent(info_hash)

        self.active_torrents[info_hash]["status"] = "stopped"

        return web.json_response({"status": "stopped"})

    async def remove_torrent(self, request):
        """Remove a torrent"""
        info_hash = request.match_info["info_hash"]
        data = await request.json()
        delete_files = data.get("delete_files", False)

        if info_hash not in self.active_torrents:
            return web.Response(status=404, text="Torrent not found")

        if self.client_manager:
            await self.client_manager.remove_torrent(info_hash, delete_files)

        if delete_files:
            # Delete downloaded files
            torrent_data = self.active_torrents[info_hash]
            download_path = Path(torrent_data.get("download_path", ""))
            if download_path.exists():
                import shutil

                shutil.rmtree(download_path)

        del self.active_torrents[info_hash]

        return web.json_response({"status": "removed"})

    async def check_torrent(self, request):
        """Check torrent hash"""
        info_hash = request.match_info["info_hash"]

        if info_hash not in self.active_torrents:
            return web.Response(status=404, text="Torrent not found")

        # Implement hash checking logic
        if self.client_manager:
            await self.client_manager.check_torrent_hash(info_hash)

        return web.json_response({"status": "checking"})

    # File Operations
    async def list_files(self, request):
        """List files in a torrent"""
        info_hash = binascii.unhexlify(request.match_info["info_hash"])

        if info_hash not in self.active_torrents:
            return web.Response(status=404, text="Torrent not found")

        files = await self._get_torrent_files(info_hash)
        return web.json_response({"files": files})

    async def set_file_priority(self, request):
        """Set file priority"""
        info_hash = binascii.unhexlify(request.match_info["info_hash"])
        file_index = int(request.match_info["file_index"])

        data = await request.json()
        priority = data.get("priority", 1)  # 0=skip, 1=normal, 2=high

        if info_hash not in self.active_torrents:
            return web.Response(status=404, text="Torrent not found")

        if self.client_manager:
            await self.client_manager.set_file_priority(info_hash, file_index, priority)

        return web.json_response({"status": "priority_set"})

    # Tracker Operations
    async def list_trackers(self, request):
        """List trackers for a torrent"""
        info_hash = binascii.unhexlify(request.match_info["info_hash"])

        if info_hash not in self.active_torrents:
            return web.Response(status=404, text="Torrent not found")

        trackers = self.client_manager.get_trackers(info_hash)
        return web.json_response({"trackers": trackers})

    async def enable_tracker(self, request):
        """Enable a tracker"""
        info_hash = binascii.unhexlify(request.match_info["info_hash"])

        tracker_url = request.match_info["tracker_url"]

        if info_hash not in self.active_torrents:
            return web.Response(status=404, text="Torrent not found")

        if self.client_manager:
            await self.client_manager.enable_tracker(info_hash, tracker_url)

        return web.json_response({"status": "enabled"})

    async def disable_tracker(self, request):
        """Disable a tracker"""
        info_hash = binascii.unhexlify(request.match_info["info_hash"])
        tracker_url = request.match_info["tracker_url"]

        if info_hash not in self.active_torrents:
            return web.Response(status=404, text="Torrent not found")

        if self.client_manager:
            await self.client_manager.disable_tracker(info_hash, tracker_url)

        return web.json_response({"status": "disabled"})

    # System Information
    async def system_info(self, request):
        """Get system information"""
        return web.json_response(
            {
                "client": "BlobTorrent",
                "version": "0.1.0",
                "api_version": "1.0",
                "data_directory": str(self.data_directory),
                "active_torrents": len(self.active_torrents),
            }
        )

    async def system_stats(self, request):
        """Get system statistics"""
        total_download = 0
        total_upload = 0
        total_size = 0
        total_completed = 0

        for info_hash, data in self.active_torrents.items():
            torrent_info = await self._get_torrent_info(info_hash, data)
            total_download += torrent_info.get("down_total", 0)
            total_upload += torrent_info.get("up_total", 0)
            total_size += torrent_info.get("size_bytes", 0)
            total_completed += torrent_info.get("completed_bytes", 0)

        return web.json_response(
            {
                "total_download": total_download,
                "total_upload": total_upload,
                "total_size": total_size,
                "total_completed": total_completed,
                "active_torrents": len(self.active_torrents),
                "download_speed": 0,  # Would track real-time speed
                "upload_speed": 0,  # Would track real-time speed
            }
        )

    # File System Operations
    async def list_directory(self, request):
        """List files in a directory"""
        directory = request.query.get("path", str(self.data_directory))
        path = Path(directory)

        if not path.exists():
            return web.Response(status=404, text="Directory not found")

        files = []
        for item in path.iterdir():
            files.append(
                {
                    "name": item.name,
                    "path": str(item),
                    "size": item.stat().st_size if item.is_file() else 0,
                    "is_directory": item.is_dir(),
                    "modified": item.stat().st_mtime,
                }
            )

        return web.json_response({"files": files})

    async def delete_file(self, request):
        """Delete a file or directory"""
        data = await request.json()
        file_path = data.get("path")

        if not file_path:
            return web.Response(status=400, text="Path is required")

        path = Path(file_path)
        if not path.exists():
            return web.Response(status=404, text="File not found")

        try:
            if path.is_file():
                path.unlink()
            else:
                import shutil

                shutil.rmtree(path)

            return web.json_response({"status": "deleted"})
        except Exception as e:
            return web.Response(status=500, text=str(e))

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
        """Add a torrent file from base64 data"""
        try:
            # Decode base64
            torrent_data = base64.b64decode(torrent_b64)

            # Save to temporary file to parse
            with tempfile.NamedTemporaryFile(delete=False, suffix=".torrent") as f:
                f.write(torrent_data)
                temp_path = f.name

            try:
                # Parse torrent file to get info hash
                # This would use your existing TorrentFile class
                info_hash = await self._get_torrent_info_hash(Path(temp_path))

                if not info_hash:
                    raise ValueError("Could not parse torrent file")

                # Save torrent file to data directory
                torrent_filename = f"{info_hash}.torrent"
                torrent_path = self.data_directory / torrent_filename
                with open(torrent_path, "wb") as f:
                    f.write(torrent_data)

                # Add to active torrents
                self.active_torrents[info_hash] = {
                    "file_path": str(torrent_path),
                    "download_path": download_dir,
                    "added_time": time.time(),
                    "status": "stopped",
                    "name": torrent_path.stem,
                }

                return info_hash

            finally:
                # Clean up temporary file
                if os.path.exists(temp_path):
                    os.unlink(temp_path)

        except Exception as e:
            raise ValueError(f"Invalid torrent file: {e}")

    async def _get_torrent_info(
        self, info_hash: str, torrent_data: Dict
    ) -> Dict[str, Any]:
        """Get detailed torrent information (rTorrent compatible)"""
        return {
            "info_hash": binascii.hexlify(info_hash).decode(),
            "name": torrent_data.get("name", "Unknown"),
            "size_bytes": torrent_data["size_bytes"],
            "completed_bytes": 0,
            "up_total": 0,
            "down_total": 0,
            "up_rate": 0,
            "down_rate": 0,
            "ratio": 0.0,
            "peers_connected": torrent_data["connected_peers"],
            "peers_accounted": torrent_data["connected_peers"],
            "state": torrent_data.get("status", "stopped"),
            "message": torrent_data.get("message", ""),
            "added_time": torrent_data.get("added_time", 0),
            "download_path": torrent_data.get("download_path", ""),
        }

    async def get_torrent_files(self, info_hash: str) -> List[Dict]:
        """Get detailed file information for Flood"""
        if info_hash not in self.active_torrents:
            return []

        files = []
        torrent_data = self.active_torrents[info_hash]

        if self.client_manager:
            client_files = await self.client_manager.get_torrent_files(info_hash)
            for i, file_info in enumerate(client_files):
                files.append(
                    {
                        "index": i,
                        "path": file_info.get("path", ""),
                        "size_bytes": file_info.get("size", 0),
                        "completed_chunks": file_info.get("completed_pieces", 0),
                        "priority": file_info.get("priority", 1),
                        "is_open": True,
                        "frozen_path": file_info.get("path", ""),
                        "range_first": 0,
                        "range_second": file_info.get("size", 0),
                        "offset": 0,
                    }
                )

        return files

    async def get_torrent_trackers(self, info_hash: str) -> List[Dict]:
        """Get detailed tracker information for Flood"""
        if info_hash not in self.active_torrents:
            return []

        trackers = []
        torrent_data = self.active_torrents[info_hash]

        if self.client_manager:
            client_trackers = await self.client_manager.get_torrent_trackers(info_hash)
            for i, tracker_info in enumerate(client_trackers):
                trackers.append(
                    {
                        "index": i,
                        "url": tracker_info.get("url", ""),
                        "is_enabled": tracker_info.get("enabled", True),
                        "scrape_complete": tracker_info.get("seeders", 0),
                        "scrape_incomplete": tracker_info.get("leechers", 0),
                        "scrape_downloaded": tracker_info.get("downloads", 0),
                        "last_announce": tracker_info.get("last_announce", 0),
                        "last_scrape": tracker_info.get("last_scrape", 0),
                        "min_interval": tracker_info.get("min_interval", 0),
                        "normal_interval": tracker_info.get("normal_interval", 0),
                        "scrape_state": 2 if tracker_info.get("enabled") else 0,
                        "announce_state": 2 if tracker_info.get("enabled") else 0,
                        "is_usable": tracker_info.get("enabled", True),
                        "is_busy": False,
                        "is_extra": False,
                        "type": "http"
                        if tracker_info.get("url", "").startswith("http")
                        else "udp",
                    }
                )

        return trackers


async def main():
    """Main function to start the API server"""
    api_server = TorrentAPIServer(client_manager=TorrentManager())
    await api_server.start()


if __name__ == "__main__":
    asyncio.run(main())
