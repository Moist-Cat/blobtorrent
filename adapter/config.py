import os
import subprocess
from pathlib import Path

SOCK_DIR = Path(__file__).parent / "socks"

hostname = subprocess.run(["hostname"], capture_output=True, text=True).stdout
hostname = hostname.strip()

# SCGI server configuration
class Config:
    SCGI_HOST = os.getenv("SCGI_HOST", "0.0.0.0")
    SCGI_PORT = int(os.getenv("SCGI_PORT", "5000"))
    # SCGI_SOCKET = os.getenv("SCGI_SOCKET", str(SOCK_DIR / f"{hostname}.sock"))
    SCGI_SOCKET = None

    BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000")
