import os

# SCGI server configuration
class Config:
    SCGI_HOST = os.getenv("SCGI_HOST", "localhost")
    SCGI_PORT = int(os.getenv("SCGI_PORT", "5000"))

    BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000")
