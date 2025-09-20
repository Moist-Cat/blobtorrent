import argparse
import logging

from client import BitTorrentClient
from log import master


def main():
    parser = argparse.ArgumentParser(description="BitTorrent Client")
    parser.add_argument("torrent", help="Path to the torrent file")
    parser.add_argument(
        "--output-dir", default=".", help="Output directory for downloaded file"
    )
    parser.add_argument(
        "--log-level",
        default="DEBUG",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Set the logging level",
    )

    args = parser.parse_args()

    # Set log level
    master.setLevel(getattr(logging, args.log_level.upper()))

    try:
        client = BitTorrentClient(args.torrent, args.output_dir)
        client.start()
    except Exception as e:
        master.error(f"Fatal error: {e}")
        raise


if __name__ == "__main__":
    main()
