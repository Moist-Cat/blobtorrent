"""
TrackerGossipConnection - Handles individual tracker-to-tracker connections.
"""

import threading
import time
import json
import binascii
import random
import socket
from typing import Dict, List, Tuple, Any, Optional, Callable
import logging
from dataclasses import asdict

from tracker.protocol import TrackerInfo, TrackerGossipProtocol

logger = logging.getLogger(__name__)


class TrackerGossipConnection:
    """
    Handles a single tracker-to-tracker connection.
    Manages communication, message handling, and connection lifecycle.
    """
    
    def __init__(
        self,
        protocol: TrackerGossipProtocol,
        on_message_callback: Callable[[TrackerGossipProtocol, int, bytes], None],
        on_closed_callback: Callable[[bytes], None],
        peer_id: bytes
    ):
        """
        Initialize a tracker gossip connection.
        
        Args:
            protocol: The gossip protocol instance for this connection
            on_message_callback: Callback for handling incoming messages
            on_closed_callback: Callback when connection is closed
            peer_id: Our own tracker ID
        """
        self.protocol = protocol
        self.on_message_callback = on_message_callback
        self.on_closed_callback = on_closed_callback
        self.peer_id = peer_id
        self.remote_peer_id = protocol.remote_peer_id
        self.is_running = False
        self.message_thread = None
        self.keepalive_thread = None
        self.last_activity = time.time()
        self.message_count = 0
        
        logger.debug(f"TrackerGossipConnection initialized for peer {protocol.peer}")
    
    def start(self):
        """Start the connection handler."""
        if self.is_running:
            return
        
        self.is_running = True
        
        # Start message handling thread
        self.message_thread = threading.Thread(
            target=self._message_loop,
            daemon=True
        )
        self.message_thread.start()
        
        # Start keepalive thread
        self.keepalive_thread = threading.Thread(
            target=self._keepalive_loop,
            daemon=True
        )
        self.keepalive_thread.start()
        
        logger.info(f"Tracker connection started to {self.protocol.peer}")
    
    def stop(self):
        """Stop the connection handler."""
        self.is_running = False
        
        # Wait for threads to finish
        if self.message_thread:
            self.message_thread.join(timeout=2.0)
        if self.keepalive_thread:
            self.keepalive_thread.join(timeout=2.0)

        self.on_closed_callback(self.protocol.remote_peer_id)
        
        # Close the protocol
        self.protocol.close()
        
        logger.info(f"Tracker connection stopped to {self.protocol.peer}")
    
    def send_message(self, message_id: int, payload: bytes = b"") -> bool:
        """Send a message through this connection."""
        try:
            success = self.protocol.send_message(message_id, payload)
            if success:
                self.last_activity = time.time()
                self.message_count += 1
            return success
        except Exception as e:
            logger.error(f"Failed to send message {message_id} to {self.protocol.peer}: {e}")
            self.close()
            return False
    
    def send_gossip_hello(self, tracker_info: TrackerInfo) -> bool:
        """Send HELLO message with tracker information."""
        try:
            data = json.dumps(tracker_info.to_dict()).encode('utf-8')
            return self.send_message(self.protocol.GOSSIP_HELLO, data)
        except Exception as e:
            logger.error(f"Failed to send HELLO to {self.protocol.peer}: {e}")
            return False
    
    def send_gossip_peer_list(self, info_hash: bytes, peers: List[Tuple[str, int]]) -> bool:
        """Send peer list for a specific torrent."""
        try:
            data = {
                'info_hash': binascii.hexlify(info_hash).decode(),
                'peers': [f"{ip}:{port}" for ip, port in peers],
                'timestamp': time.time()
            }
            payload = json.dumps(data).encode('utf-8')
            return self.send_message(self.protocol.GOSSIP_PEER_LIST, payload)
        except Exception as e:
            logger.error(f"Failed to send peer list to {self.protocol.peer}: {e}")
            return False
    
    def send_gossip_tracker_list(self, trackers: List[TrackerInfo]) -> bool:
        """Send list of known trackers."""
        try:
            data = {
                'trackers': [t.to_dict() for t in trackers],
                'timestamp': time.time()
            }
            payload = json.dumps(data).encode('utf-8')
            return self.send_message(self.protocol.GOSSIP_TRACKER_LIST, payload)
        except Exception as e:
            logger.error(f"Failed to send tracker list to {self.protocol.peer}: {e}")
            return False
    
    def send_gossip_stats(self, stats: Dict[str, Any]) -> bool:
        """Send statistics."""
        try:
            payload = json.dumps(stats).encode('utf-8')
            return self.send_message(self.protocol.GOSSIP_STATS, payload)
        except Exception as e:
            logger.error(f"Failed to send stats to {self.protocol.peer}: {e}")
            return False
    
    def send_gossip_ping(self) -> bool:
        """Send PING message."""
        try:
            payload = bytes([0])  # Simple payload
            return self.send_message(self.protocol.GOSSIP_PING, payload)
        except Exception as e:
            logger.error(f"Failed to send PING to {self.protocol.peer}: {e}")
            return False
    
    def send_gossip_pong(self) -> bool:
        """Send PONG response to PING."""
        try:
            payload = bytes([0])  # Simple payload
            return self.send_message(self.protocol.GOSSIP_PONG, payload)
        except Exception as e:
            logger.error(f"Failed to send PONG to {self.protocol.peer}: {e}")
            return False
    
    def send_keepalive(self) -> bool:
        """Send keepalive message."""
        return self.protocol.send_keep_alive()
    
    def close(self):
        """Close the connection."""
        self.stop()
    
    def _message_loop(self):
        """Main message handling loop."""
        while self.is_running and self.protocol.connected:
            try:
                # Receive messages
                message_id, payload = self.protocol.receive_message()
                
                if message_id is None:
                    # Connection closed or timeout
                    if self.is_running:
                        logger.debug(f"Connection closed by {self.protocol.peer}")
                    break
                
                # Update last activity
                self.last_activity = time.time()
                
                # Handle gossip messages
                if message_id >= 100:  # Custom gossip messages
                    self.on_message_callback(self.protocol, message_id, payload)
                else:
                    logger.debug(f"Received non-gossip message {message_id} from {self.protocol.peer}")
            
            except socket.timeout:
                # Normal timeout, continue
                continue
            except ConnectionError as e:
                logger.debug(f"Connection error with {self.protocol.peer}: {e}")
                break
            except Exception as e:
                logger.error(f"Error in message loop for {self.protocol.peer}: {e}")
                break
        
        # Connection ended
        self.close()
    
    def _keepalive_loop(self):
        """Send periodic keepalive messages."""
        while self.is_running and self.protocol.connected:
            try:
                time.sleep(30)  # Send keepalive every 30 seconds
                
                # Check if we've been inactive for too long
                if time.time() - self.last_activity > 60:
                    # Send keepalive
                    self.send_keepalive()
                    logger.debug(f"Sent keepalive to {self.protocol.peer}")
                    
                    # Also send PING occasionally
                    if random.random() < 0.3:  # 30% chance
                        self.send_gossip_ping()
            
            except Exception as e:
                logger.error(f"Error in keepalive loop for {self.protocol.peer}: {e}")
                break
    
    def is_active(self) -> bool:
        """Check if connection is still active."""
        return (self.is_running and 
                self.protocol.connected and 
                time.time() - self.last_activity < 300)  # 5 minutes timeout
    
    def get_stats(self) -> Dict[str, Any]:
        """Get connection statistics."""
        return {
            'peer': self.protocol.peer,
            'messages_sent': self.message_count,
            'last_activity': time.time() - self.last_activity,
            'is_active': self.is_active(),
            'connected': self.protocol.connected
        }

    def __str__(self):
        return f"<{self.__class__.__name__} ({self.peer=}, {self.message_count=} {self.last_activity=})>"
