# method_mapper.py
import logging
from typing import Any, List, Dict, Optional
import requests
import json

# Configure logging
logging.basicConfig(level=0)
logger = logging.getLogger("method-mapper")

class MethodMapper:
    def __init__(self, backend_url: str):
        self.backend_url = backend_url.rstrip('/')
        self.session = requests.Session()
        
    def call_backend(self, endpoint: str, method: str = 'GET', data: Dict = None) -> Any:
        """Generic method to call the backend API with proper error handling"""
        url = f"{self.backend_url}/{endpoint.lstrip('/')}"
        
        try:
            logger.debug(f"Backend API call: {method} {url} - Data: {data}")
            
            if method.upper() == 'GET':
                response = self.session.get(url, params=data, timeout=30)
            elif method.upper() == 'POST':
                response = self.session.post(url, json=data, timeout=30)
            elif method.upper() == 'DELETE':
                response = self.session.delete(url, json=data, timeout=30)
            else:
                response = self.session.request(method.upper(), url, json=data, timeout=30)
                
            response.raise_for_status()
            result = response.json()
            logger.debug(f"Backend API response: {result}")
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Backend API error calling {url}: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Backend API JSON decode error: {e}")
            return None

    def _dispatch(self, method: str, params: List) -> Any:
        """Main dispatch method that handles all XML-RPC calls"""
        real_method = method.replace(".", "_")
        logger.info(f"XML-RPC call: {method} with params: {params}")
        
        if not hasattr(self, real_method):
            logger.error(f"Method {method} not supported in Mapper")
            raise Exception(f"Method {method} not supported")
        
        try:
            result = getattr(self, real_method)(*params)
            logger.info(f"XML-RPC result for {method}: {result}")
            return result
        except Exception as e:
            logger.error(f"Error executing {method}: {e}")
            raise

    # System methods
    def _listMethods(self) -> List[str]:
        """Return list of available methods - Flood expects this"""
        methods = [
            # System methods
            'system.listMethods', 'system.methodSignature', 'system.methodHelp', #'system.multicall',
            
            # Torrent listing and info
            'd.multicall2', 'd.name', 'd.hash', 'd.size_bytes',
            'd.directory',
            'd.completed_bytes', 'd.down.rate', 'd.up.rate',
            'd.peers_connected', 'd.state', 'd.ratio', 'd.bytes_done', 'd.message',
            'd.base_path',
            'd.base_filename',
            'd.down.total', 'd.up.total',
            'd.left_bytes', 'd.is_active', 'd.complete', 'd.is_private',

            'd.peers_accounted', 'd.peers_not_connected',
            'd.creation_date',
            'd.custom1', 'd.custom2', 'd.priority',
            'd.size_chunks', 'd.completed_chunks',
            'd.chunk_size',
            'd.hashing', 'd.ignore_commands',
            'd.local_id', 'd.connection_current', 'd.connection_leech',
            'd.connection_seed', 'd.tied_to_file', 'd.bitfield',
            'd.tracker_focus', 'd.tracker_numwant',
            'd.activity_time_last',
            'd.activity_time_seen', 'd.throttle_name', 'd.is_hash_checking',
            'd.is_hash_checked', 'd.is_open', 'd.is_multi_file',
            
            # Torrent control methods
            'd.erase', 'd.pause', 'd.resume', 'd.stop', 'd.close', 'd.check_hash',
            
            # File methods
            #'f.multicall', 'f.path', 'f.size_bytes', 'f.completed_chunks',
            #'f.priority', 'f.set_priority',
            
            # Tracker methods  
            #'t.multicall', 't.url', 't.is_enabled', 't.scrape_complete',
            #'t.scrape_incomplete', 't.scrape_downloaded', 't.group',
            #'t.id', 't.type', 't.is_open', 't.is_usable', 't.is_busy',
            #'t.is_extra', 't.min_interval', 't.normal_interval',
            #'t.scrape_time_last', 't.scrape_state', 't.announce_state',
            #'t.announce_time_last', 't.url',
            
            # Add torrent methods
            #'load.start', 'load.normal', 'load.raw', 'load.raw_start',
            #'load.raw_verbose', 'load.raw_upload',
        ]
        logger.debug(f"Returning {len(methods)} available methods")
        return methods

    def _methodSignature(self, method_name: str) -> List[str]:
        """Return method signatures - simplified for Flood"""
        # Flood expects array of possible signatures
        logger.debug(f"Method signature requested for: {method_name}")
        return ['string']  # Most methods return strings

    def _methodHelp(self, method_name: str) -> str:
        """Return method help - simplified for Flood"""
        logger.debug(f"Method help requested for: {method_name}")
        return f"Help for {method_name}"

    def system_multicall(self, calls: List[List]) -> List[Any]:
        """Handle system.multicall - batch multiple RPC calls"""
        logger.info(f"System multicall with {len(calls)} calls")
        results = []
        for call in calls:
            method_name = call["methodName"]
            if method_name == "false=":
                continue
            params = call["params"]
            try:
                result = self._dispatch(method_name, params)
                results.append([result])
            except Exception as e:
                logger.error(f"Multicall error for {method_name}: {e}")
                results.append({'faultCode': 1, 'faultString': str(e)})
        return results

    # Main torrent listing method - CRITICAL for Flood
    def d_multicall2(self, view: str, *args: str) -> List[List[Any]]:
        """
        Main method Flood uses to get torrent lists
        This is the most important method for Flood compatibility
        """
        logger.info(f"d.multicall2 called with view: {view}, args: {args}")
        
        # Get torrents from backend
        response = self.call_backend('/api/torrents', 'GET')
        if not response:
            logger.warning("No response from backend for torrent list")
            return []
            
        torrents_data = response.get('torrents', [])
        logger.info(f"Retrieved {len(torrents_data)} torrents from backend")
        
        result = []
        for torrent in torrents_data:
            torrent_result = []
            info_hash = torrent.get('info_hash', '')
            
            for method_call in args[1:]:
                # Remove trailing = if present (Flood sometimes adds it)
                method_clean = method_call.rstrip('=')
                if method_clean != "false":
                    logger.debug(f"Processing method: {method_clean} for torrent {info_hash}")
                
                try:
                    if method_clean.startswith('cat='):
                        # Handle complex cat expressions (usually for trackers)
                        value = self._handle_cat_expression(method_clean, info_hash)
                    elif method_clean == 'false':
                        value = False
                    elif method_clean == 'd.is_active':
                        value = self._get_torrent_active_state(torrent)
                    else:
                        # Map standard method calls
                        value = self._map_method_to_field(method_clean, torrent)
                    
                    torrent_result.append(value)
                except Exception as e:
                    logger.error(f"Error processing {method_clean} for {info_hash}: {e}")
                    torrent_result.append(False)  # Fallback empty value
            
            result.append(torrent_result)
        
        logger.info(f"d.multicall2 returning {len(result)} torrent results")
        return result

    def _handle_cat_expression(self, cat_expr: str, info_hash: str) -> str:
        """Handle cat expressions - usually for tracker data"""
        logger.debug(f"Handling cat expression: {cat_expr} for {info_hash}")
        
        if 't.multicall' in cat_expr:
            # Handle tracker multicall
            trackers = self._get_torrent_trackers(info_hash)
            tracker_strings = []
            for tracker in trackers:
                # Format tracker data as Flood expects
                tracker_str = f"{tracker.get('url', '')}|||{tracker.get('status', '')}"
                tracker_strings.append(tracker_str)
            return '||'.join(tracker_strings)
        
        return ''  # Default empty for unhandled cat expressions

    def _get_torrent_active_state(self, torrent: Dict) -> bool:
        """Determine if torrent is active"""
        state = torrent.get('state', 'stopped')
        return state in ['downloading', 'seeding', 'checking']

    def _map_method_to_field(self, method: str, torrent: Dict) -> Any:
        """Enhanced mapping with all required Flood fields"""
        mapping = {
            # Basic info
            'd.hash': torrent.get('info_hash', ''),
            'd.name': torrent.get('name', 'Unknown'),
            'd.base_path': torrent.get('base_path', ''),
            'd.base_filename': torrent.get('base_filename', 'Unknown'),
            'd.directory': torrent.get('directory', ''),
            
            # Size and progress
            'd.size_bytes': torrent.get('size_bytes', 0),
            'd.completed_bytes': torrent.get('completed_bytes', 0),
            'd.left_bytes': torrent.get('left_bytes', 0),
            'd.size_chunks': torrent.get('size_chunks', 0),
            'd.completed_chunks': torrent.get('completed_chunks', 0),
            'd.chunk_size': torrent.get('chunk_size', 16384),
            
            # Transfer stats
            'd.down.rate': str(torrent.get('down_rate', 0)),
            'd.up.rate': str(torrent.get('up_rate', 0)),
            'd.down.total': torrent.get('down_total', 0),
            'd.up.total': torrent.get('up_total', 0),
            'd.ratio': str(torrent.get('ratio', 0.0)),
            'd.bytes_done': torrent.get('completed_bytes', 0),
            
            # Peer info
            'd.peers_connected': torrent.get('peers_connected', 0),
            'd.peers_accounted': torrent.get('peers_accounted', 0),
            'd.peers_not_connected': torrent.get('peers_not_connected', 0),
            'd.peers_complete': torrent.get('peers_complete', 0),
            
            # State and status
            'd.state': torrent.get('state_code', 0),
            'd.message': torrent.get('message', ''),
            'd.is_active': 1 if torrent.get('is_active') else 0,
            'd.complete': 1 if torrent.get('is_complete') else 0,
            'd.is_private': 0,
            'd.is_hash_checking': 0,  # You can implement this
            'd.is_hash_checked': 1,
            'd.is_open': 1,
            'd.is_multi_file': 1,
            
            # Additional fields
            'd.creation_date': str(torrent.get('added_time', 0)),
            'd.priority': 1,
            'd.local_id': torrent.get('info_hash', '')[:8],
            'd.connection_current': f"↓{torrent.get('down_rate', 0):.0f}/↑{torrent.get('up_rate', 0):.0f}",
            'd.connection_leech': 'leech' if not torrent.get('is_complete') else 'seed',
            'd.connection_seed': 'seed' if torrent.get('is_complete') else 'leech',
            'd.tied_to_file': torrent.get('download_path', ''),
            'd.custom1': '',
            'd.custom2': '',
            'd.ignore_commands': 0,
            'd.hashing': 0,
            'd.bitfield': '',
            'd.tracker_focus': 0,
            'd.tracker_numwant': 50,
            'd.activity_time_last': str(torrent.get('last_activity', 0)),
            'd.activity_time_seen': str(torrent.get('added_time', 0)),
            'd.throttle_name': '',
        }
        value = mapping.get(method, '')
        logger.debug(f"Mapped {method} to value: {value}")
        return value

    def _map_state_to_rtorrent(self, state: str) -> int:
        """Map backend state to rTorrent state codes"""
        state_map = {
            'stopped': 0,
            'paused': 0, 
            'queued': 0,
            'checking': 8,
            'downloading': 1,
            'seeding': 2,
            'error': 4,
        }
        return state_map.get(state, 0)

    # Individual torrent methods
    def d_name(self, info_hash: str) -> str:
        """Get torrent name"""
        torrent = self.call_backend(f'/api/torrents/{info_hash}', 'GET')
        return torrent.get('name', '') if torrent else ''

    def d_hash(self, info_hash: str) -> str:
        """Get torrent hash"""
        return info_hash

    def d_size_bytes(self, info_hash: str) -> int:
        """Get torrent size in bytes"""
        torrent = self.call_backend(f'/api/torrents/{info_hash}', 'GET')
        return torrent.get('size_bytes', 0) if torrent else 0

    def d_completed_bytes(self, info_hash: str) -> int:
        """Get completed bytes"""
        torrent = self.call_backend(f'/api/torrents/{info_hash}', 'GET')
        return torrent.get('completed_bytes', 0) if torrent else 0

    def d_state(self, info_hash: str) -> int:
        """Get torrent state"""
        torrent = self.call_backend(f'/api/torrents/{info_hash}', 'GET')
        state = torrent.get('state', 'stopped') if torrent else 'stopped'
        return self._map_state_to_rtorrent(state)

    # Torrent control methods
    def d_pause(self, info_hash: str) -> int:
        """Pause torrent"""
        result = self.call_backend(f'/api/torrents/{info_hash}/pause', 'POST')
        return 1 if result else 0

    def d_resume(self, info_hash: str) -> int:
        """Resume torrent"""
        result = self.call_backend(f'/api/torrents/{info_hash}/resume', 'POST')
        return 1 if result else 0

    def d_stop(self, info_hash: str) -> int:
        """Stop torrent"""
        result = self.call_backend(f'/api/torrents/{info_hash}/stop', 'POST')
        return 1 if result else 0

    def d_erase(self, info_hash: str) -> int:
        """Remove torrent"""
        result = self.call_backend(f'/api/torrents/{info_hash}', 'DELETE', {'delete_files': False})
        return 1 if result else 0

    def d_close(self, info_hash: str) -> int:
        """Close torrent - treat as stop"""
        return self.d_stop(info_hash)

    def d_check_hash(self, info_hash: str) -> int:
        """Check torrent hash"""
        result = self.call_backend(f'/api/torrents/{info_hash}/check', 'POST')
        return 1 if result else 0

    # Additional required methods
    def d_base_path(self, info_hash: str) -> str:
        """Get base path"""
        torrent = self.call_backend(f'/api/torrents/{info_hash}', 'GET')
        return torrent.get('download_path', '') if torrent else ''

    def d_down_total(self, info_hash: str) -> int:
        """Get total downloaded"""
        torrent = self.call_backend(f'/api/torrents/{info_hash}', 'GET')
        return torrent.get('down_total', 0) if torrent else 0

    def d_up_total(self, info_hash: str) -> int:
        """Get total uploaded"""
        torrent = self.call_backend(f'/api/torrents/{info_hash}', 'GET')
        return torrent.get('up_total', 0) if torrent else 0

    def d_left_bytes(self, info_hash: str) -> int:
        """Get left bytes"""
        torrent = self.call_backend(f'/api/torrents/{info_hash}', 'GET')
        if torrent:
            return torrent.get('size_bytes', 0) - torrent.get('completed_bytes', 0)
        return 0

    def d_is_active(self, info_hash: str) -> int:
        """Check if torrent is active"""
        torrent = self.call_backend(f'/api/torrents/{info_hash}', 'GET')
        if torrent:
            state = torrent.get('state', 'stopped')
            return 1 if state in ['downloading', 'seeding', 'checking'] else 0
        return 0

    def d_is_complete(self, info_hash: str) -> int:
        """Check if torrent is complete"""
        torrent = self.call_backend(f'/api/torrents/{info_hash}', 'GET')
        if torrent:
            completed = torrent.get('completed_bytes', 0)
            total = torrent.get('size_bytes', 1)
            return 1 if completed >= total else 0
        return 0

    # File methods (simplified)
    def f_multicall(self, info_hash: str, *args: str) -> List[List[Any]]:
        """File multicall - simplified implementation"""
        files_data = self.call_backend(f'/api/torrents/{info_hash}/files', 'GET')
        files = files_data.get('files', []) if files_data else []
        
        result = []
        for file_index, file_info in enumerate(files):
            file_result = []
            for method in args:
                value = self._map_file_method(method, file_info, file_index)
                file_result.append(value)
            result.append(file_result)
        
        return result

    def _map_file_method(self, method: str, file_info: Dict, index: int) -> Any:
        """Map file methods to file data"""
        mapping = {
            'f.path': file_info.get('path', ''),
            'f.size_bytes': file_info.get('size_bytes', 0),
            'f.completed_chunks': file_info.get('completed_chunks', 0),
            'f.priority': file_info.get('priority', 1),
        }
        return mapping.get(method, '')

    # Tracker methods
    def t_multicall(self, info_hash: str, *args: str) -> List[List[Any]]:
        """Tracker multicall"""
        trackers_data = self.call_backend(f'/api/torrents/{info_hash}/trackers', 'GET')
        trackers = trackers_data.get('trackers', []) if trackers_data else []
        
        result = []
        for tracker in trackers:
            tracker_result = []
            for method in args:
                value = self._map_tracker_method(method, tracker)
                tracker_result.append(value)
            result.append(tracker_result)
        
        return result

    def _map_tracker_method(self, method: str, tracker: Dict) -> Any:
        """Map tracker methods to tracker data"""
        mapping = {
            't.url': tracker.get('url', ''),
            't.is_enabled': 1 if tracker.get('enabled', True) else 0,
            't.scrape_complete': tracker.get('seeders', 0),
            't.scrape_incomplete': tracker.get('leechers', 0),
            't.scrape_downloaded': tracker.get('downloads', 0),
        }
        return mapping.get(method, 0)

    def _get_torrent_trackers(self, info_hash: str) -> List[Dict]:
        """Get trackers for a torrent"""
        trackers_data = self.call_backend(f'/api/torrents/{info_hash}/trackers', 'GET')
        return trackers_data.get('trackers', []) if trackers_data else []

    # Add torrent method
    def load_start(self, data: str, *args) -> int:
        """Add torrent from URL, magnet, or raw data"""
        logger.info(f"load_start called with data length: {len(data)}, args: {args}")
        
        if data.startswith('magnet:'):
            result = self.call_backend('/api/torrents', 'POST', {'magnet': data})
        elif data.startswith('http://') or data.startswith('https://'):
            result = self.call_backend('/api/torrents', 'POST', {'url': data})
        else:
            # Assume raw torrent data - encode as base64
            import base64
            torrent_b64 = base64.b64encode(data.encode() if isinstance(data, str) else data).decode()
            result = self.call_backend('/api/torrents', 'POST', {'torrent': torrent_b64})
        
        success = result is not None
        logger.info(f"load_start result: {'success' if success else 'failed'}")
        return 1 if success else 0

    # Alias for load.start
    def load_normal(self, *args) -> int:
        """Alias for load_start"""
        return self.load_start(*args)
