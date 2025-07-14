


import requests
import pandas as pd
import os
from datetime import datetime
from pathlib import Path
import logging
from typing import Dict, List, Any, Optional, Union
import uuid
from urllib.parse import urlparse
# import warnings
from requests.adapters import HTTPAdapter
# from requests.packages.urllib3.util.retry import Retry
from urllib3.util.retry import Retry
import getpass
import tkinter as tk
from tkinter import simpledialog
import platform
import json
import xml.etree.ElementTree as ET
from enum import Enum

class FileType(Enum):
    """Supported file types for data collection and persistence."""
    JSON = "json"
    CSV = "csv"
    TXT = "txt"
    XML = "xml"
    TSV = "tsv"
    HTML = "html"
    BINARY = "bin"

# Disable SSL warnings since we're not using SSL verification
# warnings.filterwarnings('ignore', message='Unverified HTTPS request')
# requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class APIDataCollector:
    def __init__(self, config_file: str = 'api_config.json', base_output_dir: str = 'api_data',
                 password: Optional[str] = None):
        """
        Initialize the API Data Collector

        Args:
            config_file: Path to JSON configuration file
            base_output_dir: Base directory for saving CSV files
            password: Password for API authentication (will prompt if not provided)
        """
        self.config_file = config_file
        self.base_output_dir = Path(base_output_dir)
        self.config = self.load_config()

        # Get password if not provided
        self.password = password
        if self.password is None:
            self.password = get_password()
            if not self.password:
                raise ValueError("Password is required for API access")

        # Create base output directory if it doesn't exist
        self.base_output_dir.mkdir(exist_ok=True)

        # Set up session with retry strategy
        self.session = self._create_session()

        # Default security headers (without SSL-specific headers)
        self.default_security_headers = {
            'User-Agent': 'APIDataCollector/1.0 (Python Requests)',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Connection': 'keep-alive'
        }

    def _create_session(self) -> requests.Session:
        """
        Create a session with retry strategy (SSL verification disabled)

        Returns:
            Configured requests session
        """
        session = requests.Session()

        # Configure retry strategy
        retry = Retry(
            total=3,
            read=3,
            connect=3,
            backoff_factor=0.3,
            status_forcelist=(500, 502, 503, 504)
        )

        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        # Disable SSL verification for all requests in this session
        session.verify = False

        return session

    def load_config(self) -> Dict:
        """Load API configuration from JSON file"""
        try:
            with open(self.config_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"Configuration file {self.config_file} not found")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON configuration: {e}")
            raise

    def process_url_parameters(self, url: str, params: Dict) -> str:
        """
        Replace URL placeholders with actual parameter values

        Args:
            url: URL template with placeholders like {date}
            params: Dictionary of parameter values

        Returns:
            Processed URL with placeholders replaced
        """
        for key, value in params.items():
            placeholder = f"{{{key}}}"
            if placeholder in url:
                url = url.replace(placeholder, str(value))
        return url

    def make_api_call(self, api_config: Dict) -> Dict:
        """
        Make API call with HTTP Basic Authentication

        Args:
            api_config: API configuration dictionary

        Returns:
            Dictionary with response data and headers
        """
        url = api_config['url']
        method = api_config.get('method', 'GET')
        headers = api_config.get('headers', {})
        params = api_config.get('params', {})
        timeout = api_config.get('timeout', 30)

        # Process URL parameters (for URLs with placeholders)
        url = self.process_url_parameters(url, params)

        # Validate URL
        if not self._validate_url(url):
            raise ValueError(f"Invalid URL: {url}")

        # Merge headers with API-specific headers
        final_headers = self.default_security_headers.copy()
        final_headers.update(headers)

        # Set up authentication
        auth = None
        auth_type = api_config.get('auth_type', 'basic')

        if auth_type == 'basic':
            # Get username from config, use default if not specified
            username = api_config.get('username', 'steveg93@gmail.com')
            # Use HTTP Basic Authentication with username and password
            auth = (username, self.password)
            logger.info(f"Using Basic Auth with username: {username}")

        elif api_config.get('use_password_as_api_key', False):
            # Use password as API key
            api_key_header = api_config.get('api_key_header', 'X-API-Key')
            final_headers[api_key_header] = self.password
            logger.info(f"Using password as API key in header: {api_key_header}")

        elif api_config.get('use_password_as_bearer', False):
            # Use password as Bearer token
            final_headers['Authorization'] = f'Bearer {self.password}'
            logger.info("Using password as Bearer token")

        logger.info(f"Making {method} request to: {url}")
        logger.debug(f"Headers: {self._sanitize_headers(final_headers)}")

        try:
            if method.upper() == 'GET':
                response = self.session.get(
                    url,
                    headers=final_headers,
                    params=params,
                    timeout=timeout,
                    verify=False,  # SSL verification disabled
                    auth=auth
                )
            elif method.upper() == 'POST':
                response = self.session.post(
                    url,
                    headers=final_headers,
                    json=params,
                    timeout=timeout,
                    verify=False,  # SSL verification disabled
                    auth=auth
                )
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            response.raise_for_status()

            # Try to parse JSON response first
            try:
                data = response.json()
            except json.JSONDecodeError:
                # If response is not JSON, return as text
                data = response.text

            return {
                'data': data,
                'headers': dict(response.headers),
                'status_code': response.status_code
            }

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                logger.error("Authentication failed. Please check your password.")
            logger.error(f"HTTP error: {e}")
            raise
        except requests.exceptions.Timeout:
            logger.error(f"Request timed out after {timeout} seconds")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise

    def _validate_url(self, url: str) -> bool:
        """
        Validate URL format

        Args:
            url: URL to validate

        Returns:
            True if URL is valid, False otherwise
        """
        try:
            result = urlparse(url)
            # Check for valid scheme and netloc
            if result.scheme not in ['http', 'https']:
                logger.warning(f"Invalid URL scheme: {result.scheme}")
                return False
            if not result.netloc:
                logger.warning("URL missing network location")
                return False
            return True
        except Exception as e:
            logger.error(f"URL validation failed: {e}")
            return False

    def _sanitize_headers(self, headers: Dict) -> Dict:
        """
        Sanitize headers for logging (remove sensitive data)

        Args:
            headers: Headers dictionary

        Returns:
            Sanitized headers
        """
        sensitive_headers = [
            'authorization', 'x-api-key', 'api-key', 'token',
            'cookie', 'set-cookie', 'x-auth-token', 'password'
        ]

        sanitized = headers.copy()
        for header in headers:
            if header.lower() in sensitive_headers:
                sanitized[header] = '[REDACTED]'

        return sanitized

    def flatten_json_data(self, data: Any, parent_key: str = '', sep: str = '_') -> Dict:
        """
        Flatten nested JSON data into a single level dictionary

        Args:
            data: JSON data to flatten
            parent_key: Parent key for nested items
            sep: Separator for concatenating keys

        Returns:
            Flattened dictionary
        """
        items = []

        if isinstance(data, dict):
            for k, v in data.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                if isinstance(v, dict):
                    items.extend(self.flatten_json_data(v, new_key, sep=sep).items())
                elif isinstance(v, list):
                    for i, item in enumerate(v):
                        items.extend(self.flatten_json_data(item, f"{new_key}{sep}{i}", sep=sep).items())
                else:
                    items.append((new_key, v))
        elif isinstance(data, list):
            for i, item in enumerate(data):
                items.extend(self.flatten_json_data(item, f"{parent_key}{sep}{i}", sep=sep).items())
        else:
            items.append((parent_key, data))

        return dict(items)

    def detect_file_type(self, data: Any, response_headers: Dict = None) -> FileType:
        """
        Detect file type based on data content and response headers
        
        Args:
            data: API response data
            response_headers: HTTP response headers
            
        Returns:
            Detected FileType
        """
        # Check content-type header first
        if response_headers:
            content_type = response_headers.get('content-type', '').lower()
            if 'application/json' in content_type:
                return FileType.JSON
            elif 'text/csv' in content_type:
                return FileType.CSV
            elif 'text/xml' in content_type or 'application/xml' in content_type:
                return FileType.XML
            elif 'text/html' in content_type:
                return FileType.HTML
            elif 'text/tab-separated-values' in content_type:
                return FileType.TSV
        
        # Fallback to data structure analysis
        if isinstance(data, (dict, list)):
            return FileType.JSON
        elif isinstance(data, str):
            # Try to detect XML
            if data.strip().startswith('<') and data.strip().endswith('>'):
                return FileType.XML
            # Try to detect CSV (simple heuristic)
            elif ',' in data and '\n' in data:
                return FileType.CSV
            else:
                return FileType.TXT
        else:
            return FileType.BINARY
    
    def save_to_file(self, data: Any, api_config: Dict, file_type: FileType = None, response_headers: Dict = None) -> Dict:
        """
        Save API response data to appropriate file format

        Args:
            data: API response data
            api_config: API configuration dictionary
            file_type: Specific file type to save as (auto-detected if None)
            response_headers: HTTP response headers for type detection

        Returns:
            Dictionary with file path, raw data, and metadata
        """
        # Auto-detect file type if not specified
        if file_type is None:
            file_type = self.detect_file_type(data, response_headers)
        
        # Create output folder
        output_folder = self.base_output_dir / api_config['output_folder']
        output_folder.mkdir(exist_ok=True)

        # Generate unique filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        unique_id = str(uuid.uuid4())[:8]
        file_prefix = api_config.get('file_prefix', api_config.get('csv_prefix', 'data'))
        filename = f"{file_prefix}_{timestamp}_{unique_id}.{file_type.value}"
        filepath = output_folder / filename

        # Save data based on file type
        if file_type == FileType.JSON:
            self._save_json_file(data, filepath)
        elif file_type == FileType.CSV:
            self._save_csv_file(data, filepath, api_config, unique_id)
        elif file_type == FileType.XML:
            self._save_xml_file(data, filepath)
        elif file_type == FileType.TSV:
            self._save_tsv_file(data, filepath, api_config, unique_id)
        elif file_type == FileType.HTML:
            self._save_html_file(data, filepath)
        elif file_type == FileType.TXT:
            self._save_txt_file(data, filepath)
        elif file_type == FileType.BINARY:
            self._save_binary_file(data, filepath)
        else:
            # Default to text file
            self._save_txt_file(data, filepath)

        logger.info(f"Data saved as {file_type.value} to: {filepath}")
        
        return {
            'file_path': str(filepath),
            'file_type': file_type.value,
            'raw_data': data,
            'metadata': {
                'api_name': api_config['name'],
                'timestamp': datetime.now().isoformat(),
                'unique_id': unique_id,
                'file_size': os.path.getsize(filepath) if filepath.exists() else 0
            }
        }
    
    def _save_json_file(self, data: Any, filepath: Path) -> None:
        """Save data as JSON file"""
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, default=str, ensure_ascii=False)
    
    def _save_csv_file(self, data: Any, filepath: Path, api_config: Dict, unique_id: str) -> None:
        """Save data as CSV file"""
        # Convert data to DataFrame (existing logic)
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            if any(isinstance(v, (list, dict)) for v in data.values()):
                flattened_data = self.flatten_json_data(data)
                df = pd.DataFrame([flattened_data])
            else:
                df = pd.DataFrame([data])
        else:
            df = pd.DataFrame([{'value': data}])

        # Add metadata columns
        df['api_name'] = api_config['name']
        df['timestamp'] = datetime.now()
        df['unique_id'] = unique_id

        df.to_csv(filepath, index=False)
    
    def _save_xml_file(self, data: Any, filepath: Path) -> None:
        """Save data as XML file"""
        if isinstance(data, str):
            # Data is already XML string
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(data)
        else:
            # Convert data to XML format
            root = ET.Element('root')
            self._dict_to_xml(data, root)
            tree = ET.ElementTree(root)
            tree.write(filepath, encoding='utf-8', xml_declaration=True)
    
    def _save_tsv_file(self, data: Any, filepath: Path, api_config: Dict, unique_id: str) -> None:
        """Save data as TSV file"""
        # Similar to CSV but with tab separator
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            if any(isinstance(v, (list, dict)) for v in data.values()):
                flattened_data = self.flatten_json_data(data)
                df = pd.DataFrame([flattened_data])
            else:
                df = pd.DataFrame([data])
        else:
            df = pd.DataFrame([{'value': data}])

        df['api_name'] = api_config['name']
        df['timestamp'] = datetime.now()
        df['unique_id'] = unique_id

        df.to_csv(filepath, index=False, sep='\t')
    
    def _save_html_file(self, data: Any, filepath: Path) -> None:
        """Save data as HTML file"""
        if isinstance(data, str):
            content = data
        else:
            # Convert to HTML representation
            content = f"<html><body><pre>{json.dumps(data, indent=2, default=str)}</pre></body></html>"
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
    
    def _save_txt_file(self, data: Any, filepath: Path) -> None:
        """Save data as text file"""
        if isinstance(data, str):
            content = data
        else:
            content = json.dumps(data, indent=2, default=str)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
    
    def _save_binary_file(self, data: Any, filepath: Path) -> None:
        """Save data as binary file"""
        if isinstance(data, bytes):
            with open(filepath, 'wb') as f:
                f.write(data)
        else:
            # Convert to string and encode
            content = str(data).encode('utf-8')
            with open(filepath, 'wb') as f:
                f.write(content)
    
    def _dict_to_xml(self, data: Any, parent: ET.Element) -> None:
        """Convert dictionary to XML elements"""
        if isinstance(data, dict):
            for key, value in data.items():
                child = ET.SubElement(parent, str(key))
                self._dict_to_xml(value, child)
        elif isinstance(data, list):
            for i, item in enumerate(data):
                child = ET.SubElement(parent, f'item_{i}')
                self._dict_to_xml(item, child)
        else:
            parent.text = str(data)
    
    def save_to_csv(self, data: Any, api_config: Dict) -> str:
        """
        Legacy method for backward compatibility - Save API response data to CSV file
        
        Args:
            data: API response data
            api_config: API configuration dictionary

        Returns:
            Path to saved CSV file
        """
        result = self.save_to_file(data, api_config, FileType.CSV)
        return result['file_path']

    def run_all_apis(self) -> List[Dict]:
        """
        Run all API calls defined in configuration

        Returns:
            List of results with API name, status, file info, and raw data
        """
        results = []

        for api_config in self.config.get('apis', []):
            api_name = api_config.get('name', 'unnamed_api')
            logger.info(f"Processing API: {api_name}")

            try:
                # Make API call
                api_response = self.make_api_call(api_config)
                
                # Save to appropriate file format
                file_result = self.save_to_file(
                    data=api_response['data'],
                    api_config=api_config,
                    response_headers=api_response['headers']
                )

                results.append({
                    'api_name': api_name,
                    'status': 'success',
                    'file_path': file_result['file_path'],
                    'file_type': file_result['file_type'],
                    'raw_data': file_result['raw_data'],
                    'metadata': file_result['metadata'],
                    'response_headers': api_response['headers']
                })

            except Exception as e:
                logger.error(f"Failed to process API {api_name}: {e}")
                results.append({
                    'api_name': api_name,
                    'status': 'failed',
                    'error': str(e)
                })

        return results

    def run_single_api(self, api_name: str) -> Dict:
        """
        Run a single API call by name

        Args:
            api_name: Name of the API to run

        Returns:
            Result dictionary with status, file info, and raw data
        """
        # Find API configuration by name
        api_config = None
        for config in self.config.get('apis', []):
            if config.get('name') == api_name:
                api_config = config
                break

        if not api_config:
            raise ValueError(f"API configuration not found for: {api_name}")

        logger.info(f"Processing API: {api_name}")

        try:
            # Make API call
            api_response = self.make_api_call(api_config)
            
            # Save to appropriate file format
            file_result = self.save_to_file(
                data=api_response['data'],
                api_config=api_config,
                response_headers=api_response['headers']
            )

            return {
                'api_name': api_name,
                'status': 'success',
                'file_path': file_result['file_path'],
                'file_type': file_result['file_type'],
                'raw_data': file_result['raw_data'],
                'metadata': file_result['metadata'],
                'response_headers': api_response['headers']
            }

        except Exception as err:
            logger.error(f"Failed to process API {api_name}: {err}")
            return {
                'api_name': api_name,
                'status': 'failed',
                'error': str(err)
            }
# Load Zones (same as AWS version)
LOAD_ZONES = [
    '.Z.MAINE',
    '.Z.NEWHAMPSHIRE',
    '.Z.VERMONT',
    '.Z.CONNECTICUT',
    '.Z.RHODEISLAND',
    '.Z.SEMASS',
    '.Z.WCMASS',
    '.Z.NEMASSBOST'
]



def get_password_windows():
    """
    Get password using Windows GUI dialog

    Returns:
        Password string or None if cancelled
    """
    try:
        # Create root window and hide it
        root = tk.Tk()
        root.withdraw()

        # Show password dialog
        password = simpledialog.askstring(
            "API Authentication",
            "Enter password for API access:",
            show='*'
        )

        # Destroy the root window
        root.destroy()

        return password
    except Exception as e:
        logger.error(f"Failed to show password dialog: {e}")
        return None


def get_password_console():
    """
    Get password using console input (fallback)

    Returns:
        Password string
    """
    return getpass.getpass("Enter password for API access: ")


def get_password():
    """
    Get password using appropriate method for the platform

    Returns:
        Password string
    """
    if platform.system() == 'Windows':
        password = get_password_windows()
        if password is None:
            logger.info("Password dialog cancelled, falling back to console input")
            password = get_password_console()
    else:
        password = get_password_console()

    return password


def main():
    """Main function to demonstrate usage"""
    print("API Data Collector - ISO New England")
    print("=" * 40)
    print("HTTP Basic Authentication")
    print("Default username: steveg93@gmail.com")
    print("=" * 40)

    try:
        # Initialize the API collector (will prompt for password)
        collector = APIDataCollector()

        print("\nAuthentication successful. Starting API calls...")

        # Example 1: Run all APIs
        print("\nRunning all APIs...")
        results = collector.run_all_apis()

        print("\nResults:")
        for result in results:
            print(f"- {result['api_name']}: {result['status']}")
            if result['status'] == 'success':
                print(f"  CSV saved to: {result['csv_path']}")
            else:
                print(f"  Error: {result.get('error', 'Unknown error')}")

        # Example 2: Run a single API
        print("\n\nRunning single API (iso_ne_current_demand)...")
        single_result = collector.run_single_api('iso_ne_current_demand')
        print(f"Result: {single_result}")

    except ValueError as val_e:
        print(f"\nError: {val_e}")
        print("Exiting...")
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        logger.exception("Unexpected error in main")


if __name__ == "__main__":
    main()