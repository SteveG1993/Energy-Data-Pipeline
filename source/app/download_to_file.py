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


import requests
import pandas as pd
import os
from datetime import datetime
from pathlib import Path
import logging
from typing import Dict, List, Any, Optional
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
            API response data as dictionary
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

            # Try to parse JSON response
            try:
                return response.json()
            except json.JSONDecodeError:
                # If response is not JSON, return as text
                return {'raw_response': response.text}

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

    def save_to_csv(self, data: Any, api_config: Dict) -> str:
        """
        Save API response data to CSV file

        Args:
            data: API response data
            api_config: API configuration dictionary

        Returns:
            Path to saved CSV file
        """
        # Create output folder
        output_folder = self.base_output_dir / api_config['output_folder']
        output_folder.mkdir(exist_ok=True)

        # Generate unique filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        unique_id = str(uuid.uuid4())[:8]
        csv_prefix = api_config.get('csv_prefix', 'data')
        filename = f"{csv_prefix}_{timestamp}_{unique_id}.csv"
        filepath = output_folder / filename

        # Convert data to DataFrame
        if isinstance(data, list):
            # If data is a list of dictionaries
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            # If data is a single dictionary or nested structure
            if any(isinstance(v, (list, dict)) for v in data.values()):
                # Flatten nested data
                flattened_data = self.flatten_json_data(data)
                df = pd.DataFrame([flattened_data])
            else:
                df = pd.DataFrame([data])
        else:
            # For other data types, create a simple DataFrame
            df = pd.DataFrame([{'value': data}])

        # Add metadata columns
        df['api_name'] = api_config['name']
        df['timestamp'] = datetime.now()
        df['unique_id'] = unique_id

        # Save to CSV
        df.to_csv(filepath, index=False)
        logger.info(f"Data saved to: {filepath}")

        return str(filepath)

    def run_all_apis(self) -> List[Dict]:
        """
        Run all API calls defined in configuration

        Returns:
            List of results with API name and saved file path
        """
        results = []

        for api_config in self.config.get('apis', []):
            api_name = api_config.get('name', 'unnamed_api')
            logger.info(f"Processing API: {api_name}")

            try:
                # Make API call
                data = self.make_api_call(api_config)

                # Save to CSV
                csv_path = self.save_to_csv(data, api_config)

                results.append({
                    'api_name': api_name,
                    'status': 'success',
                    'csv_path': csv_path
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
            Result dictionary with status and file path
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
            data = self.make_api_call(api_config)

            # Save to CSV
            csv_path = self.save_to_csv(data, api_config)

            return {
                'api_name': api_name,
                'status': 'success',
                'csv_path': csv_path
            }

        except Exception as e:
            logger.error(f"Failed to process API {api_name}: {e}")
            return {
                'api_name': api_name,
                'status': 'failed',
                'error': str(e)
            }


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

    except ValueError as e:
        print(f"\nError: {e}")
        print("Exiting...")
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        logger.exception("Unexpected error in main")


if __name__ == "__main__":
    main()