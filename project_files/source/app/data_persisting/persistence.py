import boto3
import pandas as pd
import json
import os
from datetime import datetime
from typing import Optional, Dict, Any, Union, List
import logging
from io import StringIO, BytesIO
from botocore.exceptions import ClientError, NoCredentialsError
from enum import Enum
import mimetypes

try:
    from dotenv import load_dotenv
    load_dotenv()  # Load environment variables from .env file
except ImportError:
    # dotenv not installed, will use system environment variables
    pass

class DataFormat(Enum):
    """Supported data formats for persistence."""
    CSV = "csv"
    JSON = "json"
    TXT = "txt"
    PARQUET = "parquet"
    XML = "xml"
    TSV = "tsv"
    HTML = "html"
    YAML = "yaml"
    BINARY = "bin"

class S3DataPersistence:
    """
    Handles writing multiple data formats to S3 bucket for energy data pipeline.
    Supports CSV, JSON, TXT, and Parquet formats.
    """
    
    def __init__(self, 
                 bucket_name: str = "s3-for-energy",
                 aws_profile: Optional[str] = None,
                 region: str = "us-east-1"):
        """
        Initialize S3 persistence handler.
        
        Args:
            bucket_name: Name of S3 bucket (defaults to S3_BUCKET_NAME env var if available)
            aws_profile: AWS profile name (if using named profiles)
            region: AWS region (defaults to AWS_DEFAULT_REGION env var if available)
        """
        # Use environment variables as defaults if available
        self.bucket_name = bucket_name or os.getenv('S3_BUCKET_NAME', 's3-for-energy')
        region = region or os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        
        self.logger = logging.getLogger(__name__)
        
        # Content type mapping
        self.content_types = {
            DataFormat.CSV: 'text/csv',
            DataFormat.JSON: 'application/json',
            DataFormat.TXT: 'text/plain',
            DataFormat.PARQUET: 'application/octet-stream',
            DataFormat.XML: 'application/xml',
            DataFormat.TSV: 'text/tab-separated-values',
            DataFormat.HTML: 'text/html',
            DataFormat.YAML: 'text/yaml',
            DataFormat.BINARY: 'application/octet-stream'
        }
        
        try:
            if aws_profile:
                session = boto3.Session(profile_name=aws_profile)
                self.s3_client = session.client('s3', region_name=region)
            else:
                # Create S3 client - will use AWS credential chain including:
                # 1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
                # 2. AWS credentials file (~/.aws/credentials)
                # 3. IAM roles (EC2, ECS, Lambda)
                # 4. AWS SSO
                self.s3_client = boto3.client('s3', region_name=region)
                
            # Verify bucket access
            self._verify_bucket_access()
            
        except NoCredentialsError:
            self.logger.error("AWS credentials not found. Please set up credentials using one of:")
            self.logger.error("1. Environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY")
            self.logger.error("2. AWS credentials file: aws configure")
            self.logger.error("3. IAM roles (if running on AWS)")
            raise
        except Exception as e:
            self.logger.error(f"Failed to initialize S3 client: {e}")
            raise
    
    def _verify_bucket_access(self) -> None:
        """Verify we can access the S3 bucket."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            self.logger.info(f"Successfully connected to bucket: {self.bucket_name}")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                self.logger.error(f"Bucket {self.bucket_name} does not exist")
            elif error_code == '403':
                self.logger.error(f"Access denied to bucket {self.bucket_name}")
            else:
                self.logger.error(f"Error accessing bucket: {e}")
            raise
    
    def _ensure_folder_exists(self, folder_path: str) -> None:
        """
        Ensure S3 folder exists by creating a placeholder object if needed.
        
        Args:
            folder_path: Path to the folder in S3 bucket
        """
        if not folder_path:
            return
            
        # Normalize folder path
        folder_path = folder_path.strip('/') + '/'
        
        try:
            # Check if folder already exists by listing objects with the prefix
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=folder_path,
                MaxKeys=1
            )
            
            # If no objects exist with this prefix, create a placeholder
            if 'Contents' not in response:
                placeholder_key = f"{folder_path}.folder_placeholder"
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=placeholder_key,
                    Body=b'',
                    Metadata={'placeholder': 'true', 'created_by': 'S3DataPersistence'}
                )
                self.logger.info(f"Created S3 folder: {folder_path}")
            else:
                self.logger.debug(f"S3 folder already exists: {folder_path}")
                
        except Exception as e:
            self.logger.warning(f"Could not verify/create S3 folder {folder_path}: {e}")
            # Continue anyway - S3 will create the folder structure when we upload the file
    
    def _auto_detect_format(self, data: Any, file_path: Optional[str] = None) -> DataFormat:
        """
        Auto-detect data format based on data type and optional file path.
        
        Args:
            data: Input data of various types
            file_path: Optional file path for extension-based detection
            
        Returns:
            Detected DataFormat
        """
        # First try file extension if available
        if file_path:
            detected = self._detect_format_from_extension(file_path)
            if detected:
                return detected
        
        # Fallback to data type analysis
        if isinstance(data, pd.DataFrame):
            return DataFormat.CSV  # Default for DataFrames
        elif isinstance(data, (dict, list)):
            return DataFormat.JSON
        elif isinstance(data, str):
            # Try to detect specific text formats
            data_lower = data.strip().lower()
            if data_lower.startswith('<?xml') or (data_lower.startswith('<') and data_lower.endswith('>')):
                return DataFormat.XML
            elif data_lower.startswith('<!doctype html') or '<html' in data_lower:
                return DataFormat.HTML
            elif '\t' in data and '\n' in data:  # Simple TSV detection
                return DataFormat.TSV
            elif ',' in data and '\n' in data:  # Simple CSV detection
                return DataFormat.CSV
            else:
                return DataFormat.TXT
        elif isinstance(data, bytes):
            return DataFormat.BINARY
        else:
            return DataFormat.TXT  # Default fallback
    
    def _detect_format_from_extension(self, file_path: str) -> Optional[DataFormat]:
        """
        Detect format from file extension.
        
        Args:
            file_path: Path to file
            
        Returns:
            DataFormat if detected, None otherwise
        """
        if not file_path:
            return None
            
        # Extract extension
        extension = file_path.lower().split('.')[-1] if '.' in file_path else ''
        
        # Map extensions to formats
        extension_mapping = {
            'csv': DataFormat.CSV,
            'json': DataFormat.JSON,
            'txt': DataFormat.TXT,
            'xml': DataFormat.XML,
            'tsv': DataFormat.TSV,
            'html': DataFormat.HTML,
            'htm': DataFormat.HTML,
            'yaml': DataFormat.YAML,
            'yml': DataFormat.YAML,
            'parquet': DataFormat.PARQUET,
            'bin': DataFormat.BINARY
        }
        
        return extension_mapping.get(extension)
    
    def _detect_format_from_file(self, file_path: str) -> DataFormat:
        """
        Detect format by reading a file and analyzing its content.
        
        Args:
            file_path: Path to the file to analyze
            
        Returns:
            Detected DataFormat
        """
        try:
            # First try extension-based detection
            detected = self._detect_format_from_extension(file_path)
            if detected:
                return detected
            
            # Read file content for analysis
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                sample = f.read(1024)  # Read first 1KB
            
            return self._auto_detect_format(sample)
            
        except UnicodeDecodeError:
            # File is likely binary
            return DataFormat.BINARY
        except Exception:
            # Default fallback
            return DataFormat.TXT
    
    def _prepare_csv_data(self, data: pd.DataFrame) -> bytes:
        """Convert DataFrame to CSV bytes."""
        csv_buffer = StringIO()
        data.to_csv(csv_buffer, index=False)
        return csv_buffer.getvalue().encode('utf-8')
    
    def _prepare_json_data(self, data: Union[Dict, List, pd.DataFrame]) -> bytes:
        """Convert data to JSON bytes."""
        if isinstance(data, pd.DataFrame):
            # Convert DataFrame to JSON records format
            json_data = data.to_dict('records')
        else:
            json_data = data
        
        return json.dumps(json_data, indent=2, default=str).encode('utf-8')
    
    def _prepare_txt_data(self, data: Union[str, pd.DataFrame]) -> bytes:
        """Convert data to text bytes."""
        if isinstance(data, pd.DataFrame):
            # Convert DataFrame to tab-separated text
            txt_buffer = StringIO()
            data.to_csv(txt_buffer, sep='\t', index=False)
            return txt_buffer.getvalue().encode('utf-8')
        else:
            return str(data).encode('utf-8')
    
    def _prepare_parquet_data(self, data: pd.DataFrame) -> bytes:
        """Convert DataFrame to Parquet bytes."""
        if not isinstance(data, pd.DataFrame):
            raise ValueError("Parquet format requires pandas DataFrame")
        
        parquet_buffer = BytesIO()
        data.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        return parquet_buffer.getvalue()
    
    def _prepare_xml_data(self, data: Union[str, Dict, List, pd.DataFrame]) -> bytes:
        """Convert data to XML bytes."""
        if isinstance(data, str):
            # Assume data is already XML
            return data.encode('utf-8')
        elif isinstance(data, pd.DataFrame):
            # Convert DataFrame to XML
            xml_string = data.to_xml(index=False)
            return xml_string.encode('utf-8')
        else:
            # Convert dict/list to simple XML structure
            import xml.etree.ElementTree as ET
            root = ET.Element('root')
            self._dict_to_xml_element(data, root)
            return ET.tostring(root, encoding='utf-8')
    
    def _prepare_tsv_data(self, data: Union[pd.DataFrame, str]) -> bytes:
        """Convert data to TSV bytes."""
        if isinstance(data, pd.DataFrame):
            tsv_buffer = StringIO()
            data.to_csv(tsv_buffer, sep='\t', index=False)
            return tsv_buffer.getvalue().encode('utf-8')
        else:
            # Assume data is already TSV format
            return str(data).encode('utf-8')
    
    def _prepare_html_data(self, data: Union[str, pd.DataFrame, Dict, List]) -> bytes:
        """Convert data to HTML bytes."""
        if isinstance(data, str):
            # Assume data is already HTML
            return data.encode('utf-8')
        elif isinstance(data, pd.DataFrame):
            # Convert DataFrame to HTML table
            html_string = data.to_html(index=False)
            return html_string.encode('utf-8')
        else:
            # Convert to JSON and wrap in HTML
            json_str = json.dumps(data, indent=2, default=str)
            html_content = f"<html><body><pre>{json_str}</pre></body></html>"
            return html_content.encode('utf-8')
    
    def _prepare_yaml_data(self, data: Union[Dict, List, pd.DataFrame, str]) -> bytes:
        """Convert data to YAML bytes."""
        try:
            import yaml
            if isinstance(data, pd.DataFrame):
                # Convert DataFrame to dict then to YAML
                data_dict = data.to_dict('records')
                return yaml.dump(data_dict, default_flow_style=False).encode('utf-8')
            elif isinstance(data, str):
                # Assume already YAML format
                return data.encode('utf-8')
            else:
                return yaml.dump(data, default_flow_style=False).encode('utf-8')
        except ImportError:
            # Fallback to JSON if PyYAML not available
            return self._prepare_json_data(data)
    
    def _prepare_binary_data(self, data: Union[bytes, str]) -> bytes:
        """Prepare binary data."""
        if isinstance(data, bytes):
            return data
        else:
            return str(data).encode('utf-8')
    
    def _dict_to_xml_element(self, data: Any, parent) -> None:
        """Convert dictionary to XML elements."""
        import xml.etree.ElementTree as ET
        if isinstance(data, dict):
            for key, value in data.items():
                # Sanitize key for XML
                safe_key = str(key).replace(' ', '_').replace('-', '_')
                child = ET.SubElement(parent, safe_key)
                self._dict_to_xml_element(value, child)
        elif isinstance(data, list):
            for i, item in enumerate(data):
                child = ET.SubElement(parent, f'item_{i}')
                self._dict_to_xml_element(item, child)
        else:
            parent.text = str(data)
    
    def _prepare_data(self, data: Any, format_type: DataFormat) -> bytes:
        """
        Prepare data based on specified format.
        
        Args:
            data: Input data
            format_type: Target format
            
        Returns:
            Prepared data as bytes
        """
        preparation_methods = {
            DataFormat.CSV: self._prepare_csv_data,
            DataFormat.JSON: self._prepare_json_data,
            DataFormat.TXT: self._prepare_txt_data,
            DataFormat.PARQUET: self._prepare_parquet_data,
            DataFormat.XML: self._prepare_xml_data,
            DataFormat.TSV: self._prepare_tsv_data,
            DataFormat.HTML: self._prepare_html_data,
            DataFormat.YAML: self._prepare_yaml_data,
            DataFormat.BINARY: self._prepare_binary_data
        }
        
        method = preparation_methods.get(format_type)
        if not method:
            # Fallback to text format
            return self._prepare_txt_data(data)
        
        return method(data)
    
    def save_data(self, 
                  data: Union[pd.DataFrame, Dict, List, str],
                  file_prefix: str = "energy_data",
                  data_format: Optional[DataFormat] = None,
                  metadata: Optional[Dict[str, Any]] = None,
                  include_timestamp: bool = True,
                  folder_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Save data to S3 in specified format.
        
        Args:
            data: Data to save (DataFrame, dict, list, or string)
            file_prefix: Prefix for the S3 key/filename
            data_format: Target format (auto-detected if None)
            metadata: Optional metadata dict to add as S3 object metadata
            include_timestamp: Whether to include timestamp in filename
            folder_path: Optional folder path within S3 bucket (will be created if doesn't exist)
            
        Returns:
            Dict with upload results and S3 key
        """
        try:
            # Auto-detect format if not specified
            if data_format is None:
                data_format = self._auto_detect_format(data, file_prefix)
            
            # Generate S3 key (filename) with optional folder path
            if include_timestamp:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{file_prefix}_{timestamp}.{data_format.value}"
            else:
                filename = f"{file_prefix}.{data_format.value}"
            
            # Include folder path if specified
            if folder_path:
                # Ensure folder path doesn't start with '/' and ends with '/'
                folder_path = folder_path.strip('/')
                if folder_path:
                    # Ensure folder exists in S3
                    self._ensure_folder_exists(folder_path)
                    s3_key = f"{folder_path}/{filename}"
                else:
                    s3_key = filename
            else:
                s3_key = filename
            
            # Prepare data based on format
            prepared_data = self._prepare_data(data, data_format)
            
            # Calculate record count
            record_count = self._calculate_record_count(data)
            
            # Prepare metadata for S3 object
            s3_metadata = {
                'upload_timestamp': datetime.now().isoformat(),
                'record_count': str(record_count),
                'data_format': data_format.value
            }
            
            if isinstance(data, pd.DataFrame):
                s3_metadata['columns'] = str(len(data.columns))
                s3_metadata['column_names'] = ','.join(data.columns.tolist())
            
            if metadata:
                # Add custom metadata (S3 metadata keys must be strings)
                for key, value in metadata.items():
                    s3_metadata[key] = str(value)
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=prepared_data,
                ContentType=self.content_types[data_format],
                Metadata=s3_metadata
            )
            
            self.logger.info(f"Successfully uploaded {record_count} records as {data_format.value} to s3://{self.bucket_name}/{s3_key}")
            
            return {
                'success': True,
                's3_key': s3_key,
                'bucket': self.bucket_name,
                'data_format': data_format.value,
                'record_count': record_count,
                'file_size_bytes': len(prepared_data),
                'upload_timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Failed to upload {data_format.value if data_format else 'data'} to S3: {e}")
            return {
                'success': False,
                'error': str(e),
                'upload_timestamp': datetime.now().isoformat()
            }
    
    def _calculate_record_count(self, data: Any) -> int:
        """Calculate number of records in data."""
        if isinstance(data, pd.DataFrame):
            return len(data)
        elif isinstance(data, list):
            return len(data)
        elif isinstance(data, dict):
            return 1
        elif isinstance(data, str):
            return data.count('\n') + 1 if data else 0
        else:
            return 1
    
    def save_csv(self, data: pd.DataFrame, file_prefix: str = "data", **kwargs) -> Dict[str, Any]:
        """Convenience method for saving CSV data."""
        return self.save_data(data, file_prefix, DataFormat.CSV, **kwargs)
    
    def save_json(self, data: Union[Dict, List, pd.DataFrame], file_prefix: str = "data", **kwargs) -> Dict[str, Any]:
        """Convenience method for saving JSON data."""
        return self.save_data(data, file_prefix, DataFormat.JSON, **kwargs)
    
    def save_txt(self, data: Union[str, pd.DataFrame], file_prefix: str = "data", **kwargs) -> Dict[str, Any]:
        """Convenience method for saving text data."""
        return self.save_data(data, file_prefix, DataFormat.TXT, **kwargs)
    
    def save_parquet(self, data: pd.DataFrame, file_prefix: str = "data", **kwargs) -> Dict[str, Any]:
        """Convenience method for saving Parquet data."""
        return self.save_data(data, file_prefix, DataFormat.PARQUET, **kwargs)
    
    def process_collector_data(self, collector_result: Dict, api_config: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Process data from collector and save to S3 with folder organization.
        
        Args:
            collector_result: Result from APIDataCollector containing data and metadata
            api_config: Optional API configuration to get folder information
            
        Returns:
            Dict with upload results and S3 key
        """
        try:
            # Extract data from collector result
            data = collector_result.get('raw_data')
            if not data:
                raise ValueError("No data found in collector result")
            
            # Get folder path from API config or collector metadata
            folder_path = None
            if api_config and 'output_folder' in api_config:
                folder_path = api_config['output_folder']
            elif 'metadata' in collector_result and 'api_name' in collector_result['metadata']:
                # Use API name as fallback folder
                folder_path = collector_result['metadata']['api_name'].lower().replace(' ', '_')
            
            # Get file prefix from API config or use default
            file_prefix = "energy_data"
            if api_config:
                file_prefix = api_config.get('csv_prefix', api_config.get('file_prefix', file_prefix))
            
            # Auto-detect format from collector result
            data_format = None
            if 'file_type' in collector_result:
                try:
                    data_format = DataFormat(collector_result['file_type'])
                except ValueError:
                    # Invalid format, will auto-detect
                    pass
            
            # Create metadata from collector result
            metadata = {
                'collector_timestamp': collector_result.get('metadata', {}).get('timestamp', ''),
                'collector_unique_id': collector_result.get('metadata', {}).get('unique_id', ''),
                'data_source': 'api_collector'
            }
            
            # Add API name if available
            if api_config and 'name' in api_config:
                metadata['api_name'] = api_config['name']
                metadata['api_description'] = api_config.get('description', '')
            
            # Save data to S3
            result = self.save_data(
                data=data,
                file_prefix=file_prefix,
                data_format=data_format,
                metadata=metadata,
                include_timestamp=True,
                folder_path=folder_path
            )
            
            # Add collector information to result
            result['collector_metadata'] = collector_result.get('metadata', {})
            result['folder_path'] = folder_path
            
            self.logger.info(f"Successfully processed collector data to S3: {result['s3_key']}")
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to process collector data: {e}")
            return {
                'success': False,
                'error': str(e),
                'upload_timestamp': datetime.now().isoformat()
            }

    def list_files(self, prefix: str = "", file_format: Optional[DataFormat] = None) -> List[Dict]:
        """
        List files in the S3 bucket with optional prefix and format filters.
        
        Args:
            prefix: Optional prefix to filter files
            file_format: Optional format filter
            
        Returns:
            List of file information dicts
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    file_info = {
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'],
                        'etag': obj['ETag']
                    }
                    
                    # Add format detection
                    if '.' in obj['Key']:
                        extension = obj['Key'].split('.')[-1].lower()
                        file_info['detected_format'] = extension
                    
                    # Filter by format if specified
                    if file_format is None or file_info.get('detected_format') == file_format.value:
                        files.append(file_info)
            
            return files
            
        except Exception as e:
            self.logger.error(f"Failed to list S3 files: {e}")
            return []


# Example usage demonstrating all formats
if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Sample data for different formats
    sample_df = pd.DataFrame({
        'timestamp': ['2025-06-24 10:00:00', '2025-06-24 11:00:00'],
        'energy_consumption': [1250.5, 1180.3],
        'region': ['Northeast', 'Northeast']
    })
    
    sample_json = {
        "metadata": {"source": "energy_api", "version": "1.0"},
        "readings": [
            {"timestamp": "2025-06-24 10:00:00", "value": 1250.5},
            {"timestamp": "2025-06-24 11:00:00", "value": 1180.3}
        ]
    }
    
    sample_text = "Energy consumption report for Northeast region\nGenerated: 2025-06-24\nTotal consumption: 2430.8 kWh"
    
    # Initialize persistence layer
    persistence = S3DataPersistence(bucket_name="s3-for-energy")
    
    metadata = {'data_source': 'energy_api', 'region': 'northeast'}
    
    # Save in different formats with folder organization
    csv_result = persistence.save_data(sample_df, "hourly_consumption", DataFormat.CSV, metadata, folder_path="iso_ne_current_7_day_forecast")
    json_result = persistence.save_data(sample_json, "api_response", DataFormat.JSON, metadata, folder_path="hourly_final_lmp")
    txt_result = persistence.save_data(sample_text, "daily_report", DataFormat.TXT, metadata, folder_path="iso_ne_fuel_mix")
    
    # Example of processing collector data
    sample_collector_result = {
        'raw_data': sample_df,
        'file_type': 'csv',
        'metadata': {
            'api_name': '7 day forecast all zones',
            'timestamp': '2025-07-02T10:00:00',
            'unique_id': 'abc123'
        }
    }
    
    sample_api_config = {
        'name': '7 day forecast all zones',
        'output_folder': 'iso_ne_current_7_day_forecast',
        'csv_prefix': 'demand',
        'description': 'ISO New England 7-Day Forecast'
    }
    
    # Process collector data with folder creation
    collector_result = persistence.process_collector_data(sample_collector_result, sample_api_config)
    
    print("Upload results:")
    for result in [csv_result, json_result, txt_result, collector_result]:
        if result['success']:
            print(f"✓ {result['data_format']}: {result['s3_key']}")
        else:
            print(f"✗ Error: {result['error']}")