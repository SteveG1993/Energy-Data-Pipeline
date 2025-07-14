"""
Energy Data Pipeline Integration

This module provides integration between data collection and persistence layers,
creating a seamless pipeline for energy data processing.
"""

import logging
import json
import os
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
from pathlib import Path

# Import our custom modules
from app.data_collection.collector import APIDataCollector
from app.data_persisting.persistence import S3DataPersistence, DataFormat


class EnergyDataPipelineIntegration:
    """
    Integration class that orchestrates the complete energy data pipeline.
    
    This class connects the APIDataCollector with S3DataPersistence to create
    a seamless data flow from API collection to cloud storage with proper
    folder organization based on API configuration.
    
    Features:
    - Automatic data collection from configured APIs
    - Intelligent S3 folder organization based on API config
    - Error handling and retry logic
    - Comprehensive logging and monitoring
    - Flexible execution modes (all APIs, specific APIs, or scheduled)
    """
    
    def __init__(self, 
                 config_file: str = 'api_config.json',
                 s3_bucket: str = None,
                 password: Optional[str] = None,
                 aws_profile: Optional[str] = None,
                 region: str = None):
        """
        Initialize the Energy Data Pipeline Integration.
        
        Args:
            config_file: Path to API configuration JSON file
            s3_bucket: S3 bucket name (defaults to S3_BUCKET_NAME env var)
            password: Password for API authentication
            aws_profile: AWS profile name for S3 access
            region: AWS region for S3 operations
        """
        self.config_file = config_file
        self.logger = logging.getLogger(__name__)
        
        # Load API configuration
        self.api_config = self._load_api_config()
        
        # Initialize collector
        self.collector = APIDataCollector(
            config_file=config_file,
            password=password
        )
        
        # Initialize S3 persistence
        s3_bucket = s3_bucket or os.getenv('S3_BUCKET_NAME', 's3-for-energy')
        region = region or os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        
        self.persistence = S3DataPersistence(
            bucket_name=s3_bucket,
            aws_profile=aws_profile,
            region=region
        )
        
        # Statistics tracking
        self.stats = {
            'total_apis': 0,
            'successful_collections': 0,
            'successful_uploads': 0,
            'failed_collections': 0,
            'failed_uploads': 0,
            'start_time': None,
            'end_time': None
        }
        
        self.logger.info(f"Energy Data Pipeline Integration initialized")
        self.logger.info(f"Config file: {config_file}")
        self.logger.info(f"S3 bucket: {s3_bucket}")
        self.logger.info(f"APIs configured: {len(self.api_config.get('apis', []))}")
    
    def _load_api_config(self) -> Dict:
        """Load and validate API configuration."""
        try:
            with open(self.config_file, 'r') as f:
                config = json.load(f)
            
            if 'apis' not in config:
                raise ValueError("API configuration must contain 'apis' array")
            
            self.logger.info(f"Loaded configuration with {len(config['apis'])} APIs")
            return config
            
        except FileNotFoundError:
            self.logger.error(f"Configuration file not found: {self.config_file}")
            raise
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in configuration file: {e}")
            raise
    
    def _find_api_config(self, api_name: str) -> Optional[Dict]:
        """Find API configuration by name."""
        for api_config in self.api_config.get('apis', []):
            if api_config.get('name') == api_name:
                return api_config
        return None
    
    def _process_single_result(self, collector_result: Dict) -> Dict[str, Any]:
        """
        Process a single collector result and upload to S3.
        
        Args:
            collector_result: Result from APIDataCollector
            
        Returns:
            Processing result with upload status
        """
        api_name = collector_result.get('api_name', 'unknown')
        
        try:
            if collector_result.get('status') != 'success':
                return {
                    'api_name': api_name,
                    'collection_status': 'failed',
                    'upload_status': 'skipped',
                    'error': collector_result.get('error', 'Collection failed'),
                    'timestamp': datetime.now().isoformat()
                }
            
            # Find matching API configuration
            api_config = self._find_api_config(api_name)
            if not api_config:
                self.logger.warning(f"No API configuration found for: {api_name}")
                api_config = {
                    'name': api_name,
                    'output_folder': api_name.lower().replace(' ', '_'),
                    'csv_prefix': 'data'
                }
            
            # Process and upload to S3
            upload_result = self.persistence.process_collector_data(
                collector_result, api_config
            )
            
            if upload_result.get('success'):
                self.stats['successful_uploads'] += 1
                self.logger.info(f"Successfully uploaded {api_name} to S3: {upload_result['s3_key']}")
                
                return {
                    'api_name': api_name,
                    'collection_status': 'success',
                    'upload_status': 'success',
                    's3_key': upload_result['s3_key'],
                    'folder_path': upload_result.get('folder_path'),
                    'data_format': upload_result.get('data_format'),
                    'record_count': upload_result.get('record_count'),
                    'file_size_bytes': upload_result.get('file_size_bytes'),
                    'timestamp': upload_result.get('upload_timestamp')
                }
            else:
                self.stats['failed_uploads'] += 1
                self.logger.error(f"Failed to upload {api_name} to S3: {upload_result.get('error')}")
                
                return {
                    'api_name': api_name,
                    'collection_status': 'success',
                    'upload_status': 'failed',
                    'error': upload_result.get('error'),
                    'timestamp': datetime.now().isoformat()
                }
                
        except Exception as e:
            self.stats['failed_uploads'] += 1
            self.logger.error(f"Unexpected error processing {api_name}: {e}")
            
            return {
                'api_name': api_name,
                'collection_status': 'success',
                'upload_status': 'failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def run_full_pipeline(self) -> Dict[str, Any]:
        """
        Execute the complete data pipeline for all configured APIs.
        
        Returns:
            Comprehensive results including statistics and individual API results
        """
        self.logger.info("Starting full energy data pipeline execution")
        self.stats['start_time'] = datetime.now()
        
        # Reset statistics
        self.stats.update({
            'total_apis': len(self.api_config.get('apis', [])),
            'successful_collections': 0,
            'successful_uploads': 0,
            'failed_collections': 0,
            'failed_uploads': 0
        })
        
        try:
            # Run data collection for all APIs
            self.logger.info("Starting data collection phase")
            collection_results = self.collector.run_all_apis()
            
            # Process each result
            pipeline_results = []
            for collector_result in collection_results:
                # Update collection statistics
                if collector_result.get('status') == 'success':
                    self.stats['successful_collections'] += 1
                else:
                    self.stats['failed_collections'] += 1
                
                # Process and upload to S3
                result = self._process_single_result(collector_result)
                pipeline_results.append(result)
            
            self.stats['end_time'] = datetime.now()
            duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
            
            self.logger.info(f"Pipeline execution completed in {duration:.2f} seconds")
            self.logger.info(f"Collections: {self.stats['successful_collections']}/{self.stats['total_apis']} successful")
            self.logger.info(f"Uploads: {self.stats['successful_uploads']}/{self.stats['successful_collections']} successful")
            
            return {
                'pipeline_status': 'completed',
                'statistics': self.stats.copy(),
                'duration_seconds': duration,
                'results': pipeline_results,
                'summary': {
                    'total_apis': self.stats['total_apis'],
                    'successful_end_to_end': self.stats['successful_uploads'],
                    'collection_success_rate': self.stats['successful_collections'] / max(self.stats['total_apis'], 1),
                    'upload_success_rate': self.stats['successful_uploads'] / max(self.stats['successful_collections'], 1) if self.stats['successful_collections'] > 0 else 0
                }
            }
            
        except Exception as e:
            self.stats['end_time'] = datetime.now()
            self.logger.error(f"Pipeline execution failed: {e}")
            
            return {
                'pipeline_status': 'failed',
                'error': str(e),
                'statistics': self.stats.copy(),
                'timestamp': datetime.now().isoformat()
            }
    
    def run_single_api_pipeline(self, api_name: str) -> Dict[str, Any]:
        """
        Execute the pipeline for a single API.
        
        Args:
            api_name: Name of the API to process
            
        Returns:
            Processing result for the specific API
        """
        self.logger.info(f"Starting single API pipeline for: {api_name}")
        
        try:
            # Run collection for single API
            collector_result = self.collector.run_single_api(api_name)
            
            # Process and upload
            result = self._process_single_result(collector_result)
            
            self.logger.info(f"Single API pipeline completed for: {api_name}")
            return result
            
        except Exception as e:
            self.logger.error(f"Single API pipeline failed for {api_name}: {e}")
            return {
                'api_name': api_name,
                'collection_status': 'failed',
                'upload_status': 'failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def run_selective_pipeline(self, api_names: List[str]) -> Dict[str, Any]:
        """
        Execute the pipeline for selected APIs.
        
        Args:
            api_names: List of API names to process
            
        Returns:
            Results for all selected APIs
        """
        self.logger.info(f"Starting selective pipeline for APIs: {api_names}")
        
        results = []
        for api_name in api_names:
            result = self.run_single_api_pipeline(api_name)
            results.append(result)
        
        successful = sum(1 for r in results if r.get('upload_status') == 'success')
        
        return {
            'pipeline_status': 'completed',
            'selected_apis': api_names,
            'successful_count': successful,
            'total_count': len(api_names),
            'success_rate': successful / len(api_names) if api_names else 0,
            'results': results,
            'timestamp': datetime.now().isoformat()
        }
    
    def validate_configuration(self) -> Dict[str, Any]:
        """
        Validate the current configuration and connectivity.
        
        Returns:
            Validation results
        """
        validation_results = {
            'config_valid': True,
            'apis_configured': len(self.api_config.get('apis', [])),
            'issues': [],
            'warnings': []
        }
        
        # Validate API configurations
        for i, api_config in enumerate(self.api_config.get('apis', [])):
            required_fields = ['name', 'url', 'output_folder']
            for field in required_fields:
                if field not in api_config:
                    validation_results['issues'].append(f"API {i}: Missing required field '{field}'")
                    validation_results['config_valid'] = False
            
            # Check for recommended fields
            recommended_fields = ['csv_prefix', 'description']
            for field in recommended_fields:
                if field not in api_config:
                    validation_results['warnings'].append(f"API {i} ({api_config.get('name', 'unnamed')}): Missing recommended field '{field}'")
        
        # Test S3 connectivity
        try:
            self.persistence._verify_bucket_access()
            validation_results['s3_accessible'] = True
        except Exception as e:
            validation_results['s3_accessible'] = False
            validation_results['issues'].append(f"S3 access failed: {e}")
            validation_results['config_valid'] = False
        
        self.logger.info(f"Configuration validation: {'PASSED' if validation_results['config_valid'] else 'FAILED'}")
        
        return validation_results
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get current pipeline status and statistics."""
        return {
            'configuration': {
                'config_file': self.config_file,
                'apis_configured': len(self.api_config.get('apis', [])),
                's3_bucket': self.persistence.bucket_name
            },
            'last_run_statistics': self.stats.copy(),
            'timestamp': datetime.now().isoformat()
        }


# Example usage and testing
if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize the pipeline
        logger.info("Initializing Energy Data Pipeline Integration")
        pipeline = EnergyDataPipelineIntegration()
        
        # Validate configuration
        logger.info("Validating configuration")
        validation = pipeline.validate_configuration()
        
        if not validation['config_valid']:
            logger.error("Configuration validation failed:")
            for issue in validation['issues']:
                logger.error(f"  - {issue}")
            exit(1)
        
        if validation['warnings']:
            logger.warning("Configuration warnings:")
            for warning in validation['warnings']:
                logger.warning(f"  - {warning}")
        
        # Run the full pipeline
        logger.info("Starting full pipeline execution")
        results = pipeline.run_full_pipeline()
        
        # Display results
        print("\n" + "="*60)
        print("ENERGY DATA PIPELINE RESULTS")
        print("="*60)
        
        if results['pipeline_status'] == 'completed':
            summary = results['summary']
            print(f"Total APIs: {summary['total_apis']}")
            print(f"Successful end-to-end: {summary['successful_end_to_end']}")
            print(f"Collection success rate: {summary['collection_success_rate']:.1%}")
            print(f"Upload success rate: {summary['upload_success_rate']:.1%}")
            print(f"Duration: {results['duration_seconds']:.2f} seconds")
            
            print("\nIndividual API Results:")
            for result in results['results']:
                status_icon = "✓" if result['upload_status'] == 'success' else "✗"
                print(f"{status_icon} {result['api_name']}: {result['upload_status']}")
                if result['upload_status'] == 'success':
                    print(f"    S3 Key: {result['s3_key']}")
                    print(f"    Records: {result.get('record_count', 'N/A')}")
                elif 'error' in result:
                    print(f"    Error: {result['error']}")
        
        else:
            print(f"Pipeline failed: {results.get('error', 'Unknown error')}")
        
    except KeyboardInterrupt:
        logger.info("Pipeline execution interrupted by user")
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise