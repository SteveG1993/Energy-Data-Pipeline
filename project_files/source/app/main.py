"""
Energy Data Pipeline - Main Module

This module orchestrates the collection and persistence of energy data from various APIs.
It fetches data using APIDataCollector and stores it in S3 using S3DataPersistence.
"""

import logging
import os
import sys
from datetime import datetime
from typing import Dict, Any, Optional, List

from data_collection.collector import APIDataCollector
from data_persisting.persistence import S3DataPersistence

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('energy_pipeline.log', mode='a')
    ]
)
logger = logging.getLogger(__name__)


class EnergyPipelineConfig:
    """Configuration class for the energy pipeline."""
    
    def __init__(self):
        self.bucket_name = os.getenv('S3_BUCKET_NAME', 's3-for-energy')
        self.region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        self.api_config_file = os.getenv('API_CONFIG_FILE', 'api_config.json')
        self.default_region = os.getenv('DEFAULT_ENERGY_REGION', 'northeast')
        self.file_prefix = os.getenv('DATA_FILE_PREFIX', 'energy_data')
    
    def validate(self) -> bool:
        """Validate configuration parameters."""
        if not self.bucket_name:
            logger.error("S3 bucket name is required")
            return False
        
        if not os.path.exists(self.api_config_file):
            logger.error(f"API config file not found: {self.api_config_file}")
            return False
        
        return True


class EnergyPipeline:
    """Main energy data pipeline orchestrator."""
    
    def __init__(self, config: Optional[EnergyPipelineConfig] = None):
        """Initialize the energy pipeline.
        
        Args:
            config: Pipeline configuration. If None, creates default config.
        """
        self.config = config or EnergyPipelineConfig()
        self.api_collector = None
        self.persistence = None
        
        if not self.config.validate():
            raise ValueError("Invalid pipeline configuration")
    
    def initialize_components(self) -> None:
        """Initialize API collector and persistence components."""
        try:
            logger.info("Initializing pipeline components...")
            
            # Initialize API data collector
            self.api_collector = APIDataCollector(
                config_file=self.config.api_config_file,
                base_output_dir='api_data'
            )
            
            # Initialize S3 persistence
            self.persistence = S3DataPersistence(
                bucket_name=self.config.bucket_name,
                region=self.config.region
            )
            
            logger.info("Components initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize components: {e}")
            raise
    
    def process_api_result(self, api_result: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single API result and save to S3.
        
        Args:
            api_result: Result from API data collection
            
        Returns:
            Upload result dictionary
        """
        try:
            if api_result.get('status') != 'success':
                return {
                    'success': False,
                    'error': f"API call failed: {api_result.get('error', 'Unknown error')}"
                }
            
            # Prepare metadata
            metadata = {
                'api_name': api_result.get('api_name', 'unknown'),
                'collection_timestamp': datetime.now().isoformat(),
                'source': 'energy_pipeline'
            }
            
            # Add any existing metadata from API result
            if 'metadata' in api_result:
                metadata.update(api_result['metadata'])
            
            # Save data to S3
            if 'file_path' in api_result.get('metadata', {}):
                # Data was saved to a file, read and upload
                with open(api_result['metadata']['file_path'], 'r') as f:
                    data = f.read()
                
                upload_result = self.persistence.save_data(
                    data=data,
                    file_prefix=f"{self.config.file_prefix}_{api_result['api_name']}",
                    metadata=metadata
                )
            else:
                # Raw data, save directly
                upload_result = self.persistence.save_data(
                    data=api_result['raw_data'],
                    file_prefix=f"{self.config.file_prefix}_{api_result['api_name']}",
                    metadata=metadata
                )
            
            return upload_result
            
        except Exception as e:
            logger.error(f"Failed to process API result: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def run_single_api(self, api_name: str) -> Dict[str, Any]:
        """Run a single API and save results.
        
        Args:
            api_name: Name of the API to run
            
        Returns:
            Processing result dictionary
        """
        if not self.api_collector or not self.persistence:
            self.initialize_components()
        
        try:
            logger.info(f"Running single API: {api_name}")
            
            # Collect data from API
            api_result = self.api_collector.run_single_api(api_name)
            
            # Process and save to S3
            upload_result = self.process_api_result(api_result)
            
            if upload_result['success']:
                logger.info(f"Successfully processed {api_name}: {upload_result['s3_key']}")
            else:
                logger.error(f"Failed to process {api_name}: {upload_result.get('error')}")
            
            return {
                'api_name': api_name,
                'api_result': api_result,
                'upload_result': upload_result
            }
            
        except Exception as e:
            logger.error(f"Error running single API {api_name}: {e}")
            return {
                'api_name': api_name,
                'success': False,
                'error': str(e)
            }
    
    def run_all_apis(self) -> List[Dict[str, Any]]:
        """Run all configured APIs and save results.
        
        Returns:
            List of processing results
        """
        if not self.api_collector or not self.persistence:
            self.initialize_components()
        
        try:
            logger.info("Running all APIs...")
            
            # Collect data from all APIs
            api_results = self.api_collector.run_all_apis()
            
            # Process each result
            pipeline_results = []
            for api_result in api_results:
                upload_result = self.process_api_result(api_result)
                
                pipeline_results.append({
                    'api_name': api_result.get('api_name', 'unknown'),
                    'api_result': api_result,
                    'upload_result': upload_result
                })
                
                if upload_result['success']:
                    logger.info(f"Successfully processed {api_result.get('api_name')}: {upload_result['s3_key']}")
                else:
                    logger.error(f"Failed to process {api_result.get('api_name')}: {upload_result.get('error')}")
            
            return pipeline_results
            
        except Exception as e:
            logger.error(f"Error running all APIs: {e}")
            return []
    
    def run_energy_pipeline(self, region: Optional[str] = None) -> Dict[str, Any]:
        """Run the complete energy pipeline for a specific region.
        
        Args:
            region: Energy region to process (defaults to config default)
            
        Returns:
            Pipeline execution summary
        """
        region = region or self.config.default_region
        
        try:
            logger.info(f"Starting energy pipeline for region: {region}")
            
            # Run all APIs
            results = self.run_all_apis()
            
            # Generate summary
            successful = sum(1 for r in results if r['upload_result'].get('success', False))
            failed = len(results) - successful
            
            summary = {
                'region': region,
                'total_apis': len(results),
                'successful': successful,
                'failed': failed,
                'execution_time': datetime.now().isoformat(),
                'results': results
            }
            
            logger.info(f"Pipeline completed: {successful}/{len(results)} APIs successful")
            return summary
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            return {
                'region': region,
                'success': False,
                'error': str(e),
                'execution_time': datetime.now().isoformat()
            }


def run_energy_pipeline(region: Optional[str] = None) -> Dict[str, Any]:
    """Legacy function for backward compatibility.
    
    Args:
        region: Energy region to process
        
    Returns:
        Pipeline execution result
    """
    pipeline = EnergyPipeline()
    return pipeline.run_energy_pipeline(region)


def main():
    """Main entry point for the energy pipeline."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Energy Data Pipeline')
    parser.add_argument('--region', default='northeast', help='Energy region to process')
    parser.add_argument('--api', help='Run specific API only')
    parser.add_argument('--all', action='store_true', help='Run all APIs')
    parser.add_argument('--config', help='Path to API config file')
    
    args = parser.parse_args()
    
    try:
        # Create configuration
        config = EnergyPipelineConfig()
        if args.config:
            config.api_config_file = args.config
        
        # Initialize pipeline
        pipeline = EnergyPipeline(config)
        
        if args.api:
            # Run single API
            result = pipeline.run_single_api(args.api)
            print(f"API {args.api} result: {result['upload_result'].get('success', False)}")
        elif args.all:
            # Run all APIs
            results = pipeline.run_all_apis()
            successful = sum(1 for r in results if r['upload_result'].get('success', False))
            print(f"Completed: {successful}/{len(results)} APIs successful")
        else:
            # Run full pipeline
            summary = pipeline.run_energy_pipeline(args.region)
            print(f"Pipeline completed for {args.region}: {summary['successful']}/{summary['total_apis']} successful")
    
    except Exception as e:
        logger.error(f"Main execution failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()