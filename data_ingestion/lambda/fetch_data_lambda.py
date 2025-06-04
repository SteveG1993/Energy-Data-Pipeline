import json
import boto3
import requests
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
import logging
import os

data_sources = ['/fiveminutelmp/current']




# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """
    AWS Lambda function to extract energy data from ISO-NE API
    and store it in S3 bucket
    """
    try:
        # Configuration
        bucket_name = os.environ.get('S3_BUCKET_NAME', 's3-for-energy')
        api_base_url = 'https://webservices.iso-ne.com/api/v1.1'
        full_source_url = f'{api_base_url}/{data_sources}'


        # Extract data from API
        energy_data = extract_energy_data(api_base_url)
        
        if not energy_data:
            logger.warning("No data extracted from API")
            return {
                'statusCode': 200,
                'body': json.dumps('No data to process')
            }
        
        # Transform data
        transformed_data = transform_data(energy_data)
        
        # Store data in S3
        s3_key = store_data_in_s3(transformed_data, bucket_name)
        
        logger.info(f"Successfully processed {len(transformed_data)} records")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Data pipeline executed successfully',
                'records_processed': len(transformed_data),
                's3_key': s3_key
            })
        }
        
    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }

def extract_energy_data(full_source_url):
    """
    Extract energy data from ISO-NE API
    """
    try:
        # Get current date and yesterday for data range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1)
        
        # Format dates for API
        start_date_str = start_date.strftime('%Y%m%d')
        end_date_str = end_date.strftime('%Y%m%d')
        
        # API endpoint for real-time load data
        endpoint = f"{full_source_url}"
        
        logger.info(f"Calling API endpoint: {endpoint}")
        
        # Make API request
        response = requests.get(endpoint, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Extract relevant fields
        energy_records = []
        
        if 'GenMixes' in data:
            for mix in data['GenMixes']:
                if 'GenMixs' in mix:
                    for gen_data in mix['GenMixs']:
                        record = {
                            'DateTime': gen_data.get('BeginDate', ''),
                            'kWh': float(gen_data.get('GenMw', 0)) * 1000  # Convert MW to kW
                        }
                        energy_records.append(record)
        
        logger.info(f"Extracted {len(energy_records)} records from API")
        return energy_records
        
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error extracting data: {str(e)}")
        raise

def transform_data(energy_data):
    """
    Transform the extracted data
    """
    try:
        # Convert to DataFrame for easier manipulation
        df = pd.DataFrame(energy_data)
        
        if df.empty:
            return []
        
        # Convert DateTime to proper format
        df['DateTime'] = pd.to_datetime(df['DateTime'])
        
        # Round kWh to tenths place
        df['kWh'] = df['kWh'].round(1)
        
        # Remove duplicates and sort by DateTime
        df = df.drop_duplicates().sort_values('DateTime')
        
        # Convert back to list of dictionaries
        transformed_data = df.to_dict('records')
        
        logger.info(f"Transformed {len(transformed_data)} records")
        return transformed_data
        
    except Exception as e:
        logger.error(f"Error transforming data: {str(e)}")
        raise

def store_data_in_s3(data, bucket_name):
    """
    Store transformed data in S3 bucket
    """
    try:
        # Convert data to CSV format
        df = pd.DataFrame(data)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        
        # Generate S3 key with timestamp
        timestamp = datetime.now().strftime('%Y/%m/%d/%H%M%S')
        s3_key = f"energy-data/{timestamp}/energy_data.csv"
        
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        
        logger.info(f"Data stored in S3: s3://{bucket_name}/{s3_key}")
        return s3_key
        
    except Exception as e:
        logger.error(f"Error storing data in S3: {str(e)}")
        raise