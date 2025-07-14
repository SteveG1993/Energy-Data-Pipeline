import boto3
import pandas as pd
from io import StringIO, BytesIO
import json
from botocore.exceptions import NoCredentialsError, ClientError
from typing import Union, Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def save_to_s3(data: Union[pd.DataFrame, dict, str, bytes],
               file_key: str,
               bucket_name: str = 's3-for-energy',
               file_format: str = 'csv',
               aws_access_key_id: str = None,
               aws_secret_access_key: str = None,
               region_name: str = 'us-east-1') -> bool:
    """
    Save data to AWS S3 bucket in various formats.

    Parameters:
    -----------
    data : pd.DataFrame, dict, str, or bytes
        The data to save. Can be a pandas DataFrame, dictionary, string, or bytes.
    file_key : str
        The S3 key (path/filename) where the file will be stored.
    bucket_name : str, default 's3-for-energy'
        The name of the S3 bucket.
    file_format : str, default 'csv'
        Format to save the data in ('csv', 'json', 'parquet', 'txt', 'binary').
    aws_access_key_id : str, optional
        AWS access key ID. If None, uses default AWS credentials.
    aws_secret_access_key : str, optional
        AWS secret access key. If None, uses default AWS credentials.
    region_name : str, default 'us-east-1'
        AWS region name.

    Returns:
    --------
    bool
        True if successful, False otherwise.

    Examples:
    ---------
    # Save DataFrame as CSV
    df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
    save_to_s3(df, 'data/my_data.csv', file_format='csv')

    # Save dictionary as JSON
    data_dict = {'key1': 'value1', 'key2': [1, 2, 3]}
    save_to_s3(data_dict, 'data/my_data.json', file_format='json')

    # Save string as text file
    text_data = "This is some text data"
    save_to_s3(text_data, 'data/my_text.txt', file_format='txt')
    """

    try:
        # Initialize S3 client
        if aws_access_key_id and aws_secret_access_key:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name
            )
        else:
            # Use default credentials (from ~/.aws/credentials, environment variables, or IAM role)
            s3_client = boto3.client('s3', region_name=region_name)

        # Convert data based on format
        if file_format.lower() == 'csv':
            if isinstance(data, pd.DataFrame):
                buffer = StringIO()
                data.to_csv(buffer, index=False)
                file_content = buffer.getvalue().encode('utf-8')
            else:
                raise ValueError("CSV format requires pandas DataFrame input")

        elif file_format.lower() == 'json':
            if isinstance(data, (dict, list)):
                file_content = json.dumps(data, indent=2).encode('utf-8')
            elif isinstance(data, pd.DataFrame):
                file_content = data.to_json(orient='records', indent=2).encode('utf-8')
            else:
                raise ValueError("JSON format requires dict, list, or DataFrame input")

        elif file_format.lower() == 'parquet':
            if isinstance(data, pd.DataFrame):
                buffer = BytesIO()
                data.to_parquet(buffer, index=False)
                file_content = buffer.getvalue()
            else:
                raise ValueError("Parquet format requires pandas DataFrame input")

        elif file_format.lower() == 'txt':
            if isinstance(data, str):
                file_content = data.encode('utf-8')
            else:
                file_content = str(data).encode('utf-8')

        elif file_format.lower() == 'binary':
            if isinstance(data, bytes):
                file_content = data
            else:
                raise ValueError("Binary format requires bytes input")
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=file_content
        )

        logger.info(f"Successfully uploaded {file_key} to {bucket_name}")
        return True

    except NoCredentialsError:
        logger.error("AWS credentials not found. Please configure your credentials.")
        return False
    except ClientError as e:
        logger.error(f"AWS Client Error: {e}")
        return False
    except Exception as e:
        logger.error(f"Error uploading to S3: {e}")
        return False


def save_dataframe_to_s3(df: pd.DataFrame,
                         file_key: str,
                         bucket_name: str = 's3-for-energy',
                         file_format: str = 'csv') -> bool:
    """
    Simplified function specifically for saving pandas DataFrames to S3.

    Parameters:
    -----------
    df : pd.DataFrame
        The DataFrame to save.
    file_key : str
        The S3 key (path/filename) where the file will be stored.
    bucket_name : str, default 's3-for-energy'
        The name of the S3 bucket.
    file_format : str, default 'csv'
        Format to save the DataFrame in ('csv', 'json', 'parquet').

    Returns:
    --------
    bool
        True if successful, False otherwise.
    """
    return save_to_s3(df, file_key, bucket_name, file_format)


# Example usage and testing
if __name__ == "__main__":
    # Create sample data for testing
    sample_df = pd.DataFrame({
        'date': pd.date_range('2024-01-01', periods=100),
        'energy_consumption': pd.np.random.uniform(100, 1000, 100),
        'temperature': pd.np.random.uniform(-10, 35, 100),
        'region': pd.np.random.choice(['North', 'South', 'East', 'West'], 100)
    })

    # Test different formats
    print("Testing S3 upload functions...")

    # Test CSV upload
    success = save_to_s3(sample_df, 'energy_data/test_consumption.csv', file_format='csv')
    print(f"CSV upload: {'Success' if success else 'Failed'}")

    # Test JSON upload
    metadata = {
        'dataset': 'energy_consumption',
        'created_date': '2024-01-01',
        'records': len(sample_df),
        'columns': list(sample_df.columns)
    }
    success = save_to_s3(metadata, 'energy_data/test_metadata.json', file_format='json')
    print(f"JSON upload: {'Success' if success else 'Failed'}")