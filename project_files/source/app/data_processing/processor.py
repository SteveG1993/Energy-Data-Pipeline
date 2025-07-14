# src/processor.py
"""
Data Processing Module for Energy Data Pipeline

This module handles the transformation and processing of LMP data extracted
from the ISO New England API. It calculates hourly averages, adds derived
fields, validates data quality, and saves data in multiple formats.

Key Functions:
- Calculate hourly averages from 15-minute intervals
- Add time-based derived fields
- Validate data quality and completeness
- Save data to SQLite, CSV, and JSON formats
- Handle data partitioning for efficient storage
"""

import pandas as pd
import numpy as np
import sqlite3
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import logging
from pathlib import Path

from config import LocalConfig


class DataProcessor:
    """
    Processes and transforms LMP data for analysis and storage

    This class handles all data transformation operations including:
    - Hourly average calculations
    - Data validation and quality checks
    - Multiple format storage (SQLite, CSV, JSON)
    - Data partitioning and organization
    """

    def __init__(self, config: LocalConfig):
        """
        Initialize the DataProcessor

        Args:
            config (LocalConfig): Configuration object with storage paths and settings
        """
        self.config = config
        self.logger = self._setup_logging()
        self._ensure_directories()

    def _setup_logging(self) -> logging.Logger:
        """Setup module-specific logging"""
        logger = logging.getLogger(__name__)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

    def _ensure_directories(self):
        """Create necessary directories if they don't exist"""
        directories = [
            self.config.DATA_DIR,
            f"{self.config.DATA_DIR}/raw",
            f"{self.config.DATA_DIR}/processed",
            f"{self.config.DATA_DIR}/backups",
            self.config.LOG_DIR
        ]

        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)

    def process_raw_data(self, raw_data: List[Dict], date: str) -> pd.DataFrame:
        """
        Process raw LMP data through the complete transformation pipeline

        Args:
            raw_data (List[Dict]): Raw LMP records from API
            date (str): Date string in YYYYMMDD format

        Returns:
            pd.DataFrame: Fully processed dataframe with hourly averages
        """
        if not raw_data:
            self.logger.warning(f"No raw data provided for processing on {date}")
            return pd.DataFrame()

        self.logger.info(f"Processing {len(raw_data)} raw LMP records for {date}")

        # Convert to DataFrame
        df = pd.DataFrame(raw_data)

        # Validate input data
        if not self._validate_input_data(df):
            self.logger.error("Input data validation failed")
            return pd.DataFrame()

        # Clean and standardize data
        df = self._clean_data(df)

        # Add time-based derived fields
        df = self._add_time_fields(df)

        # Calculate hourly averages
        df = self._calculate_hourly_averages(df)

        # Add additional derived fields
        df = self._add_derived_fields(df)

        # Validate processed data
        if self._validate_processed_data(df):
            self.logger.info(f"Successfully processed {len(df)} records for {date}")
            return df
        else:
            self.logger.error("Processed data validation failed")
            return pd.DataFrame()

    def _validate_input_data(self, df: pd.DataFrame) -> bool:
        """
        Validate input data quality and completeness

        Args:
            df (pd.DataFrame): Input dataframe to validate

        Returns:
            bool: True if validation passes, False otherwise
        """
        required_columns = [
            'timestamp', 'location_id', 'lmp_total',
            'energy_component', 'congestion_component', 'loss_component'
        ]

        # Check required columns exist
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            self.logger.error(f"Missing required columns: {missing_columns}")
            return False

        # Check for null values in critical fields
        critical_nulls = df[['timestamp', 'location_id', 'lmp_total']].isnull().sum()
        if critical_nulls.any():
            self.logger.error(f"Null values in critical fields: {critical_nulls[critical_nulls > 0].to_dict()}")
            return False

        # Check for reasonable LMP values (-1000 to 1000 $/MWh)
        lmp_outliers = ~df['lmp_total'].between(-1000, 1000)
        if lmp_outliers.any():
            outlier_count = lmp_outliers.sum()
            self.logger.warning(f"Found {outlier_count} LMP values outside reasonable range")
            # Log sample outliers
            sample_outliers = df[lmp_outliers][['timestamp', 'location_id', 'lmp_total']].head()
            self.logger.warning(f"Sample outliers:\n{sample_outliers}")

        return True

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize the input data

        Args:
            df (pd.DataFrame): Input dataframe

        Returns:
            pd.DataFrame: Cleaned dataframe
        """
        df = df.copy()

        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Remove any duplicate records (same timestamp + location)
        initial_count = len(df)
        df = df.drop_duplicates(subset=['timestamp', 'location_id'], keep='first')
        removed_count = initial_count - len(df)

        if removed_count > 0:
            self.logger.warning(f"Removed {removed_count} duplicate records")

        # Sort by timestamp and location for consistent processing
        df = df.sort_values(['timestamp', 'location_id']).reset_index(drop=True)

        # Clean location names (remove extra whitespace, standardize format)
        df['location_name'] = df['location_name'].str.strip()
        df['location_id'] = df['location_id'].str.strip()

        # Ensure numeric fields are properly typed
        numeric_columns = ['lmp_total', 'energy_component', 'congestion_component', 'loss_component']
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Fill any NaN values in numeric columns with 0 (after logging)
        nan_counts = df[numeric_columns].isnull().sum()
        if nan_counts.any():
            self.logger.warning(f"NaN values found and filled with 0: {nan_counts[nan_counts > 0].to_dict()}")
            df[numeric_columns] = df[numeric_columns].fillna(0)

        return df

    def _add_time_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add time-based derived fields for analysis

        Args:
            df (pd.DataFrame): Input dataframe with timestamp column

        Returns:
            pd.DataFrame: Dataframe with additional time fields
        """
        df = df.copy()

        # Basic time components
        df['date'] = df['timestamp'].dt.date.astype(str)
        df['hour_of_day'] = df['timestamp'].dt.hour
        df['minute_of_hour'] = df['timestamp'].dt.minute
        df['day_of_week'] = df['timestamp'].dt.dayofweek  # 0=Monday, 6=Sunday
        df['day_of_year'] = df['timestamp'].dt.dayofyear
        df['week_of_year'] = df['timestamp'].dt.isocalendar().week
        df['month'] = df['timestamp'].dt.month
        df['quarter'] = df['timestamp'].dt.quarter
        df['year'] = df['timestamp'].dt.year

        # Hour grouping for calculations
        df['hour'] = df['timestamp'].dt.floor('H')

        # Business time classifications
        df['is_weekend'] = df['day_of_week'].isin([5, 6])  # Saturday, Sunday
        df['is_peak_hour'] = df['hour_of_day'].between(7, 22)  # 7 AM to 10 PM
        df['is_business_hour'] = df['hour_of_day'].between(9, 17)  # 9 AM to 5 PM

        # Season classification (meteorological seasons)
        df['season'] = df['month'].map({
            12: 'Winter', 1: 'Winter', 2: 'Winter',
            3: 'Spring', 4: 'Spring', 5: 'Spring',
            6: 'Summer', 7: 'Summer', 8: 'Summer',
            9: 'Fall', 10: 'Fall', 11: 'Fall'
        })

        # Time period classifications for energy markets
        df['time_period'] = 'Off-Peak'
        df.loc[df['is_peak_hour'] & ~df['is_weekend'], 'time_period'] = 'Peak'
        df.loc[df['is_weekend'], 'time_period'] = 'Weekend'

        return df

    def _calculate_hourly_averages(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate hourly averages from 15-minute interval data

        Args:
            df (pd.DataFrame): Dataframe with 15-minute interval data

        Returns:
            pd.DataFrame: Dataframe with hourly average columns added
        """
        df = df.copy()

        # Define columns to calculate hourly averages for
        avg_columns = ['lmp_total', 'energy_component', 'congestion_component', 'loss_component']

        # Calculate hourly averages grouped by location and hour
        hourly_stats = df.groupby(['location_id', 'hour']).agg({
            **{col: ['mean', 'min', 'max', 'std', 'count'] for col in avg_columns}
        }).round(4)

        # Flatten column names
        hourly_stats.columns = [f"{col[0]}_{col[1]}_hourly" for col in hourly_stats.columns]

        # Merge back to original dataframe
        df = df.merge(
            hourly_stats,
            left_on=['location_id', 'hour'],
            right_index=True,
            how='left'
        )

        # Log summary statistics
        for col in avg_columns:
            mean_col = f"{col}_mean_hourly"
            if mean_col in df.columns:
                avg_value = df[mean_col].mean()
                self.logger.info(f"Average {col} across all hours/zones: ${avg_value:.2f}/MWh")

        return df

    def _add_derived_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add additional derived fields for analysis

        Args:
            df (pd.DataFrame): Processed dataframe

        Returns:
            pd.DataFrame: Dataframe with additional derived fields
        """
        df = df.copy()

        # Price volatility indicators
        df['lmp_volatility_hourly'] = df['lmp_total_std_hourly'].fillna(0)
        df['is_high_volatility'] = df['lmp_volatility_hourly'] > df['lmp_volatility_hourly'].quantile(0.75)

        # Congestion indicators
        df['has_congestion'] = df['congestion_component'] > 1.0  # $1/MWh threshold
        df['congestion_level'] = pd.cut(
            df['congestion_component'],
            bins=[-np.inf, 0, 5, 15, np.inf],
            labels=['None', 'Low', 'Medium', 'High']
        )

        # Loss indicators
        df['loss_percentage'] = (df['loss_component'] / df['lmp_total'] * 100).round(2)
        df['loss_percentage'] = df['loss_percentage'].clip(lower=0, upper=100)  # Cap at reasonable bounds

        # Price comparison to daily average
        daily_avg = df.groupby(['location_id', 'date'])['lmp_total'].transform('mean')
        df['price_vs_daily_avg'] = ((df['lmp_total'] - daily_avg) / daily_avg * 100).round(2)

        # Regional price differences (compared to system average)
        system_avg = df.groupby('timestamp')['lmp_total'].transform('mean')
        df['price_vs_system_avg'] = ((df['lmp_total'] - system_avg) / system_avg * 100).round(2)

        # Data quality indicators
        df['data_completeness_hourly'] = (df['lmp_total_count_hourly'] / 4 * 100).round(1)  # % of 15-min intervals
        df['is_complete_hour'] = df['data_completeness_hourly'] == 100.0

        # Processing metadata
        df['processing_timestamp'] = datetime.now().isoformat()
        df['data_version'] = '1.0'

        return df

    def _validate_processed_data(self, df: pd.DataFrame) -> bool:
        """
        Validate the processed data quality

        Args:
            df (pd.DataFrame): Processed dataframe

        Returns:
            bool: True if validation passes, False otherwise
        """
        # Check that hourly averages were calculated
        required_hourly_cols = ['lmp_total_mean_hourly', 'energy_component_mean_hourly']
        missing_hourly = [col for col in required_hourly_cols if col not in df.columns]
        if missing_hourly:
            self.logger.error(f"Missing hourly average columns: {missing_hourly}")
            return False

        # Check data completeness by zone
        completeness_check = df.groupby('location_id').agg({
            'data_completeness_hourly': 'mean',
            'timestamp': 'count'
        })

        low_completeness = completeness_check[completeness_check['data_completeness_hourly'] < 80]
        if not low_completeness.empty:
            self.logger.warning(f"Zones with low data completeness:\n{low_completeness}")

        # Check for reasonable number of records per zone (expect ~96 per day)
        records_per_zone = completeness_check['timestamp']
        if records_per_zone.min() < 80:  # Allow some tolerance
            self.logger.warning(f"Some zones have very few records: min={records_per_zone.min()}")

        return True

    def save_to_storage(self, df: pd.DataFrame, date: str) -> Dict[str, str]:
        """
        Save processed data to multiple storage formats

        Args:
            df (pd.DataFrame): Processed dataframe to save
            date (str): Date string in YYYYMMDD format

        Returns:
            Dict[str, str]: Dictionary with file paths of saved data
        """
        if df.empty:
            self.logger.warning(f"Empty dataframe provided for saving on {date}")
            return {}

        saved_files = {}

        try:
            # Save to SQLite database
            sqlite_path = self._save_to_sqlite(df)
            saved_files['sqlite'] = sqlite_path

            # Save to CSV
            csv_path = self._save_to_csv(df, date)
            saved_files['csv'] = csv_path

            # Save to JSON (raw backup)
            json_path = self._save_to_json(df, date)
            saved_files['json'] = json_path

            # Save metadata
            metadata_path = self._save_metadata(df, date)
            saved_files['metadata'] = metadata_path

            self.logger.info(f"Data saved successfully for {date}:")
            for format_type, path in saved_files.items():
                self.logger.info(f"  {format_type}: {path}")

            return saved_files

        except Exception as e:
            self.logger.error(f"Error saving data for {date}: {str(e)}")
            return {}

    def _save_to_sqlite(self, df: pd.DataFrame) -> str:
        """Save data to SQLite database with upsert logic"""
        conn = sqlite3.connect(self.config.DB_PATH)

        # Use replace method to handle duplicates
        df.to_sql('lmp_data', conn, if_exists='append', index=False, method='replace')

        conn.close()
        return self.config.DB_PATH

    def _save_to_csv(self, df: pd.DataFrame, date: str) -> str:
        """Save data to CSV file"""
        csv_path = f"{self.config.DATA_DIR}/processed/lmp_data_{date}.csv"
        df.to_csv(csv_path, index=False)
        return csv_path

    def _save_to_json(self, df: pd.DataFrame, date: str) -> str:
        """Save data to JSON file for backup"""
        json_path = f"{self.config.DATA_DIR}/raw/lmp_processed_{date}.json"

        # Convert datetime objects to strings for JSON serialization
        df_json = df.copy()
        datetime_columns = df_json.select_dtypes(include=['datetime64']).columns
        for col in datetime_columns:
            df_json[col] = df_json[col].dt.isoformat()

        df_json.to_json(json_path, orient='records', date_format='iso', indent=2)
        return json_path

    def _save_metadata(self, df: pd.DataFrame, date: str) -> str:
        """Save processing metadata"""
        metadata = {
            'date': date,
            'processing_timestamp': datetime.now().isoformat(),
            'record_count': len(df),
            'unique_locations': df['location_id'].nunique(),
            'locations': sorted(df['location_id'].unique().tolist()),
            'time_range': {
                'start': df['timestamp'].min().isoformat(),
                'end': df['timestamp'].max().isoformat()
            },
            'data_quality': {
                'avg_completeness': df['data_completeness_hourly'].mean(),
                'min_completeness': df['data_completeness_hourly'].min(),
                'complete_hours': df['is_complete_hour'].sum(),
                'total_hours': df['hour'].nunique()
            },
            'price_summary': {
                'avg_lmp': df['lmp_total'].mean(),
                'min_lmp': df['lmp_total'].min(),
                'max_lmp': df['lmp_total'].max(),
                'zones_with_congestion': df[df['has_congestion']]['location_id'].nunique()
            }
        }

        metadata_path = f"{self.config.DATA_DIR}/processed/metadata_{date}.json"
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)

        return metadata_path

    def get_processing_summary(self, df: pd.DataFrame) -> Dict:
        """
        Generate a processing summary for logging and monitoring

        Args:
            df (pd.DataFrame): Processed dataframe

        Returns:
            Dict: Summary statistics
        """
        if df.empty:
            return {'status': 'empty_dataframe'}

        summary = {
            'status': 'success',
            'record_count': len(df),
            'unique_locations': df['location_id'].nunique(),
            'date_range': {
                'start': df['timestamp'].min(),
                'end': df['timestamp'].max()
            },
            'avg_lmp_by_zone': df.groupby('location_id')['lmp_total'].mean().round(2).to_dict(),
            'data_completeness': {
                'avg': df['data_completeness_hourly'].mean(),
                'min': df['data_completeness_hourly'].min()
            },
            'congestion_summary': {
                'intervals_with_congestion': df['has_congestion'].sum(),
                'zones_with_congestion': df[df['has_congestion']]['location_id'].nunique()
            }
        }

        return summary


# Utility functions for data processing
def calculate_load_weighted_average(df: pd.DataFrame, price_col: str, load_col: str) -> pd.Series:
    """
    Calculate load-weighted average prices

    Args:
        df (pd.DataFrame): Dataframe with price and load data
        price_col (str): Column name for prices
        load_col (str): Column name for load values

    Returns:
        pd.Series: Load-weighted average prices
    """
    return (df[price_col] * df[load_col]).sum() / df[load_col].sum()


def detect_price_anomalies(df: pd.DataFrame, price_col: str = 'lmp_total',
                           method: str = 'iqr', threshold: float = 3.0) -> pd.Series:
    """
    Detect price anomalies using statistical methods

    Args:
        df (pd.DataFrame): Dataframe with price data
        price_col (str): Column name for prices
        method (str): Method to use ('iqr', 'zscore', 'modified_zscore')
        threshold (float): Threshold for anomaly detection

    Returns:
        pd.Series: Boolean series indicating anomalies
    """
    prices = df[price_col]

    if method == 'iqr':
        Q1 = prices.quantile(0.25)
        Q3 = prices.quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - threshold * IQR
        upper_bound = Q3 + threshold * IQR
        return (prices < lower_bound) | (prices > upper_bound)

    elif method == 'zscore':
        z_scores = np.abs((prices - prices.mean()) / prices.std())
        return z_scores > threshold

    elif method == 'modified_zscore':
        median = prices.median()
        mad = np.median(np.abs(prices - median))
        modified_z_scores = 0.6745 * (prices - median) / mad
        return np.abs(modified_z_scores) > threshold

    else:
        raise ValueError(f"Unknown method: {method}")


if __name__ == "__main__":
    # Example usage for testing
    from config import LocalConfig

    config = LocalConfig()
    processor = DataProcessor(config)

    # Example raw data for testing
    sample_data = [
        {
            'timestamp': '2025-06-04T10:00:00Z',
            'location_id': '.Z.MAINE',
            'location_name': 'Maine',
            'lmp_total': 45.5,
            'energy_component': 40.0,
            'congestion_component': 3.5,
            'loss_component': 2.0,
            'extraction_time': '2025-06-04T10:05:00Z'
        },
        {
            'timestamp': '2025-06-04T10:15:00Z',
            'location_id': '.Z.MAINE',
            'location_name': 'Maine',
            'lmp_total': 46.2,
            'energy_component': 40.5,
            'congestion_component': 3.7,
            'loss_component': 2.0,
            'extraction_time': '2025-06-04T10:20:00Z'
        }
    ]

    # Process the sample data
    processed_df = processor.process_raw_data(sample_data, '20250604')

    if not processed_df.empty:
        print("Processing successful!")
        print(f"Processed {len(processed_df)} records")
        print("\nSample processed data:")
        print(processed_df[['timestamp', 'location_id', 'lmp_total', 'lmp_total_mean_hourly']].head())

        # Save the data
        saved_files = processor.save_to_storage(processed_df, '20250604')
        print(f"\nSaved to: {saved_files}")
    else:
        print("Processing failed!")