# src/utils/api_client.py
import requests
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class ISONEApiClient:
    """Client for interacting with ISO-NE Web API"""
    
    def __init__(self, base_url: str = "https://webservices.iso-ne.com/api/v1.1"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Energy-Data-Pipeline/1.0',
            'Accept': 'application/json'
        })
    
    def get_generation_mix(self, start_date: Optional[datetime] = None, 
                          end_date: Optional[datetime] = None) -> List[Dict]:
        """
        Retrieve generation mix data from ISO-NE API
        
        Args:
            start_date: Start date for data retrieval
            end_date: End date for data retrieval
            
        Returns:
            List of energy generation records
        """
        try:
            # Default to last 24 hours if no dates provided
            if not end_date:
                end_date = datetime.now()
            if not start_date:
                start_date = end_date - timedelta(days=1)
            
            # Format dates for API
            start_str = start_date.strftime('%Y%m%d')
            end_str = end_date.strftime('%Y%m%d')
            
            # API endpoint
            endpoint = f"{self.base_url}/genmix/current.json"
            
            # Add date parameters if needed
            params = {}
            if start_str != end_str:
                params.update({
                    'startdate': start_str,
                    'enddate': end_str
                })
            
            logger.info(f"Fetching data from {endpoint} with params: {params}")
            
            response = self.session.get(endpoint, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            return self._parse_generation_data(data)
            
        except requests.exceptions.Timeout:
            logger.error("API request timed out")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in API client: {str(e)}")
            raise
    
    def get_load_forecast(self) -> List[Dict]:
        """
        Retrieve load forecast data
        
        Returns:
            List of load forecast records
        """
        try:
            endpoint = f"{self.base_url}/hourlyloadforecast/current.json"
            
            response = self.session.get(endpoint, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            return self._parse_load_data(data)
            
        except Exception as e:
            logger.error(f"Error fetching load forecast: {str(e)}")
            raise
    
    def _parse_generation_data(self, data: Dict) -> List[Dict]:
        """Parse generation mix data from API response"""
        records = []
        
        try:
            if 'GenMixes' in data:
                for mix in data['GenMixes']:
                    if 'GenMixs' in mix:
                        for gen_data in mix['GenMixs']:
                            record = {
                                'DateTime': gen_data.get('BeginDate', ''),
                                'kWh': float(gen_data.get('GenMw', 0)) * 1000,  # MW to kW
                                'FuelType': gen_data.get('FuelCategory', 'Unknown'),
                                'Source': 'generation_mix'
                            }
                            records.append(record)
            
            logger.info(f"Parsed {len(records)} generation records")
            return records
            
        except Exception as e:
            logger.error(f"Error parsing generation data: {str(e)}")
            raise
    
    def _parse_load_data(self, data: Dict) -> List[Dict]:
        """Parse load forecast data from API response"""
        records = []
        
        try:
            if 'HourlyLoadForecasts' in data:
                for forecast in data['HourlyLoadForecasts']:
                    if 'HourlyLoadForecast' in forecast:
                        for load_data in forecast['HourlyLoadForecast']:
                            record = {
                                'DateTime': load_data.get('BeginDate', ''),
                                'kWh': float(load_data.get('LoadMw', 0)) * 1000,  # MW to kW
                                'FuelType': 'Load_Forecast',
                                'Source': 'load_forecast'
                            }
                            records.append(record)
            
            logger.info(f"Parsed {len(records)} load forecast records")
            return records
            
        except Exception as e:
            logger.error(f"Error parsing load data: {str(e)}")
            raise

---
# src/utils/data_processor.py
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)

class EnergyDataProcessor:
    """Data processor for energy data transformations"""
    
    def __init__(self):
        self.processed_count = 0
        self.error_count = 0
    
    def process_energy_data(self, raw_data: List[Dict]) -> List[Dict]:
        """
        Main processing function for energy data
        
        Args:
            raw_data: List of raw energy records
            
        Returns:
            List of processed energy records
        """
        try:
            if not raw_data:
                logger.warning("No data provided for processing")
                return []
            
            # Convert to DataFrame for easier processing
            df = pd.DataFrame(raw_data)
            
            # Apply transformations
            df = self._clean_datetime(df)
            df = self._round_kwh(df)
            df = self._remove_duplicates(df)
            df = self._add_derived_fields(df)
            df = self._validate_data(df)
            df = self._sort_data(df)
            
            # Convert back to list of dictionaries
            processed_data = df.to_dict('records')
            
            self.processed_count = len(processed_data)
            logger.info(f"Successfully processed {self.processed_count} records")
            
            return processed_data
            
        except Exception as e:
            logger.error(f"Error processing energy data: {str(e)}")
            self.error_count += 1
            raise
    
    def _clean_datetime(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize datetime fields"""
        try:
            # Convert DateTime to pandas datetime
            df['DateTime'] = pd.to_datetime(df['DateTime'], errors='coerce')
            
            # Remove records with invalid dates
            before_count = len(df)
            df = df.dropna(subset=['DateTime'])
            after_count = len(df)
            
            if before_count != after_count:
                logger.warning(f"Removed {before_count - after_count} records with invalid dates")
            
            return df
            
        except Exception as e:
            logger.error(f"Error cleaning datetime: {str(e)}")
            raise
    
    def _round_kwh(self, df: pd.DataFrame) -> pd.DataFrame:
        """Round kWh values to tenths place"""
        try:
            # Convert kWh to numeric, handling any non-numeric values
            df['kWh'] = pd.to_numeric(df['kWh'], errors='coerce')
            
            # Round to 1 decimal place
            df['kWh'] = df['kWh'].round(1)
            
            # Remove records with invalid kWh values
            before_count = len(df)
            df = df.dropna(subset=['kWh'])
            after_count = len(df)
            
            if before_count != after_count:
                logger.warning(f"Removed {before_count - after_count} records with invalid kWh values")
            
            return df
            
        except Exception as e:
            logger.error(f"Error rounding kWh values: {str(e)}")
            raise
    
    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate records"""
        try:
            before_count = len(df)
            
            # Remove duplicates based on DateTime and kWh
            df = df.drop_duplicates(subset=['DateTime', 'kWh'])
            
            after_count = len(df)
            
            if before_count != after_count:
                logger.info(f"Removed {before_count - after_count} duplicate records")
            
            return df
            
        except Exception as e:
            logger.error(f"Error removing duplicates: {str(e)}")
            raise
    
    def _add_derived_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived fields for analysis"""
        try:
            # Add date components
            df['Date'] = df['DateTime'].dt.date
            df['Hour'] = df['DateTime'].dt.hour
            df['DayOfWeek'] = df['DateTime'].dt.dayofweek
            df['Month'] = df['DateTime'].dt.month
            df['Year'] = df['DateTime'].dt.year
            
            # Add time period classification
            df['TimePeriod'] = df['Hour'].apply(self._classify_time_period)
            
            # Add data quality indicators
            df['IsWeekend'] = df['DayOfWeek'].isin([5, 6])
            df['ProcessedAt'] = datetime.now()
            
            return df
            
        except Exception as e:
            logger.error(f"Error adding derived fields: {str(e)}")
            raise
    
    def _classify_time_period(self, hour: int) -> str:
        """Classify hour into time periods"""
        if 6 <= hour < 18:
            return 'Day'
        elif 18 <= hour < 22:
            return 'Evening'
        else:
            return 'Night'
    
    def _validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate data quality and remove outliers"""
        try