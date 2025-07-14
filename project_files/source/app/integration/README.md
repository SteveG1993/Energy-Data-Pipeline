# Energy Data Pipeline Integration

## Overview

An **Integration Class** is a software design pattern that serves as a bridge between different components or systems, orchestrating their interactions to create a cohesive workflow. Integration classes handle the complexity of connecting disparate systems, managing data flow, error handling, and ensuring that different components work together seamlessly.

## What is the EnergyDataPipelineIntegration Class?

The `EnergyDataPipelineIntegration` class is a comprehensive integration solution that connects the **APIDataCollector** with the **S3DataPersistence** system to create an automated energy data pipeline. This class orchestrates the complete workflow from API data collection to cloud storage.

### Core Responsibilities

1. **Data Flow Orchestration**: Manages the end-to-end process from API collection to S3 storage
2. **Configuration Management**: Loads and validates API configurations from `api_config.json`
3. **Error Handling**: Provides robust error handling and recovery mechanisms
4. **Folder Organization**: Automatically creates and manages S3 folder structures based on API configurations
5. **Statistics & Monitoring**: Tracks pipeline performance and success rates
6. **Flexible Execution**: Supports running all APIs, specific APIs, or custom selections

## Architecture

```
┌─────────────────┐    ┌──────────────────────────┐    ┌─────────────────┐
│   API Sources   │───▶│ EnergyDataPipelineIntegra │───▶│   S3 Storage    │
│                 │    │         tion Class        │    │                 │
│ • ISO-NE APIs   │    │                          │    │ • Organized     │
│ • Fuel Mix      │    │ ┌──────────────────────┐ │    │   Folders       │
│ • LMP Data      │    │ │   APIDataCollector   │ │    │ • Timestamped   │
│ • Forecasts     │    │ └──────────────────────┘ │    │   Files         │
└─────────────────┘    │ ┌──────────────────────┐ │    │ • Metadata      │
                       │ │  S3DataPersistence   │ │    │                 │
                       │ └──────────────────────┘ │    └─────────────────┘
                       └──────────────────────────┘
```

## Key Features

### 1. **Automatic Pipeline Execution**
```python
# Run complete pipeline for all configured APIs
pipeline = EnergyDataPipelineIntegration()
results = pipeline.run_full_pipeline()
```

### 2. **Selective API Processing**
```python
# Process specific APIs only
results = pipeline.run_selective_pipeline(['iso_ne_fuel_mix', 'Morning Report'])
```

### 3. **Intelligent Folder Organization**
Based on `api_config.json`, data is automatically organized:
```
s3-for-energy/
├── iso_ne_current_7_day_forecast/
│   └── demand_20250702_143025.csv
├── hourly_final_lmp/
│   └── hourly_final_lmp_20250702_143025.csv
├── iso_ne_fuel_mix/
│   └── fuel_mix_20250702_143025.csv
└── iso_ne_morning_report/
    └── morning_report_20250515_143025.csv
```

### 4. **Comprehensive Error Handling**
- API collection failures are logged and tracked
- S3 upload failures are handled gracefully
- Partial pipeline failures don't stop other APIs from processing
- Detailed error reporting for troubleshooting

### 5. **Performance Monitoring**
```python
{
    'pipeline_status': 'completed',
    'statistics': {
        'total_apis': 4,
        'successful_collections': 4,
        'successful_uploads': 3,
        'failed_collections': 0,
        'failed_uploads': 1
    },
    'summary': {
        'collection_success_rate': 1.0,
        'upload_success_rate': 0.75,
        'successful_end_to_end': 3
    }
}
```

## Configuration Integration

The integration class seamlessly uses your existing `api_config.json`:

```json
{
  "apis": [
    {
      "name": "7 day forecast all zones",
      "url": "https://webservices.iso-ne.com/api/v1.1/sevendayforecast/current",
      "output_folder": "iso_ne_current_7_day_forecast",  ← S3 folder
      "csv_prefix": "demand"                             ← File prefix
    }
  ]
}
```

## Usage Examples

### Basic Usage
```python
from app.integration.energy_pipeline_integration import EnergyDataPipelineIntegration

# Initialize with default settings
pipeline = EnergyDataPipelineIntegration()

# Run all configured APIs
results = pipeline.run_full_pipeline()

# Check results
if results['pipeline_status'] == 'completed':
    print(f"Successfully processed {results['summary']['successful_end_to_end']} APIs")
```

### Advanced Configuration
```python
# Custom initialization
pipeline = EnergyDataPipelineIntegration(
    config_file='custom_api_config.json',
    s3_bucket='my-energy-bucket',
    aws_profile='production',
    region='us-west-2'
)

# Validate before running
validation = pipeline.validate_configuration()
if validation['config_valid']:
    results = pipeline.run_full_pipeline()
```

### Single API Processing
```python
# Process just one API
result = pipeline.run_single_api_pipeline('iso_ne_fuel_mix')

if result['upload_status'] == 'success':
    print(f"Data saved to: {result['s3_key']}")
```

## Error Handling & Resilience

The integration class provides multiple layers of error handling:

1. **Configuration Validation**: Checks API config before execution
2. **Collection Error Handling**: Continues processing other APIs if one fails
3. **Upload Error Handling**: Logs failures but doesn't crash the pipeline
4. **Comprehensive Logging**: Detailed logs for debugging and monitoring

## Benefits of This Integration Class

### 1. **Simplified Operations**
- Single command executes the entire pipeline
- No manual coordination between collector and persistence layers
- Automatic error recovery and logging

### 2. **Scalability**
- Easy to add new APIs by updating configuration
- Supports partial processing for large API sets
- Efficient resource utilization

### 3. **Maintainability**
- Clear separation of concerns
- Centralized configuration management
- Comprehensive monitoring and debugging capabilities

### 4. **Reliability**
- Robust error handling prevents cascade failures
- Detailed statistics help identify issues
- Validation ensures configuration correctness

## Monitoring & Debugging

The integration class provides extensive monitoring capabilities:

```python
# Get current status
status = pipeline.get_pipeline_status()

# Validate configuration
validation = pipeline.validate_configuration()

# Check detailed results
for result in pipeline_results['results']:
    if result['upload_status'] == 'failed':
        print(f"Failed: {result['api_name']} - {result['error']}")
```

## Future Enhancements

The integration class is designed for extensibility:
- **Scheduling**: Add cron-like scheduling capabilities
- **Notifications**: Email/Slack alerts for failures
- **Data Transformation**: Add data processing steps before storage
- **Multiple Destinations**: Support additional storage backends
- **Metrics Export**: Integration with monitoring systems like Prometheus

## Getting Started

1. **Ensure your configuration is set up**:
   ```bash
   # Check your api_config.json
   cat app/api_config.json
   
   # Verify your .env file has S3 credentials
   cat .env
   ```

2. **Run the integration**:
   ```python
   from app.integration.energy_pipeline_integration import EnergyDataPipelineIntegration
   
   pipeline = EnergyDataPipelineIntegration()
   results = pipeline.run_full_pipeline()
   ```

3. **Check your S3 bucket**:
   ```bash
   aws s3 ls s3://s3-for-energy/ --recursive
   ```

This integration class transforms your energy data collection from a manual, multi-step process into a single, automated pipeline that reliably moves data from APIs to organized cloud storage.