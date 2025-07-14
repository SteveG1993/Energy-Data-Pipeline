# Energy Pipeline Architecture Diagram

## Class Structure

```mermaid
classDiagram
    class EnergyPipelineConfig {
        +str bucket_name
        +str region
        +str api_config_file
        +str default_region
        +str file_prefix
        +__init__()
        +validate() bool
    }
    
    class EnergyPipeline {
        +EnergyPipelineConfig config
        +APIDataCollector api_collector
        +S3DataPersistence persistence
        +__init__(config)
        +initialize_components()
        +process_api_result(api_result) Dict
        +run_single_api(api_name) Dict
        +run_all_apis() List[Dict]
        +run_energy_pipeline(region) Dict
    }
    
    class APIDataCollector {
        +run_single_api(name) Dict
        +run_all_apis() List[Dict]
    }
    
    class S3DataPersistence {
        +save_data(data, prefix, metadata) Dict
    }
    
    EnergyPipeline --> EnergyPipelineConfig : uses
    EnergyPipeline --> APIDataCollector : contains
    EnergyPipeline --> S3DataPersistence : contains
```

## Execution Flow

```mermaid
flowchart TD
    A[main()] --> B{Parse CLI Args}
    B --> C[Create EnergyPipelineConfig]
    C --> D[Initialize EnergyPipeline]
    D --> E{Execution Mode}
    
    E -->|--api| F[run_single_api()]
    E -->|--all| G[run_all_apis()]
    E -->|default| H[run_energy_pipeline()]
    
    F --> I[APIDataCollector.run_single_api()]
    G --> J[APIDataCollector.run_all_apis()]
    H --> J
    
    I --> K[process_api_result()]
    J --> L[Loop: process_api_result()]
    L --> K
    
    K --> M[S3DataPersistence.save_data()]
    M --> N[Log Results]
    N --> O[Return Summary]
```

## Data Flow

```mermaid
sequenceDiagram
    participant CLI as CLI Interface
    participant EP as EnergyPipeline
    participant AC as APIDataCollector
    participant S3 as S3DataPersistence
    participant AWS as AWS S3
    
    CLI->>EP: Initialize with config
    EP->>AC: Create APIDataCollector
    EP->>S3: Create S3DataPersistence
    
    CLI->>EP: run_energy_pipeline()
    EP->>AC: run_all_apis()
    AC-->>EP: List[api_results]
    
    loop For each API result
        EP->>EP: process_api_result()
        EP->>S3: save_data()
        S3->>AWS: Upload to S3 bucket
        AWS-->>S3: Upload confirmation
        S3-->>EP: Upload result
    end
    
    EP-->>CLI: Pipeline summary
```

## Configuration Management

```mermaid
graph LR
    A[Environment Variables] --> B[EnergyPipelineConfig]
    C[CLI Arguments] --> B
    D[Default Values] --> B
    
    B --> E[Pipeline Initialization]
    
    subgraph "Environment Variables"
        A1[S3_BUCKET_NAME]
        A2[AWS_DEFAULT_REGION]
        A3[API_CONFIG_FILE]
        A4[DEFAULT_ENERGY_REGION]
        A5[DATA_FILE_PREFIX]
    end
    
    subgraph "CLI Arguments"
        C1[--region]
        C2[--api]
        C3[--all]
        C4[--config]
    end
```

## Error Handling Strategy

```mermaid
flowchart TD
    A[Function Call] --> B{Try Block}
    B -->|Success| C[Process Result]
    B -->|Exception| D[Catch Exception]
    
    D --> E[Log Error]
    E --> F[Return Error Dict]
    F --> G[Continue Pipeline]
    
    C --> H[Validate Result]
    H -->|Valid| I[Continue Processing]
    H -->|Invalid| D
    
    I --> J[Log Success]
    J --> K[Return Success Dict]
```

## Component Dependencies

```mermaid
graph TB
    subgraph "External Dependencies"
        A[boto3 - AWS SDK]
        B[pandas - Data Processing]
        C[logging - Python Logging]
        D[argparse - CLI Parsing]
    end
    
    subgraph "Internal Modules"
        E[data_collection.collector]
        F[data_persisting.persistence]
    end
    
    subgraph "Main Pipeline"
        G[EnergyPipelineConfig]
        H[EnergyPipeline]
        I[main()]
    end
    
    A --> F
    B --> E
    B --> F
    C --> H
    D --> I
    E --> H
    F --> H
    G --> H
    H --> I
```

## Logging Architecture

```mermaid
graph LR
    A[Logger Configuration] --> B[StreamHandler]
    A --> C[FileHandler]
    
    B --> D[Console Output]
    C --> E[energy_pipeline.log]
    
    F[Pipeline Events] --> G[logger.info()]
    F --> H[logger.error()]
    F --> I[logger.warning()]
    
    G --> A
    H --> A
    I --> A
```