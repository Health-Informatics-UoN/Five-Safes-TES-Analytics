# TRE-FX Analytics API Reference

## Quick Start

```python
from analysis_orchestrator import AnalysisOrchestrator

# Initialize
orchestrator = AnalysisOrchestrator(token="your_token", project="YourProject")

# Run analysis
result = orchestrator.run_mean_analysis(concept_id=21490742, tres=["Nottingham"])
print(f"Result: {result['result']}")
```

## Configuration

The system uses environment variables for configuration. See `env.example` for all available options:

```bash
# Required environment variables:
5STES_TOKEN=your_jwt_token_here
5STES_PROJECT=your_project_name
TES_BASE_URL=http://your-tes-endpoint:5034/v1/tasks
TES_DOCKER_IMAGE=harbor.your-registry.com/your-image:tag

# Database Configuration
DB_HOST=your-database-host
DB_PORT=5432
DB_USERNAME=your-database-username
DB_PASSWORD=your-database-password
DB_NAME=your-database-name

# MinIO Configuration
MINIO_STS_ENDPOINT=http://your-minio-endpoint:9000/sts
MINIO_ENDPOINT=your-minio-endpoint:9000
MINIO_OUTPUT_BUCKET=your-output-bucket-name
```

## AnalysisOrchestrator

Main orchestrator class for federated analysis workflow.

### Constructor
```python
AnalysisOrchestrator(token: str, project: str = None)
```

**Parameters:**
- `token`: Authentication token for TRE-FX services (required)
- `project`: Project name for TES tasks (defaults to 5STES_PROJECT environment variable)

### Methods

#### `run_analysis(analysis_type, query, tres, task_name=None, bucket=None)`
Run a complete federated analysis workflow.

**Parameters:**
- `analysis_type` (str): Type of analysis ("mean", "variance", "PMCC", "chi_squared_scipy")
- `query` (str): SQL query to execute
- `tres` (List[str]): List of TREs to run analysis on
- `task_name` (str, optional): Name for the TES task (defaults to "analysis {analysis_type}")
- `bucket` (str, optional): MinIO bucket for outputs (defaults to MINIO_OUTPUT_BUCKET environment variable)

**Returns:** Dict with analysis results

#### `run_mean_analysis(concept_id, tres)`
Run mean analysis for a specific concept.

#### `run_variance_analysis(concept_id, tres)`
Run variance analysis for a specific concept.

#### `run_pmcc_analysis(x_concept_id, y_concept_id, tres)`
Run Pearson's correlation analysis between two concepts.

#### `run_chi_squared_analysis(tres)`
Run chi-squared analysis for gender vs race contingency table.

## QueryBuilder

Build SQL queries for different analysis types.

### Methods

#### `build_mean_query(concept_id: int) -> str`
Build query for mean calculation.

#### `build_variance_query(concept_id: int) -> str`
Build query for variance calculation.

#### `build_pmcc_query(x_concept_id: int, y_concept_id: int) -> str`
Build query for Pearson's correlation coefficient.

#### `build_contingency_query(race_concept_ids: Optional[List[int]] = None) -> str`
Build query for contingency table generation.

#### `build_custom_query(table, columns, conditions, group_by, order_by, limit) -> str`
Build a custom SQL query.

#### `validate_query(query: str) -> bool`
Validate SQL query for safety.

## StatisticalAnalyzer

Perform statistical calculations and analysis.

### Methods

#### `analyze_data(input_data, analysis_type: str)`
Analyze data using specified analysis type.

#### `get_supported_analysis_types() -> List[str]`
Get list of supported analysis types.

#### `calculate_descriptive_statistics(data: np.ndarray) -> Dict[str, float]`
Calculate descriptive statistics.

#### `perform_hypothesis_test(data1, data2, test_type="t_test") -> Dict[str, Any]`
Perform hypothesis testing between two datasets.

#### `calculate_confidence_interval(data, confidence=0.95) -> Dict[str, float]`
Calculate confidence interval for the mean.

## DataProcessor

Handle data processing, aggregation, and file operations.

### Methods

#### `import_data(input_data: Union[str, List[str]]) -> np.ndarray`
Import data from CSV string or list of strings.

#### `combine_contingency_tables(contingency_tables: List[str]) -> Dict[str, Any]`
Combine multiple contingency tables.

#### `aggregate_data(inputs: List[str], analysis_type: str)`
Aggregate data based on analysis type.

#### `validate_data(data: Any, analysis_type: str) -> bool`
Validate data for a given analysis type.

#### `dict_to_array(contingency_dict: Dict[str, Any]) -> np.ndarray`
Convert contingency table dictionary to numpy array.

## TESClient

Handle TES (Task Execution Service) operations.

### Constructor
```python
TESClient(base_url=None, 
          default_image=None,
          default_db_config=None,
          default_db_port=None)
```

**Parameters:**
- `base_url`: TES API endpoint (defaults to `TES_BASE_URL` environment variable)
- `default_image`: Docker image (defaults to `TES_DOCKER_IMAGE` environment variable)
- `default_db_config`: Database configuration dict (defaults to environment variables)
- `default_db_port`: Database port (defaults to `DB_PORT` environment variable)

### Methods

#### `generate_tes_task(query, name, image, db_config, output_path) -> Dict[str, Any]`
Generate a TES task JSON configuration.

#### `submit_task(template: Dict[str, Any], token: str) -> Dict[str, Any]`
Submit a TES task using the requests library.

#### `get_task_status(task_id: str, token: str) -> Dict[str, Any]`
Get the status of a submitted task.

#### `list_tasks(token: str, limit: int = 100) -> List[Dict[str, Any]]`
List recent tasks.

## MinIOClient

Handle MinIO operations and token management.

### Constructor
```python
MinIOClient(token: str, 
           sts_endpoint=None,
           minio_endpoint=None)
```

**Parameters:**
- `token`: OIDC token for authentication
- `sts_endpoint`: STS endpoint URL (defaults to `MINIO_STS_ENDPOINT` environment variable)
- `minio_endpoint`: MinIO endpoint URL (defaults to `MINIO_ENDPOINT` environment variable)

### Methods

#### `get_object(bucket: str, object_path: str) -> Optional[str]`
Get object content from MinIO.

#### `list_objects(bucket: str, prefix: str = "") -> List[str]`
List objects in a bucket.

#### `wait_for_object(bucket: str, object_path: str, timeout: int = 300) -> Optional[str]`
Wait for an object to appear and return its content.

#### `refresh_credentials()`
Force refresh of credentials.

## Usage Examples

### Basic Mean Analysis
```python
from analysis_orchestrator import AnalysisOrchestrator

orchestrator = AnalysisOrchestrator("your_token")
result = orchestrator.run_mean_analysis(21490742, ["Nottingham"])
print(f"Mean: {result['result']}")
```

### Custom Query Analysis
```python
from query_builder import QueryBuilder
from analysis_orchestrator import AnalysisOrchestrator

qb = QueryBuilder()
custom_query = qb.build_summary_stats_query(concept_id=21490742)

orchestrator = AnalysisOrchestrator("your_token")
result = orchestrator.run_analysis(
    analysis_type="mean",
    query=custom_query,
    tres=["Nottingham"],
    task_name="Custom Analysis"
)
```

### Using Individual Components
```python
from statistical_analyzer import StatisticalAnalyzer
from data_processor import DataProcessor

# Statistical analysis
analyzer = StatisticalAnalyzer()
supported_types = analyzer.get_supported_analysis_types()
print(f"Supported types: {supported_types}")

# Data processing
processor = DataProcessor()
sample_data = ["2,117", "3,150"]
processed_data = [processor.import_data(data) for data in sample_data]
```

### Environment Variable Configuration
```python
import os
from tes_client import TESClient

# Configure via environment variables
os.environ['TES_BASE_URL'] = 'http://your-tes-endpoint:5034/v1/tasks'
os.environ['TES_DOCKER_IMAGE'] = 'harbor.your-registry.com/your-image:tag'
os.environ['DB_HOST'] = 'your-database-host'
os.environ['DB_PORT'] = '5432'
os.environ['DB_USERNAME'] = 'your-database-username'
os.environ['DB_PASSWORD'] = 'your-database-password'
os.environ['DB_NAME'] = 'your-database-name'
os.environ['MINIO_STS_ENDPOINT'] = 'http://your-minio-endpoint:9000/sts'
os.environ['MINIO_ENDPOINT'] = 'your-minio-endpoint:9000'
os.environ['MINIO_OUTPUT_BUCKET'] = 'your-output-bucket-name'

# Client will use environment variables automatically
client = TESClient()
```

## Error Handling

### Common Exceptions

- `TokenExpiredError`: Token has expired
- `requests.exceptions.RequestException`: Network or API errors
- `ValueError`: Invalid parameters or data
- `KeyError`: Missing required data

### Example Error Handling
```python
from minio_client import TokenExpiredError

try:
    result = engine.run_analysis("mean", query, tres)
except TokenExpiredError:
    print("Token expired, please refresh")
except requests.exceptions.RequestException as e:
    print(f"Network error: {e}")
except ValueError as e:
    print(f"Invalid data: {e}")
```

## Supported Analysis Types

- `"mean"`