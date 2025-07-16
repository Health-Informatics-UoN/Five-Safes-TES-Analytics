# TRE-FX Analytics

A federated analysis framework for Trusted Research Environments (TREs) using object-oriented design.

## Quick Start

### 1. Environment Setup

First, set up your environment variables:

```bash
# Copy the example environment file
cp env.example .env

# Edit .env with your actual values
nano .env  # or use your preferred editor
```

### 2. Required Environment Variables

All variables in `env.example` are **required**. Here's what you need to configure:

```bash
# Authentication
TRE_FX_TOKEN=your_jwt_token_here
TRE_FX_PROJECT=your_project_name

# TES (Task Execution Service) Configuration
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

### 3. Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Or using poetry
poetry install
```

### 4. Basic Usage

```python
from analysis_engine import AnalysisEngine

# Initialize the engine (uses environment variables automatically)
engine = AnalysisEngine("your_token", project="YourProject")

# Define your own SQL query
custom_query = """WITH user_query AS (
  SELECT value_as_number FROM public.measurement 
  WHERE measurement_concept_id = 3037532
  AND value_as_number IS NOT NULL
)
SELECT
  COUNT(value_as_number) AS n,
  SUM(value_as_number) AS total
FROM user_query;"""

# Run the analysis
result = engine.run_analysis(
    analysis_type="mean",
    query=custom_query,
    tres=["Nottingham", "Nottingham 2"]
)

print(f"Analysis result: {result}")
```