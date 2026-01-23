# Five Safes TES Analytics

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
5STES_TOKEN=your_jwt_token_here
5STES_PROJECT=your_project_name

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
from analytics_tes import AnalyticsTES
from analyser import Analyser
from string import Template
import os


# Will use 5STES_PROJECT from environment and 5STES_TOKEN from environment
analytics_tes = AnalyticsTES()
engine = AnalysisEngine(tes_client=analytics_tes) 
analyser = Analyser(engine)
sql_schema = os.getenv("SQL_SCHEMA", "public")



# Define your own SQL query
query_template = Template("""WITH user_query AS (
  SELECT value_as_number FROM $schema.measurement 
  WHERE measurement_concept_id = 21490742
  AND value_as_number IS NOT NULL
)
SELECT
  COUNT(value_as_number) AS n,
  SUM(value_as_number) AS total
FROM user_query;""")

custom_query = query_template.safe_substitute(schema=sql_schema)

# Run the analysis
result = analyser.run_analysis(
    analysis_type="mean",
    task_name="DEMO: mean analysis test",
    user_query=custom_query,
    tres=["Nottingham","Nottingham 2"]
)

print(f"Analysis result: {result}")
```
