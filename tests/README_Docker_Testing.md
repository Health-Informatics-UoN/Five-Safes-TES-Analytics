# Docker Container Testing

This directory contains comprehensive tests for the TRE-FX local processing Docker container functionality.

## Test Files

### 1. `test_local_processing.py`
**Unit tests for local processing classes**
- Tests for `BaseLocalProcessing`, `Mean`, `Variance`, `PMCC`, `ContingencyTable`, `PercentileSketch`
- Tests for query building, SQL generation, and Python analysis
- Tests for error handling and edge cases
- Tests for the processing class registry

### 2. `test_docker_container.py`
**Integration tests for Docker container functionality**
- Tests for `query_resolver.py` entry point
- Tests for JSON serialization with `DecimalEncoder`
- Tests for different analysis types in container context
- Tests for Docker environment variables and command line arguments
- Tests for Dockerfile structure and dependencies

### 3. `test_docker_runner.py`
**Test runner script for Docker container testing**
- Automated test execution
- Docker build and run testing
- Integration with different database types
- Comprehensive test reporting

## Running Tests

### Run All Tests
```bash
python tests/test_docker_runner.py --all-tests
```

### Run Specific Test Categories
```bash
# Unit tests only
python tests/test_docker_runner.py --unit-tests

# Integration tests only
python tests/test_docker_runner.py --integration-tests

# Docker tests only
python tests/test_docker_runner.py --docker-tests
```

### Run Individual Test Files
```bash
# Unit tests
pytest tests/test_local_processing.py -v

# Integration tests
pytest tests/test_docker_container.py -v
```

### Docker-Specific Testing
```bash
# Test Docker build only
python tests/test_docker_runner.py --build-only

# Test Docker run only
python tests/test_docker_runner.py --run-only
```

## Test Coverage

### Local Processing Classes
- ✅ **BaseLocalProcessing**: Abstract base class functionality
- ✅ **Mean**: Mean calculation with SQL aggregation
- ✅ **Variance**: Variance calculation with SQL aggregation
- ✅ **PMCC**: Pearson's correlation coefficient calculation
- ✅ **ContingencyTable**: Contingency table generation with dynamic column detection
- ✅ **PercentileSketch**: TDigest-based percentile calculation

### Docker Container Functionality
- ✅ **Query Resolver**: Command-line interface and entry point
- ✅ **Database Integration**: SQLAlchemy engine and connection handling
- ✅ **Output Formats**: JSON and CSV output generation
- ✅ **Error Handling**: Database errors, unsupported analysis types
- ✅ **Environment Variables**: Docker environment variable support

### Integration Scenarios
- ✅ **Mean Analysis**: Complete workflow from SQL to JSON output
- ✅ **Contingency Table**: Dynamic column detection and aggregation
- ✅ **Percentile Sketch**: TDigest processing and JSON serialization
- ✅ **Error Scenarios**: Database connection failures, invalid queries

## Docker Container Testing

### Prerequisites
- Docker installed and running
- Python 3.12+ for local testing
- Required Python packages: `pytest`, `sqlalchemy`, `numpy`, `tdigest`

### Building the Container
```bash
cd Container
docker build -t tre-fx-local-processing .
```

### Running the Container
```bash
# Basic usage
docker run --rm tre-fx-local-processing \
  --user-query "SELECT value_as_number FROM measurements" \
  --analysis mean \
  --db-connection "postgresql://user:pass@host:5432/db" \
  --output-filename result \
  --output-format json

# With environment variables
docker run --rm -e DB_CONNECTION="postgresql://..." tre-fx-local-processing
```

### Test Scenarios

#### 1. Mean Analysis
```bash
docker run --rm tre-fx-local-processing \
  --user-query "SELECT 1 as value_as_number UNION SELECT 2 UNION SELECT 3" \
  --analysis mean \
  --db-connection "sqlite:///:memory:"
```

#### 2. Contingency Table
```bash
docker run --rm tre-fx-local-processing \
  --user-query "SELECT gender, race FROM patients" \
  --analysis contingency_table \
  --db-connection "postgresql://user:pass@host:5432/db"
```

#### 3. Percentile Sketch
```bash
docker run --rm tre-fx-local-processing \
  --user-query "SELECT value_as_number FROM measurements" \
  --analysis percentile_sketch \
  --db-connection "postgresql://user:pass@host:5432/db"
```

## Expected Outputs

### Mean Analysis Output
```json
{
  "n": 100,
  "total": 1500.5
}
```

### Contingency Table Output
```json
[
  {"gender": "Male", "race": "White", "n": 45},
  {"gender": "Male", "race": "Black", "n": 23},
  {"gender": "Female", "race": "White", "n": 52},
  {"gender": "Female", "race": "Black", "n": 28}
]
```

### Percentile Sketch Output
```json
{
  "centroids": [...],
  "count": 100
}
```

## Troubleshooting

### Common Issues

1. **Database Connection Errors**
   - Verify database connection string
   - Check network connectivity
   - Ensure database is running

2. **Analysis Type Errors**
   - Verify analysis type is supported
   - Check spelling and case sensitivity

3. **Query Errors**
   - Verify SQL syntax
   - Check column names and table names
   - Ensure proper permissions

4. **Docker Build Errors**
   - Check Dockerfile syntax
   - Verify all required files are present
   - Check Python package dependencies

### Debug Mode
Add `--debug` flag to see detailed query execution:
```bash
docker run --rm tre-fx-local-processing \
  --user-query "SELECT * FROM users" \
  --analysis mean \
  --db-connection "postgresql://..." \
  --debug
```

## Contributing

When adding new analysis types or modifying existing ones:

1. Add unit tests to `test_local_processing.py`
2. Add integration tests to `test_docker_container.py`
3. Update this README with new test scenarios
4. Run the full test suite to ensure compatibility

## Test Data

The tests use mock data and in-memory SQLite databases to avoid requiring external database connections. For integration testing with real databases, modify the test configuration to use your database connection strings.
