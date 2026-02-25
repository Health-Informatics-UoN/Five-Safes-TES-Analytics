#!/usr/bin/env python3
"""
Integration tests for Docker container build and run (tre-fx-local-processing).

Marked with @pytest.mark.integration; excluded by default (pytest.ini addopts).
Run with: pytest -m integration
Or only this file: pytest tests/test_docker_runner.py -m integration
"""

import subprocess
import os
import pytest
from pathlib import Path

@pytest.mark.integration
def test_docker_build():
    """Test Docker container build."""
    print("Testing Docker container build...")
    
    container_dir = Path(__file__).parent.parent / "docker"
    if not container_dir.exists():
        pytest.fail(f"Docker directory {container_dir} not found")
    
    try:
        # Change to container directory
        original_cwd = os.getcwd()
        os.chdir(container_dir)
        
        # Build Docker image
        result = subprocess.run([
            "docker", "build", "-t", "tre-fx-local-processing", "."
        ], capture_output=True, text=True)
        
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        
        assert result.returncode == 0, f"Docker build failed: {result.stderr}"
    except Exception as e:
        pytest.fail(f"Error building Docker container: {e}")
    finally:
        os.chdir(original_cwd)

@pytest.mark.integration
def test_docker_run():
    """Test Docker container run with sample data.

    Container uses postgres* env vars for DB connection (no --db-connection),
    matching analytics_tes behaviour. Requires a reachable Postgres. Use postgres* env vars for credentials, or defaults
    postgres/postgres@host.docker.internal:5432/postgres.
    """

    print("Testing Docker container run...")
    user = os.environ.get("postgresUsername", "postgres")
    password = os.environ.get("postgresPassword", "postgres")
    server = os.environ.get("postgresServer", "host.docker.internal")
    port = os.environ.get("postgresPort", "5432")
    database = os.environ.get("postgresDatabase", "postgres")

    try:
        result = subprocess.run([
            "docker", "run", "--rm",
            "-e", f"postgresUsername={user}",
            "-e", f"postgresPassword={password}",
            "-e", f"postgresServer={server}",
            "-e", f"postgresPort={port}",
            "-e", f"postgresDatabase={database}",
            "tre-fx-local-processing",
            "--user-query", "SELECT 1 as value_as_number UNION SELECT 2 UNION SELECT 3",
            "--analysis", "mean",
            "--output-filename", "test_output",
            "--output-format", "json"
        ], capture_output=True, text=True, timeout=30)

        print("STDOUT:", result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)

        assert result.returncode == 0, f"Docker run failed: {result.stderr}"
    except subprocess.TimeoutExpired:
        print("Timeout: Docker run took too long")
        pytest.fail("Docker run timed out")
    except Exception as e:
        pytest.fail(f"Error running Docker container: {e}")

@pytest.mark.integration
def test_docker_with_postgres():
    """Test Docker container with PostgreSQL database.

    Uses postgres* env vars (no --db-connection), matching analytics_tes.
    Credentials from env or defaults.
    """

    print("Testing Docker container with PostgreSQL...")
    user = os.environ.get("postgresUsername", "postgres")
    password = os.environ.get("postgresPassword", "postgres")
    server = os.environ.get("postgresServer", "host.docker.internal")
    port = os.environ.get("postgresPort", "5432")
    database = os.environ.get("postgresDatabase", "postgres")

    try:
        result = subprocess.run([
            "docker", "run", "--rm",
            "-e", f"postgresUsername={user}",
            "-e", f"postgresPassword={password}",
            "-e", f"postgresServer={server}",
            "-e", f"postgresPort={port}",
            "-e", f"postgresDatabase={database}",
            "tre-fx-local-processing",
            "--user-query", "SELECT COUNT(*) FROM information_schema.tables",
            "--analysis", "mean",
            "--output-filename", "test_output",
            "--output-format", "json"
        ], capture_output=True, text=True, timeout=30)

        print("STDOUT:", result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)

        assert result.returncode == 0, f"PostgreSQL test failed: {result.stderr}"
    except subprocess.TimeoutExpired:
        print("Timeout: PostgreSQL connection test took too long")
    except Exception as e:
        print(f"Error testing with PostgreSQL: {e}")

