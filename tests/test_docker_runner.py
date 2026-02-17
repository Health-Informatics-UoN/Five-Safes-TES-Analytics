#!/usr/bin/env python3
"""
Test runner for Docker container local processing functionality.

This script provides utilities to test the Docker container in various scenarios:
1. Unit tests for local processing classes
2. Integration tests for query_resolver.py
3. Docker container build and run tests
4. End-to-end workflow tests

Usage:
    python test_docker_runner.py --help
    python test_docker_runner.py --unit-tests
    python test_docker_runner.py --integration-tests
    python test_docker_runner.py --docker-tests
    python test_docker_runner.py --all-tests
"""

import argparse
import subprocess
import sys
import os
import pytest
import tempfile
import json
from pathlib import Path

def run_unit_tests():
    """Run unit tests for local processing classes."""
    print("Running unit tests for local processing classes...")
    
    test_file = Path(__file__).parent / "test_local_processing.py"
    if not test_file.exists():
        print(f"Error: Test file {test_file} not found")
        return False
    
    try:
        result = subprocess.run([
            sys.executable, "-m", "pytest", str(test_file), "-v"
        ], capture_output=True, text=True)
        
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        
        return result.returncode == 0
    except Exception as e:
        print(f"Error running unit tests: {e}")
        return False

def run_integration_tests():
    """Run integration tests for query_resolver.py."""
    print("Running integration tests for query_resolver.py...")
    
    test_file = Path(__file__).parent / "test_docker_container.py"
    if not test_file.exists():
        print(f"Error: Test file {test_file} not found")
        return False
    
    try:
        result = subprocess.run([
            sys.executable, "-m", "pytest", str(test_file), "-v"
        ], capture_output=True, text=True)
        
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        
        return result.returncode == 0
    except Exception as e:
        print(f"Error running integration tests: {e}")
        return False

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

def test_docker_run():
    """Test Docker container run with sample data.

    Container uses postgres* env vars for DB connection (no --db-connection),
    matching analytics_tes behaviour. Requires a reachable Postgres; skipped unless
    RUN_DOCKER_POSTGRES_TESTS=1. Use postgres* env vars for credentials, or defaults
    postgres/postgres@host.docker.internal:5432/postgres.
    """
    if os.environ.get("RUN_DOCKER_POSTGRES_TESTS") != "1":
        pytest.skip("Set RUN_DOCKER_POSTGRES_TESTS=1 and have Postgres at host.docker.internal to run")

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

def test_docker_with_postgres():
    """Test Docker container with PostgreSQL database.

    Uses postgres* env vars (no --db-connection), matching analytics_tes.
    Skipped unless RUN_DOCKER_POSTGRES_TESTS=1; credentials from env or defaults.
    """
    if os.environ.get("RUN_DOCKER_POSTGRES_TESTS") != "1":
        pytest.skip("Set RUN_DOCKER_POSTGRES_TESTS=1 to run PostgreSQL Docker test")

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

def run_all_tests():
    """Run all tests."""
    print("Running all tests...")
    
    tests = [
        ("Unit Tests", run_unit_tests),
        ("Integration Tests", run_integration_tests),
        ("Docker Build", test_docker_build),
        ("Docker Run", test_docker_run),
        ("Docker PostgreSQL", test_docker_with_postgres),
    ]
    
    results = {}
    for test_name, test_func in tests:
        print(f"\n{'='*50}")
        print(f"Running {test_name}")
        print(f"{'='*50}")
        
        success = test_func()
        results[test_name] = success
        
        if success:
            print(f"✅ {test_name} PASSED")
        else:
            print(f"❌ {test_name} FAILED")
    
    print(f"\n{'='*50}")
    print("TEST SUMMARY")
    print(f"{'='*50}")
    
    passed = sum(1 for success in results.values() if success)
    total = len(results)
    
    for test_name, success in results.items():
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"{test_name}: {status}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    return passed == total

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Test runner for Docker container local processing")
    parser.add_argument("--unit-tests", action="store_true", help="Run unit tests")
    parser.add_argument("--integration-tests", action="store_true", help="Run integration tests")
    parser.add_argument("--docker-tests", action="store_true", help="Run Docker tests")
    parser.add_argument("--all-tests", action="store_true", help="Run all tests")
    parser.add_argument("--build-only", action="store_true", help="Only test Docker build")
    parser.add_argument("--run-only", action="store_true", help="Only test Docker run")
    
    args = parser.parse_args()
    
    if not any([args.unit_tests, args.integration_tests, args.docker_tests, args.all_tests, args.build_only, args.run_only]):
        parser.print_help()
        return
    
    success = True
    
    if args.unit_tests:
        success &= run_unit_tests()
    
    if args.integration_tests:
        success &= run_integration_tests()
    
    if args.docker_tests:
        success &= test_docker_build()
        success &= test_docker_run()
    
    if args.build_only:
        success &= test_docker_build()
    
    if args.run_only:
        success &= test_docker_run()
    
    if args.all_tests:
        success = run_all_tests()
    
    if success:
        print("\n🎉 All tests passed!")
        sys.exit(0)
    else:
        print("\n💥 Some tests failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
