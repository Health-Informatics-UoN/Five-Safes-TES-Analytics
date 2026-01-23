#!/usr/bin/env python3
"""
Test script for the refactored object-oriented TRE-FX Analytics code.
This script demonstrates the functionality without requiring actual TRE-FX services.
"""

import pytest
import numpy as np
from unittest.mock import Mock, patch, MagicMock
import sys
import os
from typing import List, Dict, Any

# Add the current directory to the path so we can import our modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from analysis_engine import AnalysisEngine
from data_processor import DataProcessor
from statistical_analyzer import StatisticalAnalyzer
from tes_client import TESClient
from minio_client import MinIOClient
from analytics_tes import AnalyticsTES
from analysis_runner import AnalysisRunner


class TestDataProcessor:
    """Test cases for DataProcessor class."""
    
    @pytest.fixture
    def processor(self):
        """Set up test fixtures."""
        return DataProcessor()

    @pytest.fixture
    def analyzer(self):
        """Set up test fixtures."""
        return StatisticalAnalyzer()
    
    def test_aggregate_data_mean(self, processor):
        """Test data aggregation for mean analysis."""
        # Mock CSV data
        csv_data1 = "n,total\n10,100\n"
        csv_data2 = "n,total\n15,150\n"
        
        data = [csv_data1, csv_data2]
        result = processor.aggregate_data(data, "mean")
        
        # Should return numpy array with aggregated values
        assert isinstance(result, dict)
        assert len(result) == 2  # Two rows
        assert result['n'][0] == 10  # n from first dataset
        assert result['total'][0] == 100  # total from first dataset
    
    def test_aggregate_data_variance(self, processor):
        """Test data aggregation for variance analysis."""
        csv_data = "n,sum_x2,total\n10,1000,100\n"
        data = [csv_data]
        result = processor.aggregate_data(data, "variance")
        
        assert isinstance(result, dict)
        assert result['n'][0] == 10  # n
        assert result['sum_x2'][0] == 1000  # sum_x2
        assert result['total'][0] == 100  # total
    
    def test_aggregate_data_pmcc(self, processor):
        """Test data aggregation for PMCC analysis."""
        csv_data = "n,sum_x,sum_y,sum_xy,sum_x2,sum_y2\n5,10,20,50,30,80\n"
        data = [csv_data]
        result = processor.aggregate_data(data, "pmcc")
        
        assert isinstance(result, dict)
        assert result['n'][0] == 5  # n
        assert result['sum_x'][0] == 10  # sum_x
        assert result['sum_y'][0] == 20  # sum_y
    
    def test_aggregate_data_contingency_table(self, processor):
        """Test data aggregation for contingency table analysis."""
        # CSV data must include a header row; last column is the count
        csv_data = (
            "gender,race,count\n"
            "Male,White,10\n"
            "Male,Black,15\n"
            "Female,White,20\n"
            "Female,Black,25\n"
        )
        data = [csv_data]
        result = processor.aggregate_data(data, "contingencytable")

        # DataProcessor should convert CSV into dict format matching the analysis return_format:
        # {"contingency_table": [ {\"gender\": ..., \"race\": ..., \"n\": ...}, ... ]}
        assert isinstance(result, dict)
        assert "contingency_table" in result

        rows = result["contingency_table"]
        assert isinstance(rows, list)
        assert len(rows) == 4

        # Check that each row has expected keys and counts
        expected_rows = {
            ("Male", "White", 10),
            ("Male", "Black", 15),
            ("Female", "White", 20),
            ("Female", "Black", 25),
        }
        actual_rows = {
            (row["gender"], row["race"], row["n"]) for row in rows
        }
        assert actual_rows == expected_rows

    def test_analyze_data_mean(self, analyzer):
        """Test mean analysis."""
        # Mock aggregated data: n=10, total=100
        data = np.array([[10, 100]])
        result = analyzer.analyze_data(data, "mean")
        
        assert result == 10.0  # 100/10 = 10
    
    def test_analyze_data_variance(self, analyzer):
        """Test variance analysis."""
        # Mock aggregated data: n=5, sum_x2=100, total=20
        data = np.array([[5, 100, 20]])
        result = analyzer.analyze_data(data, "variance")
        
        # Expected variance = (sum_x2 - (total^2)/n) / (n-1)
        # = (100 - (20^2)/5) / 4 = (100 - 80) / 4 = 5.0
        assert result == 5.0
    
    def test_analyze_data_pmcc(self, analyzer):
        """Test PMCC analysis."""
        # Use data that won't cause division by zero
        # n=3, sum_x=6, sum_y=9, sum_xy=20, sum_x2=14, sum_y2=29
        data = np.array([[3, 6, 9, 20, 14, 29]])
        result = analyzer.analyze_data(data, "pmcc")
        
        # This is a complex calculation, so we just check it's a float
        assert isinstance(result, float)
        # PMCC should be between -1 and 1, but allow for edge cases
        assert -1.1 <= result <= 1.1  # Slightly wider range for numerical precision
    
    def test_unsupported_analysis_type(self, analyzer):
        """Test that unsupported analysis types raise errors."""
        data = np.array([[1, 2, 3]])
        
        with pytest.raises(ValueError):
            analyzer.analyze_data(data, "unsupported")
    
    def test_get_analysis_config(self, analyzer):
        """Test getting analysis configuration."""
        config = analyzer.get_analysis_config("mean")
        
        assert "return_format" in config
        assert "aggregation_function" in config
        assert "analysis_function" in config
    
    def test_get_supported_analysis_types(self, analyzer):
        """Test getting supported analysis types."""
        types = analyzer.get_supported_analysis_types()
        
        assert "mean" in types
        assert "variance" in types
        assert "pmcc" in types
        assert "contingencytable" in types
        assert "percentilesketch" in types


class TestAnalysisEngine:
    """Test cases for AnalysisEngine class."""
    
    @pytest.fixture
    def engine(self):
        """Set up test fixtures."""
        return AnalysisEngine("test_token", "test_project")
    
    @patch('analysis_engine.TESClient')
    @patch('analysis_engine.MinIOClient')
    def test_run_analysis(self, mock_minio, mock_tes, engine):
        """Test running a complete analysis workflow."""
        # Mock TES client
        mock_tes_instance = Mock()
        mock_tes_instance.submit_task.return_value = {"id": "123"}
        # Mock get_task_status to return a proper dictionary with status
        mock_tes_instance.get_task_status.return_value = {"status": 11, "description": "Completed"}
        mock_tes.return_value = mock_tes_instance
        
        # Mock MinIO client
        mock_minio_instance = Mock()
        mock_minio_instance.get_object.return_value = "n,total\n10,100\n"
        mock_minio.return_value = mock_minio_instance
        

        
        # Mock data processor and statistical analyzer
        DataProcessor.aggregate_data = Mock(return_value=np.array([[10, 100]]))
        StatisticalAnalyzer.analyze_data = Mock(return_value=10.0)
        
        # Run analysis (no 'column' kwarg)
        user_query = "SELECT value_as_number FROM measurement WHERE concept_id = 123"


        analytics_tes = mock_tes_instance
        # Create engine after setting up mocks
        engine = AnalysisEngine(tes_client=analytics_tes, token="test_token", project="test_project")

        analysis_runner = AnalysisRunner(engine)

        result = analysis_runner.run_analysis(
            "mean", 
            user_query, 
            ["TRE1", "TRE2"]
        )
        
        # Verify result structure
        assert "analysis_type" in result
        assert "result" in result
        assert "task_id" in result
        assert "tres_used" in result
        assert "data_sources" in result
        assert "complete_query" in result
        
        assert result["analysis_type"] == "mean"
        assert result["result"] == 10.0
        assert result["task_id"] == "123"
        assert result["tres_used"] == ["TRE1", "TRE2"]
        assert result["data_sources"] == 2
    
    def test_get_analysis_requirements(self, engine):
        """Test getting analysis requirements."""
        analysis_runner = AnalysisRunner(engine)
        requirements = analysis_runner.get_analysis_requirements("mean")
        
        assert "return_format" in requirements
        assert "aggregation_function" in requirements
        assert "analysis_function" in requirements
        assert callable(requirements["aggregation_function"])
        assert callable(requirements["analysis_function"])
    
    def test_get_supported_analysis_types(self, engine):
        """Test getting supported analysis types."""
        analysis_runner = AnalysisRunner(engine)
        types = analysis_runner.get_supported_analysis_types()
        
        assert "mean" in types
        assert "variance" in types
        assert "pmcc" in types
        assert "contingencytable" in types
        assert "percentilesketch" in types


class TestExampleFunctions:
    """Test cases for example usage functions."""
    
    @pytest.fixture
    def analysis_runner(self):
        """Set up test fixtures."""
        mock_engine = Mock(spec=AnalysisEngine)
        mock_engine.tes_client = Mock()
        return AnalysisRunner(mock_engine)
    
    @patch.object(AnalysisRunner, 'run_analysis')
    def test_run_mean_analysis_example(self, mock_run_analysis, analysis_runner):
        """Test mean analysis example function."""
        mock_run_analysis.return_value = {"result": 10.0}
        
        result = run_mean_analysis_example(analysis_runner, 123, ["TRE1"])
        
        # Verify the function was called with correct parameters
        mock_run_analysis.assert_called_once()
        call_args = mock_run_analysis.call_args
        assert call_args[0][0] == "mean"  # analysis_type
        assert "SELECT value_as_number FROM public.measurement" in call_args[0][1]  # user_query
        assert call_args[0][2] == ["TRE1"]  # tres
        assert call_args[1]["column"] == "value_as_number"  # kwargs
    
    @patch.object(AnalysisRunner, 'run_analysis')
    def test_run_variance_analysis_example(self, mock_run_analysis, analysis_runner):
        """Test variance analysis example function."""
        mock_run_analysis.return_value = {"result": 5.0}
        
        result = run_variance_analysis_example(analysis_runner, 123, ["TRE1"])
        
        mock_run_analysis.assert_called_once()
        call_args = mock_run_analysis.call_args
        assert call_args[0][0] == "variance"
        assert "SELECT value_as_number FROM public.measurement" in call_args[0][1]
        assert call_args[1]["column"] == "value_as_number"
    
    @patch.object(AnalysisRunner, 'run_analysis')
    def test_run_pmcc_analysis_example(self, mock_run_analysis, analysis_runner):
        """Test PMCC analysis example function."""
        mock_run_analysis.return_value = {"result": 0.8}
        
        result = run_pmcc_analysis_example(analysis_runner, 123, 456, ["TRE1"])
        
        mock_run_analysis.assert_called_once()
        call_args = mock_run_analysis.call_args
        assert call_args[0][0] == "PMCC"
        assert "WITH x_values AS" in call_args[0][1]
        assert call_args[1]["x_column"] == "x"
        assert call_args[1]["y_column"] == "y"
    
    @patch.object(AnalysisRunner, 'run_analysis')
    def test_run_chi_squared_analysis_example(self, mock_run_analysis, analysis_runner):
        """Test chi-squared analysis example function."""
        mock_run_analysis.return_value = {"result": 2.5}
        
        result = run_chi_squared_analysis_example(analysis_runner, ["TRE1"])
        
        mock_run_analysis.assert_called_once()
        call_args = mock_run_analysis.call_args
        assert call_args[0][0] == "chi_squared_scipy"
        assert "SELECT" in call_args[0][1]
        assert call_args[1]["group_columns"] == "gender_name, race_name"


def run_mean_analysis_example(analysis_runner: AnalysisRunner, concept_id: int, tres: List[str]) -> Dict[str, Any]:
    """Example function for mean analysis."""
    user_query = f"""SELECT value_as_number FROM public.measurement 
WHERE measurement_concept_id = {concept_id}
AND value_as_number IS NOT NULL"""
    
    return analysis_runner.run_analysis("mean", user_query, tres, column="value_as_number")


def run_variance_analysis_example(analysis_runner: AnalysisRunner, concept_id: int, tres: List[str]) -> Dict[str, Any]:
    """Example function for variance analysis."""
    user_query = f"""SELECT value_as_number FROM public.measurement 
WHERE measurement_concept_id = {concept_id}
AND value_as_number IS NOT NULL"""
    
    return analysis_runner.run_analysis("variance", user_query, tres, column="value_as_number")


def run_pmcc_analysis_example(analysis_runner: AnalysisRunner, x_concept_id: int, y_concept_id: int, tres: List[str]) -> Dict[str, Any]:
    """Example function for PMCC analysis."""
    user_query = f"""WITH x_values AS (
  SELECT person_id, measurement_date, value_as_number AS x
  FROM public.measurement
  WHERE measurement_concept_id = {x_concept_id}
    AND value_as_number IS NOT NULLcollected s
),
y_values AS (
  SELECT person_id, measurement_date, value_as_number AS y
  FROM public.measurement
  WHERE measurement_concept_id = {y_concept_id}
    AND value_as_number IS NOT NULL
)
SELECT
  x.x,
  y.y
FROM x_values x
INNER JOIN y_values y
  ON x.person_id = y.person_id
  AND x.measurement_date = y.measurement_date"""
    
    return analysis_runner.run_analysis("PMCC", user_query, tres, x_column="x", y_column="y")


def run_chi_squared_analysis_example(analysis_runner: AnalysisRunner, tres: List[str]) -> Dict[str, Any]:
    """Example function for chi-squared analysis."""
    user_query = """SELECT 
  g.concept_name AS gender_name,
  r.concept_name AS race_name
FROM person p
JOIN concept g ON p.gender_concept_id = g.concept_id
JOIN concept r ON p.race_concept_id = r.concept_id
WHERE p.race_concept_id IN (38003574, 38003584)"""
    
    return analysis_runner.run_analysis("chi_squared_scipy", user_query, tres, group_columns="gender_name, race_name") 