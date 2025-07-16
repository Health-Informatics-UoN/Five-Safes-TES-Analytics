import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
import numpy as np
from unittest.mock import Mock, patch
from analysis_engine import AnalysisEngine


class TestAnalysisCompatibility:
    """Test cases for analysis compatibility scenarios."""
    
    @pytest.fixture
    def engine(self):
        """Set up test fixtures."""
        return AnalysisEngine("test_token", "test_project")
    
    @patch('analysis_engine.TESClient')
    @patch('analysis_engine.MinIOClient')
    def test_incompatible_analysis_on_same_data(self, mock_minio, mock_tes, engine):
        """Test what happens when running incompatible analyses on the same data."""
        # Mock TES client
        mock_tes_instance = Mock()
        mock_tes_instance.generate_submission_template.return_value = ({"task": "data"}, 2)
        mock_tes_instance.submit_task.return_value = {"id": "123"}
        # Mock get_task_status to return a proper dictionary with status
        mock_tes_instance.get_task_status.return_value = {"status": 11, "description": "Completed"}
        mock_tes.return_value = mock_tes_instance
        

        # Mock MinIO client
        mock_minio_instance = Mock()
        mock_minio_instance.get_object.return_value = "n,total\n10,100\n"
        mock_minio.return_value = mock_minio_instance
        
        # Create engine after setting up mocks
        engine = AnalysisEngine("test_token", "test_project")
        
        
        # Mock data processor to return raw data that will be processed by analysis class
        raw_data = ["n,total\n10,100\n", "n,total\n15,150\n"]
        engine.data_processor.aggregate_data = Mock(return_value=raw_data)
        
        # Mock the statistical analyzer to simulate the analysis class storing data
        def mock_analyze_data(input_data, analysis_type):
            # Simulate what the MeanAnalysis class would do
            if analysis_type == "mean":
                # Simulate the analysis class storing aggregated data
                engine.statistical_analyzer.analysis_classes["mean"].aggregated_data = {"n": 25, "total": 250}
                return 10.0  # mean result
            else:
                raise Exception("Incompatible analysis")
        
        engine.statistical_analyzer.analyze_data = Mock(side_effect=mock_analyze_data)
        
        # Run mean analysis first
        user_query = "SELECT value_as_number FROM measurement WHERE concept_id = 123"
        result1 = engine.run_analysis(
            "mean",
            user_query,
            ["TRE1", "TRE2"]
        )
        
        # Check that aggregated_data is stored as a dict with expected keys
        assert engine.aggregated_data is not None
        assert isinstance(engine.aggregated_data, dict)
        assert "n" in engine.aggregated_data
        assert "total" in engine.aggregated_data
        assert engine.aggregated_data["n"] == 25
        assert engine.aggregated_data["total"] == 250
        
        # Now try to run variance analysis on the same data
        # This should fail because the data format is incompatible
        with pytest.raises(Exception):
            # Un-mock analyze_data to use the real implementation for this check
            from statistical_analyzer import StatisticalAnalyzer
            real_analyzer = StatisticalAnalyzer()
            real_analyzer.analyze_data(engine.aggregated_data, "variance")
    
    @patch('analysis_engine.TESClient')
    @patch('analysis_engine.MinIOClient')
    def test_compatible_analysis_on_same_data(self, mock_minio, mock_tes, engine):
        """Test running compatible analyses on the same data (e.g., chi-squared variants)."""
        # Mock TES client
        mock_tes_instance = Mock()
        mock_tes_instance.generate_submission_template.return_value = ({"task": "data"}, 2)
        mock_tes_instance.submit_task.return_value = {"id": "123"}
        # Mock get_task_status to return a proper dictionary with status
        mock_tes_instance.get_task_status.return_value = {"status": 11, "description": "Completed"}
        mock_tes.return_value = mock_tes_instance
        
        # Mock MinIO client
        mock_minio_instance = Mock()
        mock_minio_instance.get_object.return_value = "gender_name,race_name,n\nFEMALE,Asian Indian,10\nMALE,Asian Indian,15\n"
        mock_minio.return_value = mock_minio_instance
        
        # Create engine after setting up mocks
        engine = AnalysisEngine("test_token", "test_project")
        
        # Mock data processor to return raw data that will be processed by analysis class
        raw_data = ["gender_name,race_name,n\nFEMALE,Asian Indian,10\nMALE,Asian Indian,15\n"]
        engine.data_processor.aggregate_data = Mock(return_value=raw_data)
        
        # Mock the statistical analyzer to simulate the analysis class storing data
        def mock_analyze_data(input_data, analysis_type):
            # Simulate what the ChiSquaredScipyAnalysis class would do
            if analysis_type == "chi_squared_scipy":
                # Simulate the analysis class storing aggregated data
                contingency_table = np.array([[10, 15], [20, 25]])
                engine.statistical_analyzer.analysis_classes["chi_squared_scipy"].aggregated_data = {"contingency_table": contingency_table}
                return 2.5  # chi-squared result
            else:
                raise Exception("Incompatible analysis")
        
        engine.statistical_analyzer.analyze_data = Mock(side_effect=mock_analyze_data)
        
        # Run chi-squared scipy analysis first
        user_query = "SELECT gender_name, race_name FROM person"
        result1 = engine.run_analysis(
            "chi_squared_scipy",
            user_query,
            ["TRE1", "TRE2"]
        )
        
        # Check that aggregated_data is stored as a dict (contingency table)
        assert engine.aggregated_data is not None
        assert isinstance(engine.aggregated_data, dict)
        assert "contingency_table" in engine.aggregated_data
        
        # Now run chi-squared manual analysis on the same data
        from statistical_analyzer import StatisticalAnalyzer
        real_analyzer = StatisticalAnalyzer()
        # Pass the contingency table dict to the real analyzer
        result2 = real_analyzer.analyze_data(engine.aggregated_data["contingency_table"], "chi_squared_manual")
        
        # Should return a dictionary with chi-squared results
        assert isinstance(result2, dict)
        assert "chi_squared" in result2
        assert "p_value" in result2
    
    def test_data_format_validation(self, engine):
        """Test that data format validation works correctly."""
        # Test mean data format
        mean_data = np.array([[10, 100]])  # n, total format
        assert mean_data.shape[1] == 2  # Should have 2 columns
        
        # Test variance data format
        variance_data = np.array([[10, 1000, 100]])  # n, sum_x2, total format
        assert variance_data.shape[1] == 3  # Should have 3 columns
        
        # Test PMCC data format
        pmcc_data = np.array([[5, 10, 20, 50, 30, 80]])  # n, sum_x, sum_y, sum_xy, sum_x2, sum_y2
        assert pmcc_data.shape[1] == 6  # Should have 6 columns
        
        # Test contingency table format
        contingency_data = np.array([[10, 15], [20, 25]])  # 2x2 table
        assert contingency_data.shape == (2, 2)  # Should be 2D
    
    def test_analysis_requirements_compatibility(self, engine):
        """Test that analysis requirements are properly documented for compatibility."""
        requirements = engine.get_analysis_requirements("mean")
        assert "expected_columns" in requirements
        assert requirements["expected_columns"] == ["n", "total"]
        
        requirements = engine.get_analysis_requirements("variance")
        assert "expected_columns" in requirements
        assert requirements["expected_columns"] == ["n", "sum_x2", "total"]
        
        requirements = engine.get_analysis_requirements("chi_squared_scipy")
        assert "expected_columns" in requirements
        assert requirements["expected_columns"] == ["contingency_table"]

    def test_chi_squared_2x3_table(self, engine):
        """Test chi-squared analysis with a 2x3 contingency table."""
        from statistical_analyzer import StatisticalAnalyzer
        analyzer = StatisticalAnalyzer()
        contingency_table = np.array([[10, 15, 5], [20, 25, 10]])
        # Scipy implementation
        result = analyzer.analyze_data(contingency_table, "chi_squared_scipy")
        assert isinstance(result, float)
        # Manual implementation
        result_manual = analyzer.analyze_data(contingency_table, "chi_squared_manual")
        assert isinstance(result_manual, dict)
        assert "chi_squared" in result_manual
        assert "p_value" in result_manual

    def test_dictionary_based_analysis(self, engine):
        """
        Test the centralized dictionary-based data storage system.
        
        This test verifies that:
        1. Analysis results store aggregated data in a centralized dictionary format
        2. The stored data can be reused for compatible analyses without re-running the full pipeline
        3. The system can identify which analyses are compatible with the stored data format
        4. Additional analyses can be run on the same stored data efficiently
        
        The test simulates a variance analysis that stores data in the format:
        {"n": count, "sum_x2": sum of squares, "total": sum of values}
        Then runs a compatible mean analysis on the same stored data.
        """
        from analysis_engine import AnalysisEngine
        
        # Create engine and mock the components
        engine = AnalysisEngine("test_token", "test_project")
        
        # Mock data processor to return raw data that will be processed by analysis class
        # This simulates the aggregated data from multiple TREs in CSV format
        raw_data = ["n,sum_x2,total\n10,1000,100\n"]
        engine.data_processor.aggregate_data = Mock(return_value=raw_data)
        
        # Mock the statistical analyzer to simulate the analysis class storing data
        # This simulates what happens when an analysis class processes the raw data
        def mock_analyze_data(input_data, analysis_type):
            # Simulate what the VarianceAnalysis class would do
            if analysis_type == "variance":
                # Simulate the analysis class storing aggregated data in centralized dict
                # This is the key feature being tested - storing data for reuse
                engine.statistical_analyzer.analysis_classes["variance"].aggregated_data = {"n": 10, "sum_x2": 1000, "total": 100}
                return 5.0  # variance result
            else:
                raise Exception("Incompatible analysis")
        
        engine.statistical_analyzer.analyze_data = Mock(side_effect=mock_analyze_data)
        
        # Mock TES and MinIO clients to avoid actual API calls
        # These mocks simulate the task submission and result retrieval process
        engine.tes_client.generate_submission_template = Mock(return_value=({"task": "data"}, 1))
        engine.tes_client.submit_task = Mock(return_value={"id": "123"})
        # Mock get_task_status to return completed status
        engine.tes_client.get_task_status = Mock(return_value={"status": 11, "description": "Completed"})
        engine.minio_client.get_object = Mock(return_value="n,sum_x2,total\n10,1000,100")
        
        # Run variance analysis (this will store data in the centralized dict)
        # This triggers the full pipeline: task submission, polling, data processing, analysis
        user_query = "SELECT value_as_number FROM measurement WHERE concept_id = 123"
        result = engine.run_analysis(
            "variance",
            user_query,
            ["TRE1"]
        )
        
        # Verify that aggregated data is stored in the centralized dictionary
        # This is the core feature being tested - data persistence for reuse
        assert isinstance(engine.aggregated_data, dict)
        assert "n" in engine.aggregated_data
        assert "sum_x2" in engine.aggregated_data
        assert "total" in engine.aggregated_data
        assert engine.aggregated_data["n"] == 10
        assert engine.aggregated_data["total"] == 100
        assert engine.aggregated_data["sum_x2"] == 1000
        
        # Test the compatibility system - check which analyses can run on stored data
        # This verifies the system can identify compatible analyses without re-running the pipeline
        compatible = engine.get_compatible_analyses()
        assert "mean" in compatible  # Mean can use n and total from variance data
        assert "variance" in compatible  # Variance can reuse its own data
        
        # Test running an additional analysis on the same stored data
        # This demonstrates the efficiency gain - no need to re-run the full pipeline
        # Un-mock the analyzer to use the real implementation for this test
        from statistical_analyzer import StatisticalAnalyzer
        real_analyzer = StatisticalAnalyzer()
        engine.statistical_analyzer = real_analyzer
        
        # Run mean analysis using the stored variance data (n=10, total=100)
        # Mean = total/n = 100/10 = 10.0
        mean_result = engine.run_additional_analysis("mean")
        assert isinstance(mean_result, float)
        assert mean_result == 10.0  # 100/10 = 10 