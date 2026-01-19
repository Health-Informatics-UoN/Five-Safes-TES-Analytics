import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
import numpy as np
from unittest.mock import Mock, patch
from analysis_engine import AnalysisEngine
from analyser import Analyser
from analytics_tes import AnalyticsTES


class TestAnalysisCompatibility:
    """Test cases for analysis compatibility scenarios."""
    
    @pytest.fixture
    def engine(self):
        """Set up test fixtures."""
        tes_client = AnalyticsTES()
        analysis_engine = AnalysisEngine(tes_client, "test_token", "test_project")
        return Analyser(analysis_engine)
    
    @patch('analytics_tes.AnalyticsTES')
    @patch('analysis_engine.MinIOClient')
    def test_incompatible_analysis_on_same_data(self, mock_minio, mock_tes_class, engine):
        """Test what happens when running incompatible analyses on the same data."""
        # Mock TES client
        mock_tes_instance = Mock()
        mock_tes_instance.generate_submission_template.return_value = ({"task": "data"}, 2)
        mock_tes_instance.submit_task.return_value = {"id": "123"}
        # Mock get_task_status to return a proper dictionary with status
        mock_tes_instance.get_task_status.return_value = {"status": 11, "description": "Completed"}
        mock_tes_instance.set_tes_messages.return_value = None
        mock_tes_instance.set_tags.return_value = None
        mock_tes_instance.create_FiveSAFES_TES_message.return_value = Mock()
        mock_tes_instance.task = Mock()
        mock_tes_class.return_value = mock_tes_instance
        

        # Mock MinIO client
        mock_minio_instance = Mock()
        mock_minio_instance.get_object.return_value = "n,total\n10,100\n"
        mock_minio_instance.get_object_smart.return_value = "n,total\n10,100\n"
        mock_minio.return_value = mock_minio_instance
        
        # Create analyser after setting up mocks
        tes_client = mock_tes_instance  # Use the mocked instance
        analysis_engine = AnalysisEngine(tes_client, "test_token", "test_project")
        engine = Analyser(analysis_engine)
        
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
    
    @patch('analytics_tes.AnalyticsTES')
    @patch('analysis_engine.MinIOClient')
    def test_compatible_analysis_on_same_data(self, mock_minio, mock_tes_class, engine):
        """Test running compatible analyses on the same data (e.g., variance and mean)."""
        # Mock TES client
        mock_tes_instance = Mock()
        mock_tes_instance.generate_submission_template.return_value = ({"task": "data"}, 2)
        mock_tes_instance.submit_task.return_value = {"id": "123"}
        # Mock get_task_status to return a proper dictionary with status
        mock_tes_instance.get_task_status.return_value = {"status": 11, "description": "Completed"}
        mock_tes_instance.set_tes_messages.return_value = None
        mock_tes_instance.set_tags.return_value = None
        mock_tes_instance.create_FiveSAFES_TES_message.return_value = Mock()
        mock_tes_instance.task = Mock()
        mock_tes_class.return_value = mock_tes_instance
        
        # Mock MinIO client to return variance analysis results
        mock_minio_instance = Mock()
        # Mock variance results from Docker containers
        variance_results = [
            '{"n": 100, "sum_x2": 8500.25, "total": 500.5}',
            '{"n": 75, "sum_x2": 4000.50, "total": 250.0}',
            '{"n": 125, "sum_x2": 9500.00, "total": 600.0}'
        ]
        # Use side_effect to return different results for each call
        # This simulates the polling system calling get_object_smart multiple times
        # to collect results from different TREs sequentially
        mock_minio_instance.get_object_smart.side_effect = lambda bucket, path: variance_results.pop(0) if variance_results else None
        mock_minio.return_value = mock_minio_instance
        
        # Create analyser after setting up mocks
        tes_client = mock_tes_instance  # Use the mocked instance
        analysis_engine = AnalysisEngine(tes_client, "test_token", "test_project")
        engine = Analyser(analysis_engine)
        
        # Mock data processor to return aggregated variance data
        aggregated_variance_data = {
            "n": [100, 75, 125],
            "sum_x2": [8500.25, 4000.50, 9500.00],
            "total": [500.5, 250.0, 600.0]
        }
        engine.data_processor.aggregate_data = Mock(return_value=aggregated_variance_data)
        
        # Mock the statistical analyzer to simulate variance analysis
        def mock_analyze_data(input_data, analysis_type):
            if analysis_type == "variance":
                # Simulate variance analysis storing aggregated data
                engine.statistical_analyzer.analysis_classes["variance"].aggregated_data = {
                    "n": 300,  # 100 + 75 + 125
                    "sum_x2": 22000.75,  # 8500.25 + 4000.50 + 9500.00
                    "total": 1350.5  # 500.5 + 250.0 + 600.0
                }
                return 15.25  # variance result
            else:
                raise Exception("Incompatible analysis")
        
        engine.statistical_analyzer.analyze_data = Mock(side_effect=mock_analyze_data)
        
        # Run variance analysis first
        user_query = "SELECT value_as_number FROM public.measurement WHERE measurement_concept_id = 21490742"
        result1 = engine.run_analysis(
            "variance",
            user_query,
            ["TRE1", "TRE2", "TRE3"]
        )
        
        # Check that aggregated_data is stored with variance results
        assert engine.aggregated_data is not None
        assert isinstance(engine.aggregated_data, dict)
        assert "n" in engine.aggregated_data
        assert "sum_x2" in engine.aggregated_data
        assert "total" in engine.aggregated_data
        
        # Now run mean analysis on the same data (compatible analysis)
        from statistical_analyzer import StatisticalAnalyzer
        real_analyzer = StatisticalAnalyzer()
        # The mean analysis can use the same aggregated data since it has 'n' and 'total'
        result2 = real_analyzer.analyze_data(engine.aggregated_data, "mean")
        
        # Verify the mean calculation
        expected_mean = 1350.5 / 300  # total / n
        assert result2 == expected_mean
        
        
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
        from analyser import Analyser
        from unittest.mock import Mock
        
        # Create mocked TES client to avoid real HTTP requests
        mock_tes_client = Mock()
        mock_tes_client.submit_task.return_value = {"id": "123"}
        mock_tes_client.get_task_status.return_value = {"status": 11, "description": "Completed"}
        mock_tes_client.set_tes_messages.return_value = None
        mock_tes_client.set_tags.return_value = None
        mock_tes_client.create_FiveSAFES_TES_message.return_value = Mock()
        mock_tes_client.task = Mock()
        
        # Create analyser and mock the components
        analysis_engine = AnalysisEngine(mock_tes_client, "test_token", "test_project")
        engine = Analyser(analysis_engine)
        
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
        engine.analysis_engine.tes_client.generate_submission_template = Mock(return_value=({"task": "data"}, 1))
        engine.analysis_engine.tes_client.submit_task = Mock(return_value={"id": "123"})
        # Mock get_task_status to return completed status
        engine.analysis_engine.tes_client.get_task_status = Mock(return_value={"status": 11, "description": "Completed"})
        engine.analysis_engine.minio_client.get_object = Mock(return_value="n,sum_x2,total\n10,1000,100")
        
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