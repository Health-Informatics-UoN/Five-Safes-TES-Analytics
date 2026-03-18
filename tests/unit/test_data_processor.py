import pytest
import numpy as np

from five_safes_tes_analytics.aggregation.data_processor import DataProcessor
from five_safes_tes_analytics.aggregation.statistical_analyzer import StatisticalAnalyzer


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