import pytest
import numpy as np
from unittest.mock import Mock, patch
import sys
import os

# Add the parent directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from statistical_analyzer import ContingencyTableAnalysis


class TestContingencyTableAnalysis:
    """Test contingency table analysis with realistic data."""
    
    @pytest.fixture
    def contingency_analyzer(self):
        """Create a contingency table analyzer instance."""
        return ContingencyTableAnalysis()
    
    @pytest.fixture
    def realistic_2x3_data(self):
        """Realistic 2x3 contingency table data from JSON."""
        # Simulating JSON data from Docker containers: gender vs race with realistic counts
        return {
            "contingency_table": [
                {"gender": "Male", "race": "White", "n": 45},
                {"gender": "Male", "race": "Black", "n": 23},
                {"gender": "Male", "race": "Asian", "n": 12},
                {"gender": "Female", "race": "White", "n": 52},
                {"gender": "Female", "race": "Black", "n": 28},
                {"gender": "Female", "race": "Asian", "n": 15}
            ]
        }
    
    @pytest.fixture
    def aggregated_contingency_data(self):
        """Aggregated data from multiple TREs."""
        # Simulating results from 3 TREs with different sample sizes
        return [
            # TRE1 results
            {
                "contingency_table": [
                    {"gender": "Male", "race": "White", "n": 15},
                    {"gender": "Male", "race": "Black", "n": 8},
                    {"gender": "Male", "race": "Asian", "n": 4},
                    {"gender": "Female", "race": "White", "n": 17},
                    {"gender": "Female", "race": "Black", "n": 9},
                    {"gender": "Female", "race": "Asian", "n": 5}
                ]
            },
            # TRE2 results
            {
                "contingency_table": [
                    {"gender": "Male", "race": "White", "n": 18},
                    {"gender": "Male", "race": "Black", "n": 9},
                    {"gender": "Male", "race": "Asian", "n": 5},
                    {"gender": "Female", "race": "White", "n": 20},
                    {"gender": "Female", "race": "Black", "n": 11},
                    {"gender": "Female", "race": "Asian", "n": 6}
                ]
            },
            # TRE3 results
            {
                "contingency_table": [
                    {"gender": "Male", "race": "White", "n": 12},
                    {"gender": "Male", "race": "Black", "n": 6},
                    {"gender": "Male", "race": "Asian", "n": 3},
                    {"gender": "Female", "race": "White", "n": 15},
                    {"gender": "Female", "race": "Black", "n": 8},
                    {"gender": "Female", "race": "Asian", "n": 4}
                ]
            }
        ]
    
    def test_contingency_table_aggregation(self, contingency_analyzer, aggregated_contingency_data):
        """Test aggregating contingency table data from multiple TREs."""
        # Aggregate the data
        aggregated = contingency_analyzer.aggregate_data(aggregated_contingency_data)
        
        # Check that aggregated data is stored
        assert contingency_analyzer.aggregated_data is not None
        assert "contingency_table" in contingency_analyzer.aggregated_data
        
        # Get the contingency table
        contingency_table = contingency_analyzer.aggregated_data["contingency_table"]
        
        # Should be a 2x3 table (2 genders, 3 races)
        assert contingency_table.shape == (2, 3)
        
        # Check the aggregated counts - verify correct positioning
        # Expected: Male=80, Female=95, White=97, Black=46, Asian=27
        total_sum = np.sum(contingency_table)
        assert total_sum == 175  # Total of all counts
        
        # Check that we have the right number of non-zero entries
        non_zero_count = np.count_nonzero(contingency_table)
        assert non_zero_count == 6  # Should have 6 non-zero entries (2x3 table)
        
        # Check that all expected values are present somewhere in the table
        expected_values = {45, 23, 12, 52, 28, 15}
        actual_values = set(contingency_table.flatten())
        assert actual_values == expected_values
        
        # Check row and column sums to verify correct positioning
        row_sums = np.sum(contingency_table, axis=1)
        col_sums = np.sum(contingency_table, axis=0)
        
        # Row sums should be Male=80, Female=95 (order may vary)
        expected_row_sums = {80, 95}
        actual_row_sums = set(row_sums)
        assert actual_row_sums == expected_row_sums
        
        # Column sums should be White=97, Black=46, Asian=27 (order may vary)
        expected_col_sums = {97, 51, 27}
        actual_col_sums = set(col_sums)
        assert actual_col_sums == expected_col_sums
    
    
    def test_contingency_table_with_csv_format(self, contingency_analyzer):
        """Test contingency table analysis with CSV-formatted data."""
        # Simulate CSV data as string
        csv_data = [
            "gender,race,count\nMale,White,45\nMale,Black,23\nMale,Asian,12\nFemale,White,52\nFemale,Black,28\nFemale,Asian,15"
        ]
        
        # Mock the CSV parsing to return structured data
        with patch('builtins.open', create=True) as mock_open:
            mock_open.return_value.__enter__.return_value.readlines.return_value = [
                "gender,race,count\n",
                "Male,White,45\n",
                "Male,Black,23\n",
                "Male,Asian,12\n",
                "Female,White,52\n",
                "Female,Black,28\n",
                "Female,Asian,15\n"
            ]
            
            # Aggregate the data
            aggregated = contingency_analyzer.aggregate_data(csv_data)
            
            # Check that we get a valid contingency table
            contingency_table = contingency_analyzer.aggregated_data["contingency_table"]
            assert contingency_table.shape == (2, 3)
            
            # Check the aggregated counts - verify correct positioning
            total_sum = np.sum(contingency_table)
            assert total_sum == 175  # Total of all counts
            
            # Check that we have the right number of non-zero entries
            non_zero_count = np.count_nonzero(contingency_table)
            assert non_zero_count == 6  # Should have 6 non-zero entries (2x3 table)
            
            # Check that all expected values are present somewhere in the table
            expected_values = {45, 23, 12, 52, 28, 15}
            actual_values = set(contingency_table.flatten())
            assert actual_values == expected_values
            
            # Check row and column sums to verify correct positioning
            row_sums = np.sum(contingency_table, axis=1)
            col_sums = np.sum(contingency_table, axis=0)
            
            # Row sums should be Male=80, Female=95 (order may vary)
            expected_row_sums = {80, 95}
            actual_row_sums = set(row_sums)
            assert actual_row_sums == expected_row_sums
            
            # Column sums should be White=97, Black=51, Asian=27 (order may vary)
            expected_col_sums = {97, 51, 27}
            actual_col_sums = set(col_sums)
            assert actual_col_sums == expected_col_sums
    
    def test_contingency_table_edge_cases(self, contingency_analyzer):
        """Test contingency table with edge cases."""
        
        # Test with zero counts
        zero_data = {
            "contingency_table": [
                {"gender": "Male", "race": "White", "n": 0},
                {"gender": "Male", "race": "Black", "n": 0},
                {"gender": "Female", "race": "White", "n": 0},
                {"gender": "Female", "race": "Black", "n": 0}
            ]
        }
        
        contingency_analyzer.aggregate_data(zero_data)

        ## Should return contingency table and headers of all zeros
        #          "row_labels": ["Male", "Female"],
        #          "col_labels": ["White", "Black"],
        #          "header": "gender,race,n"
        assert contingency_analyzer.aggregated_data["contingency_table"].shape == (2, 2)
        assert np.all(contingency_analyzer.aggregated_data["contingency_table"] == 0)
        assert contingency_analyzer.aggregated_data["contingency_table_headers"] == {"row_labels": ["Male", "Female"], "col_labels": ["White", "Black"], "header": "gender,race,n"}
        
        # Test with single cell
        single_cell_data = {
            "contingency_table": [
                {"gender": "Male", "race": "Black", "n": 10}
            ]
        }
        
        contingency_analyzer.aggregate_data(single_cell_data)
        contingency_table, headers = contingency_analyzer.analyze()

        assert contingency_table.shape == (1, 1)
        assert headers == {"row_labels": ["Male"], "col_labels": ["Black"], "header": "gender,race,n"}
        

        row_index = list(headers['row_labels']).index("Male")
        col_index = list(headers['col_labels']).index("Black")
        assert contingency_table[row_index, col_index] == 10


    
    def test_contingency_table_return_format(self, contingency_analyzer):
        """Test that return format is correctly defined."""
        return_format = contingency_analyzer.return_format
        
        # Should return a dictionary with contingency table
        assert isinstance(return_format, dict)
        assert "contingency_table" in return_format
        assert return_format["contingency_table"] is None  # Placeholder value 