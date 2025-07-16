import numpy as np
import csv
from collections import defaultdict
from typing import List, Dict, Any, Union
import os


class DataProcessor:
    """
    Handles data processing, aggregation, and file operations for federated analysis.
    """
    
    def __init__(self):
        """Initialize the data processor."""
        pass
    
    def import_data(self, input_data: Union[str, List[str]]) -> np.ndarray:
        """
        Import data from CSV string or list of strings.
        
        Args:
            input_data: CSV string or list of CSV strings
            
        Returns:
            np.ndarray: Parsed numerical data
        """
        if isinstance(input_data, str):
            # Remove header if present
            lines = input_data.split("\n")
            if len(lines) > 1:
                input_data = lines[1]  # Take first data line
            
            values = np.array([int(x) for x in input_data.split(",")])
            return values
        else:
            # Handle list of strings
            return np.array([int(x) for x in input_data[0].split(",")])
    
    def get_result_from_local_file(self, file_path: str) -> List[str]:
        """
        Read results from a local CSV file.
        
        Args:
            file_path (str): Path to the CSV file
            
        Returns:
            List[str]: Data from the file
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
            
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            next(reader)  # Skip header row
            for row in reader:
                return row  # Return first data row
    
    def combine_file_data(self, file_list: List[str]) -> np.ndarray:
        """
        Combine data from multiple local files.
        
        Args:
            file_list (List[str]): List of file paths
            
        Returns:
            np.ndarray: Combined data as 2D array
        """
        data = None
        for file_path in file_list:
            file_data = np.array(self.get_result_from_local_file(file_path)).astype(float)
            if data is None:
                data = file_data.reshape(1, -1)  # First array, reshape to 2D
            else:
                data = np.vstack((data, file_data.reshape(1, -1)))  # Stack subsequent arrays
        return data
    
    def parse_contingency_table(self, csv_data: str) -> np.ndarray:
        """
        Parse contingency table from CSV string.
        
        Args:
            csv_data (str): CSV string containing contingency table
            
        Returns:
            np.ndarray: Contingency table array with dimensions determined by the data
        """
        # Skip header row and empty rows
        rows = [row.strip() for row in csv_data.split('\n') if row.strip()]
        
        if len(rows) < 2:  # Need at least header + 1 data row
            raise ValueError("CSV data must contain a header row and at least one data row")
            
        # Get header row to determine dimensions
        header = rows[0].split(',')
        if len(header) < 2:  # Need at least 2 columns
            raise ValueError("CSV data must contain at least 2 columns")
            
        # Skip the header row
        data_rows = rows[1:]
        
        # Extract just the counts
        counts = []
        for row in data_rows:
            try:
                count = int(row.split(',')[-1])  # Get the last column (count)
                counts.append(count)
            except (ValueError, IndexError) as e:
                raise ValueError(f"Invalid data row: {row}") from e
        
        # Determine dimensions from data
        total_cells = len(counts)
        if total_cells == 0:
            raise ValueError("No data found in CSV")
            
        # Return as 1D array if dimensions can't be determined
        return np.array(counts)
    def combine_contingency_tables(self, contingency_tables: List[str]) -> Dict[str, Any]:
        """
        Combine multiple contingency tables.
        
        Args:
            contingency_tables (List[str]): List of CSV strings containing contingency tables
            
        Returns:
            Dict[str, Any]: Combined contingency table as dictionary
        """
        labels = {}
        
        for table in contingency_tables:
            rows = [row.strip() for row in table.split('\n') if row.strip()]
            if not rows:  # Skip empty tables
                continue
                
            labels["header"] = rows[0]  # Column order is guaranteed to be the same
            
            data_rows = rows[1:]
            for row in data_rows:
                try:
                    parts = row.split(',')
                    if len(parts) < 2:  # Skip rows without enough parts
                        continue
                    count = int(parts[-1])  # Get count from last column
                    row_without_count = ','.join(parts[:-1])  # Get rest of row without count
                    if row_without_count in labels:
                        labels[row_without_count] += count
                    else:
                        labels[row_without_count] = count
                except (ValueError, IndexError) as e:
                    print(f"Warning: Skipping malformed row: {row}")
                    continue
        
        return labels
    
    def dict_to_array(self, contingency_dict: Dict[str, Any]) -> np.ndarray:
        """
        Convert contingency table dictionary to numpy array.
        
        Args:
            contingency_dict (Dict[str, Any]): Contingency table as dictionary
            
        Returns:
            np.ndarray: Contingency table as 2D array
        """
        # Get unique values for each dimension from the keys
        keys = [k for k in contingency_dict.keys() if k != 'header']
        first_values = set(k.split(',')[0] for k in keys)
        second_values = set(k.split(',')[1] for k in keys)
        
        # Create empty array
        result = np.zeros((len(second_values), len(first_values)))
        
        # Fill array using the keys to determine position
        for key, value in contingency_dict.items():
            if key != 'header':
                row, col = key.split(',')
                row_idx = list(second_values).index(col)  # Race is now rows
                col_idx = list(first_values).index(row)   # Gender is now columns
                result[row_idx, col_idx] = value
        
        return result
    
    def combine_contingency_files(self, file_list: List[str]) -> np.ndarray:
        """
        Combine contingency tables from multiple files.
        
        Args:
            file_list (List[str]): List of file paths
            
        Returns:
            np.ndarray: Combined contingency table as 2D array
        """
        labels = defaultdict(int)
        
        for file_path in file_list:
            with open(file_path, 'r') as file:
                reader = csv.reader(file)
                labels["header"] = next(reader)
                for row in reader:
                    if len(row) < 2:  # Skip rows without enough parts
                        continue
                    row_without_count = ','.join(row[:-1])  # Get rest of row without count
                    labels[row_without_count] += int(row[-1])
        
        array_table = self.dict_to_array(labels)
        return array_table
    
    def aggregate_data(self, inputs: List[str], analysis_type: str) -> Union[np.ndarray, List[np.ndarray]]:
        """
        Aggregate data based on analysis type.
        
        Args:
            inputs (List[str]): List of input data strings
            analysis_type (str): Type of analysis to perform
            
        Returns:
            Union[np.ndarray, List[np.ndarray]]: Aggregated data
        """
        from statistical_analyzer import StatisticalAnalyzer
        
        analyzer = StatisticalAnalyzer()
        analysis_config = analyzer.get_analysis_config(analysis_type)
        
        if analysis_config["return_format"] == "contingency_table":
            combined_table = self.combine_contingency_tables(inputs)
            data = self.dict_to_array(combined_table)
        else:
            data = [self.import_data(input) for input in inputs]
            # Convert list of arrays to single numpy array using vstack
            if data and len(data) > 0:
                data = np.vstack(data)
        
        return data
    
