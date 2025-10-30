import numpy as np
import csv
from collections import defaultdict
from typing import List, Dict, Any, Union
import os
import json


class DataProcessor:
    """
    Handles data processing, aggregation, and file operations for federated analysis.
    """
    
    def __init__(self):
        """Initialize the data processor."""
        pass
    
    def import_csv_data(self, input_data: Union[str, List[str]]) -> np.ndarray:
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
    
    # def get_result_from_local_file(self, file_path: str) -> List[str] | dict:
    #     """
    #     Read results from a local CSV file.
    #     
    #     Args:
    #         file_path (str): Path to the CSV file
    #         
    #     Returns:
    #         List[str]: Data from the file
    #     """
    #     if not os.path.exists(file_path):
    #         raise FileNotFoundError(f"File not found: {file_path}")
    #     
    #     if file_path.endswith('.json'):
    #         with open(file_path, 'r') as file:
    #             return json.load(file)
    #     
    #     elif file_path.endswith('.csv'):
    #         with open(file_path, 'r') as file:
    #             reader = csv.reader(file)
    #             next(reader)  # Skip header row
    #             for row in reader:
    #                 return row  # Return first data row
    
    # def combine_file_data(self, file_list: List[str]) -> np.ndarray | dict:
    #     """
    #     Combine data from multiple local files.
    #     
    #     Args:
    #         file_list (List[str]): List of file paths
    #         
    #     Returns:
    #         np.ndarray: Combined data as 2D array
    #     """
    #     data = None
    #     for file_path in file_list:
    #         
    #         #file_data = np.array(self.get_result_from_local_file(file_path)).astype(float)
    #         file_data = self.get_result_from_local_file(file_path) ## could return a dict if it is json data

    #         if isinstance (file_data, list):
    #             file_data = np.array(file_data).astype(float)
    #         elif isinstance (file_data, dict):
    #             #values = [file_data[key][0] for key in file_data.keys()]
    #             #file_data = np.array([values])
    #             if data is None:
    #                 data = {}
    #             for key, value in file_data.items():
    #                 if key not in data:
    #                     data[key] = []
    #                 data[key].append(value[0])


    #         elif isinstance (file_data, np.ndarray):
    #             file_data = file_data.astype(float)
    #         
    #         if not isinstance(data, dict):
    #             if data is not None:
    #                 data = file_data.reshape(1, -1)  # First array, reshape to 2D
    #             else:
    #                 data = np.vstack((data, file_data.reshape(1, -1)))  # Stack subsequent arrays
    #     return data
    
    def aggregate_data(self, inputs: Union[List[str], Dict[str, List[float]], List[Dict[str, Any]]], analysis_type: str) -> Union[np.ndarray, List[np.ndarray]]:
        """
        Aggregate data based on analysis type.
        
        Args:
            inputs: Either List[str] (CSV strings), Dict[str, List[float]] (pre-aggregated JSON dict), 
                    or List[Dict[str, Any]] (list of JSON results from multiple TREs)
            analysis_type (str): Type of analysis to perform
            
        Returns:
            Union[np.ndarray, List[np.ndarray]]: Aggregated data
        """
        from statistical_analyzer import StatisticalAnalyzer
        
        analyzer = StatisticalAnalyzer()
        analysis_config = analyzer.get_analysis_config(analysis_type)
        
        # Check if inputs is a list of results from multiple TREs
        if isinstance(inputs, list) and inputs:
            # Check if first element is a dict (for mean, variance, PMCC)
            if isinstance(inputs[0], dict):
                # Handle list of dicts: [{"n": 65, "total": 117.0}, {"n": 42, "total": 89.0}, ...]
                # Convert to dict of lists: {"n": [65, 42, ...], "total": [117.0, 89.0, ...]}
                combined = {}
                for item in inputs:
                    if isinstance(item, dict):
                        for key, value in item.items():
                            if key not in combined:
                                combined[key] = []
                            combined[key].append(value)
                inputs = combined
            # Check if first element is a list (for contingency tables)
            elif isinstance(inputs[0], list):
                # Handle list of lists: [[{"category": "A", "n": 10}, ...], [{"category": "B", "n": 20}, ...]]
                # Flatten all rows from all TREs into a single list
                # The statistical analyzer will handle the aggregation
                flattened = []
                for table_list in inputs:
                    if isinstance(table_list, list):
                        flattened.extend(table_list)
                
                # Store as dict with "contingency_table" key
                inputs = {"contingency_table": flattened}
        
        if analysis_config["return_format"] == "contingency_table":
            if isinstance(inputs, dict):
                # Handle JSON dict format for contingency tables
                if "contingency_table" in inputs:
                    # Already in the format {"contingency_table": [list of dicts]}
                    # Pass directly to statistical analyzer which will handle aggregation
                    data = inputs

            else:
                # Handle CSV string format (existing logic)
                combined_table = combine_contingency_tables(inputs)
                data = dict_to_array(combined_table)
        else:
            if isinstance(inputs, dict):
                # Handle JSON dict format: {"n": [65.0, 42.0], "total": [117.0, 89.0]}
                # Convert dict of lists to numpy array
                keys = list(inputs.keys())
                values = [inputs[key] for key in keys]
                data = np.array(values).T  # Transpose to get rows as data points
            else:
                # Handle CSV string format (existing logic)
                data = [self.import_csv_data(input) for input in inputs]
                # Convert list of arrays to single numpy array using vstack
                if data and len(data) > 0:
                    data = np.vstack(data)
        
        return data

def combine_contingency_tables(contingency_tables: List[str] | Dict[str, Any]) -> Dict[str, Any]:
    """
    Combine multiple contingency tables.
    
    Args:
        contingency_tables (List[str]): List of CSV strings containing contingency tables
        
    Returns:
        Dict[str, Any]: Combined contingency table as dictionary
    """

    if isinstance(contingency_tables, dict):
        ### it's already a dict of lists of the data, so we just need to sum the values in each list
        for key, value in contingency_tables.items():
            contingency_tables[key] = sum(value)
        return contingency_tables
    
        

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
    
    
    # def combine_contingency_files(self, file_list: List[str]) -> np.ndarray:
    #     """
    #     Combine contingency tables from multiple files.
    #     
    #     Args:
    #         file_list (List[str]): List of file paths
    #         
    #     Returns:
    #         np.ndarray: Combined contingency table as 2D array
    #     """
    #     labels = defaultdict(int)
    #     
    #     for file_path in file_list:
    #         with open(file_path, 'r') as file:
    #             reader = csv.reader(file)
    #             labels["header"] = next(reader)
    #             for row in reader:
    #                 if len(row) < 2:  # Skip rows without enough parts
    #                     continue
    #                 row_without_count = ','.join(row[:-1])  # Get rest of row without count
    #                 labels[row_without_count] += int(row[-1])
    #     
    #     array_table = self.dict_to_array(labels)
    #     return array_table
    
def dict_to_array(contingency_dict: Dict[str, Any]) -> np.ndarray:
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
    row_labels = list(first_values)
    col_labels = list(second_values)
    result = np.zeros((len(row_labels), len(col_labels)))
    
    labels = {"row_labels": row_labels, "col_labels": col_labels, "header": contingency_dict.get('header', '')}
    
    # Fill array using the keys to determine position
    for key, value in contingency_dict.items():
        if key != 'header':
            first_part, second_part = key.split(',')
            row_idx = row_labels.index(first_part)   # First part as rows
            col_idx = col_labels.index(second_part)  # Second part as columns
            result[row_idx, col_idx] = value
    
    return result, labels

