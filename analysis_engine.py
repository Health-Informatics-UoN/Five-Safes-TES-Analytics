import json
import time
import os
from typing import List, Dict, Any, Optional, Union
import numpy as np
from dotenv import load_dotenv
from data_processor import DataProcessor
from statistical_analyzer import StatisticalAnalyzer
from tes_client import TESClient
from tes_client import get_status_description
from minio_client import MinIOClient
from query_builder import QueryBuilder
import tes
import polling

# Load environment variables from .env file
load_dotenv()

class AnalysisEngine:
    """
    Main orchestrator class for federated analysis workflow.
    Coordinates data processing, statistical analysis, and TES task management.
    """
    
    def __init__(self, token: str = None, project: str = None):
        """
        Initialize the analysis engine.
        
        Args:
            token (str): Authentication token for TRE-FX services
            project (str): Project name for TES tasks (defaults to TRE_FX_PROJECT env var)
        """
        if token is None:
            token = os.getenv('TRE_FX_TOKEN')
            if not token:
                raise ValueError("TRE_FX_TOKEN environment variable is required when token parameter is not provided")
        self.token = token
        
        # Set project from environment variable if not provided
        if project is None:
            project = os.getenv('TRE_FX_PROJECT')
            if not project:
                raise ValueError("TRE_FX_PROJECT environment variable is required when project parameter is not provided")
        
        self.project = project
        self.data_processor = DataProcessor()
        self.statistical_analyzer = StatisticalAnalyzer()
        self.tes_client = TESClient()
        self.minio_client = MinIOClient(token)
        self.query_builder = QueryBuilder()
        self.aggregated_data = {}  # Centralized dict to store all aggregated data
        
    def run_analysis(self, 
                    analysis_type: str, 
                    user_query: str = None,
                    tres: List[str] = None,
                    task_name: str = None,
                    bucket: str = None) -> Dict[str, Any]:
        """
        Run a complete federated analysis workflow.
        
        Args:
            analysis_type (str): Type of analysis to perform
            user_query (str, optional): User's data selection query (without analysis calculations)
            tres (List[str], optional): List of TREs to run analysis on
            task_name (str, optional): Name for the TES task (defaults to "analysis {analysis_type}")
            bucket (str, optional): MinIO bucket for outputs (defaults to MINIO_OUTPUT_BUCKET env var)
            
        Returns:
            Dict[str, Any]: Analysis results
        """
        # Set default task name based on analysis type if not provided
        if task_name is None:
            task_name = f"analysis {analysis_type}"
        
        # Set default bucket from environment variable if not provided
        if bucket is None:
            bucket = os.getenv('MINIO_OUTPUT_BUCKET')
            if not bucket:
                raise ValueError("MINIO_OUTPUT_BUCKET environment variable is required when bucket parameter is not provided")
        
        # Check if user is trying to run analysis on existing data
        if user_query is None and tres is None:
            # User wants to run analysis on existing aggregated data
            compatible_analyses = self.get_compatible_analyses()
            if analysis_type in compatible_analyses:
                print(f"Running {analysis_type} analysis on existing data...")
                result = self.run_additional_analysis(analysis_type)
                return {
                    'analysis_type': analysis_type,
                    'result': result,
                    'data_source': 'existing_aggregated_data',
                    'compatible_analyses': compatible_analyses
                }
            else:
                raise ValueError(f"Analysis type '{analysis_type}' not compatible with existing data. "
                               f"Available analyses: {compatible_analyses}")
        
        # Original logic for running analysis on new data
        try:
            # Build the complete query by combining user query with analysis calculations
            complete_query = self.query_builder.build_query(analysis_type, user_query)
            
            # Validate the complete query
            if not self.query_builder.validate_query(complete_query):
                raise ValueError("Invalid SQL query generated")
            
            # Generate and submit TES task
            print(f"Submitting {analysis_type} analysis to {len(tres)} TREs...")
            submission_task, n_results = self.tes_client.generate_submission_template(
                name=task_name,
                query=complete_query,
                tres=tres,
                project=self.project
            )
            
            result = self.tes_client.submit_task(submission_task, self.token)
            
            task_id = result['id']
            print(f"Task ID: {task_id}")
            results_paths = [f"{int(task_id) + i + 1}/output.csv" for i in range(n_results)]
            


            #while True:
            #    task_info = self.tes_client.get_task_status(task_id)
            #    status = task_info['status']
            #    status_description = get_status_description(status)
            #    print(f"Task status: {status}")
            #    print(status_description)
            #    ## check for completed, failed, running, or cancelled statuses - i.e., job is done
            #    if status in [11, 27, 16, 49]:
            #        break

            # Collect results from MinIO
            #print("Collecting results from MinIO...")
            # data = self._collect_results(results_paths, bucket, n_results)
            
            ## use polling engine to collect results
            polling_engine = polling.Polling(self.tes_client, self.minio_client, task_id)
            data = polling_engine.poll_results(results_paths, bucket, n_results, polling_interval=10)

            # Process and analyze data
            print("Processing and analyzing data...")
            raw_aggregated_data = self.data_processor.aggregate_data(data, analysis_type)
            
            analysis_result = self.statistical_analyzer.analyze_data(raw_aggregated_data, analysis_type)
            
            # Store the aggregated values in the centralized dict
            self._store_aggregated_values(analysis_type)
            
            return {
                'analysis_type': analysis_type,
                'result': analysis_result,
                'task_id': task_id,
                'tres_used': tres,
                'data_sources': len(data),
                'complete_query': complete_query
            }
            
        except Exception as e:
            print(f"Analysis failed: {str(e)}")
            raise
    

    
    def _collect_results(self, results_paths: List[str], bucket: str, n_results: int) -> List[str]:
        """
        Collect results from MinIO storage.
        
        Args:
            results_paths (List[str]): List of paths to collect results from
            bucket (str): MinIO bucket name
            n_results (int): Expected number of results
            
        Returns:
            List[str]: Collected data from all sources
        """
        data = []
        while len(data) < n_results:
            data = []
            for results_path in results_paths:
                result = self.minio_client.get_object(bucket, results_path)
                if result:
                    data.append(result)
            
            if len(data) < n_results:
                print(f"Waiting for results... ({len(data)}/{n_results} received)")
                time.sleep(10)
        
        print(f"{len(data)} results collected successfully")
        return data
    
    def get_analysis_requirements(self, analysis_type: str) -> dict:
        """
        Get the requirements for a specific analysis type.
        
        Args:
            analysis_type (str): Type of analysis
            
        Returns:
            dict: Requirements including expected columns and format
        """
        return self.query_builder.get_analysis_requirements(analysis_type)
    
    def get_supported_analysis_types(self) -> List[str]:
        """
        Get list of supported analysis types.
        
        Returns:
            List[str]: List of supported analysis types
        """
        return self.query_builder.get_supported_analysis_types()

    def run_additional_analysis(self, analysis_type: str) -> Union[float, Dict[str, Any]]:
        """
        Run an additional analysis on stored aggregated data.
        
        Args:
            analysis_type (str): Type of analysis to run
            
        Returns:
            Union[float, Dict[str, Any]]: Analysis result
            
        Raises:
            ValueError: If no data is stored or analysis is incompatible
        """
        if analysis_type not in self.statistical_analyzer.analysis_classes:
            raise ValueError(f"Unsupported analysis type: {analysis_type}")
        
        analysis_class = self.statistical_analyzer.analysis_classes[analysis_type]
        
        # Check if we have the required data for this analysis
        if not self._has_required_data(analysis_type):
            raise ValueError(f"Stored data is not compatible with {analysis_type} analysis")
        
        # Convert stored data to the format expected by the analyzer
        raw_data = self._convert_stored_data_to_raw(analysis_type)
        
        return self.statistical_analyzer.analyze_data(raw_data, analysis_type)
    
    def _has_required_data(self, analysis_type: str) -> bool:
        """
        Check if we have the required data for a given analysis type.
        
        Args:
            analysis_type (str): Type of analysis to check
            
        Returns:
            bool: True if we have the required data
        """
        if analysis_type not in self.statistical_analyzer.analysis_classes:
            return False
        
        # Get the return format keys from the analysis class
        analysis_class = self.statistical_analyzer.analysis_classes[analysis_type]
        keys = analysis_class.return_format.keys()
        
        # Check if we have all the required keys
        return all(key in self.aggregated_data for key in keys)
    
    def _convert_stored_data_to_raw(self, analysis_type: str) -> np.ndarray:
        """
        Convert stored data from the centralized dict to raw numpy array for analysis.
        
        Args:
            analysis_type (str): Type of analysis to run
            
        Returns:
            np.ndarray: Raw data array
        """
        analysis_class = self.statistical_analyzer.analysis_classes[analysis_type]
        keys = list(analysis_class.return_format.keys())
        
        # Check if this analysis expects a contingency table
        if "contingency_table" in analysis_class.return_format:
            if "contingency_table" in self.aggregated_data:
                return self.aggregated_data["contingency_table"]
        else:
            # For other analyses, get values in the order of return_format keys
            if all(key in self.aggregated_data for key in keys):
                values = [self.aggregated_data[key] for key in keys]
                return np.array([values])
        
        raise ValueError(f"No compatible stored data found for {analysis_type} analysis")
    
    def get_compatible_analyses(self) -> List[str]:
        """
        Get list of analyses that can be run on the currently stored data.
        
        Returns:
            List[str]: List of compatible analysis types
        """
        compatible = []
        
        # Check each analysis type to see if we have the required data
        for analysis_type in self.statistical_analyzer.get_supported_analysis_types():
            if self._has_required_data(analysis_type):
                compatible.append(analysis_type)
        
        return compatible

    def _store_aggregated_values(self, analysis_type: str):
        """
        Store the aggregated values in the centralized dict.
        
        Args:
            analysis_type (str): Type of analysis to store
        """
        analysis_class = self.statistical_analyzer.analysis_classes[analysis_type]
        
        # Store the aggregated values from the analysis class
        self.aggregated_data.update(analysis_class.aggregated_data)


# Example usage functions for common scenarios
def run_mean_analysis_example(engine: AnalysisEngine, concept_id: int, tres: List[str]) -> Dict[str, Any]:
    """
    Example function showing how to run a mean analysis.
    
    Args:
        engine (AnalysisEngine): Analysis engine instance
        concept_id (int): Measurement concept ID
        tres (List[str]): List of TREs
        
    Returns:
        Dict[str, Any]: Analysis results
    """
    user_query = f"""SELECT value_as_number FROM public.measurement 
WHERE measurement_concept_id = {concept_id}
AND value_as_number IS NOT NULL"""
    
    return engine.run_analysis("mean", user_query, tres, column="value_as_number")


def run_variance_analysis_example(engine: AnalysisEngine, concept_id: int, tres: List[str]) -> Dict[str, Any]:
    """
    Example function showing how to run a variance analysis.
    
    Args:
        engine (AnalysisEngine): Analysis engine instance
        concept_id (int): Measurement concept ID
        tres (List[str]): List of TREs
        
    Returns:
        Dict[str, Any]: Analysis results
    """
    user_query = f"""SELECT value_as_number FROM public.measurement 
WHERE measurement_concept_id = {concept_id}
AND value_as_number IS NOT NULL"""
    
    return engine.run_analysis("variance", user_query, tres, column="value_as_number")


def run_pmcc_analysis_example(engine: AnalysisEngine, x_concept_id: int, y_concept_id: int, tres: List[str]) -> Dict[str, Any]:
    """
    Example function showing how to run a PMCC analysis.
    
    Args:
        engine (AnalysisEngine): Analysis engine instance
        x_concept_id (int): First measurement concept ID
        y_concept_id (int): Second measurement concept ID
        tres (List[str]): List of TREs
        
    Returns:
        Dict[str, Any]: Analysis results
    """
    user_query = f"""WITH x_values AS (
  SELECT person_id, measurement_date, value_as_number AS x
  FROM public.measurement
  WHERE measurement_concept_id = {x_concept_id}
    AND value_as_number IS NOT NULL
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
    
    return engine.run_analysis("PMCC", user_query, tres, x_column="x", y_column="y")


def run_chi_squared_analysis_example(engine: AnalysisEngine, tres: List[str]) -> Dict[str, Any]:
    """
    Example function showing how to run a chi-squared analysis.
    
    Args:
        engine (AnalysisEngine): Analysis engine instance
        tres (List[str]): List of TREs
        
    Returns:
        Dict[str, Any]: Analysis results
    """
    user_query = """SELECT 
  g.concept_name AS gender_name,
  r.concept_name AS race_name
FROM person p
JOIN concept g ON p.gender_concept_id = g.concept_id
JOIN concept r ON p.race_concept_id = r.concept_id
WHERE p.race_concept_id IN (38003574, 38003584)"""
    
    return engine.run_analysis("chi_squared_scipy", user_query, tres, group_columns="gender_name, race_name")


if __name__ == "__main__":
    # Example usage

    engine = AnalysisEngine()  # Will use TRE_FX_PROJECT from environment and TRE_FX_TOKEN from environment
    
    # Example: Run variance analysis first, then mean analysis on the same data
    user_query = """SELECT value_as_number FROM public.measurement 
WHERE measurement_concept_id = 3037532
AND value_as_number IS NOT NULL"""
    
    print("Running mean analysis...")
    mean_result = engine.run_analysis(
        analysis_type="mean",
        task_name="DEMO: mean analysis test",
        user_query=user_query,
        tres=["Nottingham", "Nottingham 2"]
    )
    
    print(f"Mean analysis result: {mean_result['result']}")
    
    
    # Show what aggregated data we have stored
    print(f"Stored aggregated data: {engine.aggregated_data}")











































    # Check what other analyses we can run on this data
    #compatible_analyses = engine.get_compatible_analyses()
    #print(f"Compatible analyses: {compatible_analyses}")
    
    # Run mean analysis on the same stored data (no need to re-query TREs)
    #print("Running mean analysis on stored data...")
    #mean_result = engine.run_additional_analysis("mean")
    #print(f"Mean analysis result: {mean_result}")
