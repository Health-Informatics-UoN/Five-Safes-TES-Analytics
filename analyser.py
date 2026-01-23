from analysis_engine import AnalysisEngine
from typing import List, Dict, Any, Optional, Union
from analytics_tes import AnalyticsTES
from data_processor import DataProcessor
from statistical_analyzer import StatisticalAnalyzer
import numpy as np
import os
from string import Template


class Analyser:
    def __init__(self, analysis_engine: AnalysisEngine):
        self.analysis_engine = analysis_engine
        self.tes_client = analysis_engine.tes_client
        # Own instances for aggregation and analysis
        self.data_processor = DataProcessor()
        self.statistical_analyzer = StatisticalAnalyzer()
        # Storage for aggregated values
        self.aggregated_data = {}
    
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

        task_name, bucket, tres = self.analysis_engine.setup_analysis(analysis_type, task_name, bucket, tres)
        
        # Check if we should run on existing data (returns early if so)
        existing_data_result = self.check_analysis_on_existing_data(analysis_type, user_query, tres)
        if existing_data_result is not None:
            return existing_data_result
        
        ### create the TES message for the analysis
        analytics_tes = self.analysis_engine.tes_client
        analytics_tes.set_tes_messages(query=user_query, analysis_type=analysis_type, name=task_name, output_format="json")
        analytics_tes.set_tags(tres=self.analysis_engine.tres)
        five_Safes_TES_message = analytics_tes.create_FiveSAFES_TES_message()
                

        # Submit task and collect results (common workflow)
        try:
            task_id, data = self.analysis_engine._submit_and_collect_results(
                five_Safes_TES_message,
                bucket,
                output_format="json",
                submit_message=f"Submitting {analysis_type} analysis to {len(self.analysis_engine.tres)} TREs..."
            )

            # Process and analyze data (aggregation moved to this class)
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
                'complete_query': user_query
            }
            
        except Exception as e:
            print(f"Analysis failed: {str(e)}")
            raise
    
    def _store_aggregated_values(self, analysis_type: str):
        """
        Store the aggregated values in the centralized dict.
        
        Args:
            analysis_type (str): Type of analysis to store
        """
        analysis_class = self.statistical_analyzer.analysis_classes[analysis_type]
        
        # Store the aggregated values from the analysis class
        self.aggregated_data.update(analysis_class.aggregated_data)
    
    def check_analysis_on_existing_data(self, analysis_type: str, user_query: str = None, tres: List[str] = None) -> Optional[Dict[str, Any]]:
        """
        Check if analysis should run on existing data, and run it if so.
        
        Args:
            analysis_type (str): Type of analysis to perform
            user_query (str, optional): User's data selection query
            tres (List[str], optional): List of TREs to run analysis on
            
        Returns:
            Optional[Dict[str, Any]]: Analysis results if running on existing data, None otherwise
        """
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
        return None
    
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
    
    def get_analysis_requirements(self, analysis_type: str) -> dict:
        """
        Get the requirements for a specific analysis type.
        
        Args:
            analysis_type (str): Type of analysis
            
        Returns:
            dict: Requirements including expected columns and format
        """
        return self.statistical_analyzer.get_analysis_config(analysis_type)
    
    def get_supported_analysis_types(self) -> List[str]:
        """
        Get list of supported analysis types.
        
        Returns:
            List[str]: List of supported analysis types
        """
        return self.statistical_analyzer.get_supported_analysis_types()


# Example usage functions for common scenarios
def run_mean_analysis_example(analyser: Analyser, concept_id: int, tres: List[str] = None) -> Dict[str, Any]:
    """
    Example function showing how to run a mean analysis.
    
    Args:
        analyser (Analyser): Analyser instance
        concept_id (int): Measurement concept ID
        tres (List[str], optional): List of TREs
        
    Returns:
        Dict[str, Any]: Analysis results
    """
    sql_schema = os.getenv("SQL_SCHEMA", "public")
    query_template = Template("""SELECT value_as_number FROM $schema.measurement 
WHERE measurement_concept_id = $concept_id
AND value_as_number IS NOT NULL""")
    user_query = query_template.safe_substitute(schema=sql_schema, concept_id=concept_id)
    return analyser.run_analysis("mean", user_query=user_query, tres=tres)


def run_variance_analysis_example(analyser: Analyser, concept_id: int, tres: List[str] = None) -> Dict[str, Any]:
    """
    Example function showing how to run a variance analysis.
    
    Args:
        analyser (Analyser): Analyser instance
        concept_id (int): Measurement concept ID
        tres (List[str], optional): List of TREs
        
    Returns:
        Dict[str, Any]: Analysis results
    """
    sql_schema = os.getenv("SQL_SCHEMA", "public")
    query_template = Template("""SELECT value_as_number FROM $schema.measurement 
WHERE measurement_concept_id = $concept_id
AND value_as_number IS NOT NULL""")
    user_query = query_template.safe_substitute(schema=sql_schema, concept_id=concept_id)
    return analyser.run_analysis("variance", user_query=user_query, tres=tres)


def run_pmcc_analysis_example(analyser: Analyser, x_concept_id: int, y_concept_id: int, tres: List[str] = None) -> Dict[str, Any]:
    """
    Example function showing how to run a PMCC analysis.
    
    Args:
        analyser (Analyser): Analyser instance
        x_concept_id (int): First measurement concept ID
        y_concept_id (int): Second measurement concept ID
        tres (List[str], optional): List of TREs
        
    Returns:
        Dict[str, Any]: Analysis results
    """
    sql_schema = os.getenv("SQL_SCHEMA", "public")
    query_template = Template("""WITH x_values AS (
  SELECT person_id, measurement_date, value_as_number AS x
  FROM $schema.measurement
  WHERE measurement_concept_id = $x_concept_id
    AND value_as_number IS NOT NULL
),
y_values AS (
  SELECT person_id, measurement_date, value_as_number AS y
  FROM $schema.measurement
  WHERE measurement_concept_id = $y_concept_id
    AND value_as_number IS NOT NULL
)
SELECT
  x.x,
  y.y
FROM x_values x
INNER JOIN y_values y
  ON x.person_id = y.person_id
  AND x.measurement_date = y.measurement_date""")
    user_query = query_template.safe_substitute(schema=sql_schema, x_concept_id=x_concept_id, y_concept_id=y_concept_id)
    return analyser.run_analysis("PMCC", user_query=user_query, tres=tres)


def run_chi_squared_analysis_example(analyser: Analyser, tres: List[str] = None) -> Dict[str, Any]:
    """
    Example function showing how to run a chi-squared analysis.
    
    Args:
        analyser (Analyser): Analyser instance
        tres (List[str], optional): List of TREs
        
    Returns:
        Dict[str, Any]: Analysis results
    """
    sql_schema = os.getenv("SQL_SCHEMA", "public")
    query_template = Template("""SELECT 
  g.concept_name AS gender_name,
  r.concept_name AS race_name
FROM $schema.person p
JOIN $schema.concept g ON p.gender_concept_id = g.concept_id
JOIN $schema.concept r ON p.race_concept_id = r.concept_id
WHERE p.race_concept_id IN (38003574, 38003584)""")
    
    user_query = query_template.safe_substitute(schema=sql_schema)
    return analyser.run_analysis("chi_squared_scipy", user_query=user_query, tres=tres)


# Example usage
if __name__ == "__main__":
    from string import Template
    
    # Will use 5STES_PROJECT from environment and 5STES_TOKEN from environment
    analytics_tes = AnalyticsTES()
    engine = AnalysisEngine(tes_client=analytics_tes) 
    analyser = Analyser(engine)
    sql_schema = os.getenv("SQL_SCHEMA", "public")
    
    # Example: Run variance analysis first, then mean analysis on the same data
    query_template = Template("""SELECT value_as_number FROM $schema.measurement 
WHERE measurement_concept_id = 21490742
AND value_as_number IS NOT NULL""")
    
    user_query = query_template.safe_substitute(schema=sql_schema)
    
    print("Running mean analysis...")
    mean_result = analyser.run_analysis(
        analysis_type="mean",
        task_name="DEMO: mean analysis test",
        user_query=user_query,
    )
    
    # Show what aggregated data we have stored
    print(f"Mean analysis result: {mean_result['result']}")
    print(f"Stored aggregated data: {analyser.aggregated_data}")
