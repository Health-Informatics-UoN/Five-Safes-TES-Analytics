# Example usage functions for common scenarios
def run_mean_analysis_example(analysis_runner: AnalysisRunner, concept_id: int, tres: List[str] = None) -> Dict[str, Any]:
    """
    Example function showing how to run a mean analysis.
    
    Args:
        analysis_runner (AnalysisRunner): AnalysisRunner instance
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
    return analysis_runner.run_analysis("mean", user_query=user_query, tres=tres)


def run_variance_analysis_example(analysis_runner: AnalysisRunner, concept_id: int, tres: List[str] = None) -> Dict[str, Any]:
    """
    Example function showing how to run a variance analysis.
    
    Args:
        analysis_runner (AnalysisRunner): AnalysisRunner instance
        concept_id (int): Measurement concept ID
        tres (List[str], optional): List of TREs
        
    Returns:
        Dict[str, Any]: Analysis results
    """
    sql_schema = os.getenv("postgresSchema", "public")
    query_template = Template("""SELECT value_as_number FROM $schema.measurement 
WHERE measurement_concept_id = $concept_id
AND value_as_number IS NOT NULL""")
    user_query = query_template.safe_substitute(schema=sql_schema, concept_id=concept_id)
    return analysis_runner.run_analysis("variance", user_query=user_query, tres=tres)


def run_pmcc_analysis_example(analysis_runner: AnalysisRunner, x_concept_id: int, y_concept_id: int, tres: List[str] = None) -> Dict[str, Any]:
    """
    Example function showing how to run a PMCC analysis.
    
    Args:
        analysis_runner (AnalysisRunner): AnalysisRunner instance
        x_concept_id (int): First measurement concept ID
        y_concept_id (int): Second measurement concept ID
        tres (List[str], optional): List of TREs
        
    Returns:
        Dict[str, Any]: Analysis results
    """
    sql_schema = os.getenv("postgresSchema", "public")
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
    return analysis_runner.run_analysis("PMCC", user_query=user_query, tres=tres)


def run_chi_squared_analysis_example(analysis_runner: AnalysisRunner, tres: List[str] = None) -> Dict[str, Any]:
    """
    Example function showing how to run a chi-squared analysis.
    
    Args:
        analysis_runner (AnalysisRunner): AnalysisRunner instance
        tres (List[str], optional): List of TREs
        
    Returns:
        Dict[str, Any]: Analysis results
    """
    sql_schema = os.getenv("postgresSchema", "public")
    query_template = Template("""SELECT 
  g.concept_name AS gender_name,
  r.concept_name AS race_name
FROM $schema.person p
JOIN $schema.concept g ON p.gender_concept_id = g.concept_id
JOIN $schema.concept r ON p.race_concept_id = r.concept_id
WHERE p.race_concept_id IN (38003574, 38003584)""")
    
    user_query = query_template.safe_substitute(schema=sql_schema)
    return analysis_runner.run_analysis("chi_squared_scipy", user_query=user_query, tres=tres)