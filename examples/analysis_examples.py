from typing import List, Any, Dict

from analysis_engine import AnalysisEngine

# Example usage functions for common scenarios
def run_mean_analysis_example(
    engine: AnalysisEngine, concept_id: int, tres: List[str]
) -> Dict[str, Any]:
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

    return engine.run_analysis(
        "mean",
        user_query,
        tres,
    )


def run_variance_analysis_example(
    engine: AnalysisEngine, concept_id: int, tres: List[str]
) -> Dict[str, Any]:
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

    return engine.run_analysis(
        "variance",
        user_query,
        tres,
    )


def run_pmcc_analysis_example(
    engine: AnalysisEngine, x_concept_id: int, y_concept_id: int, tres: List[str]
) -> Dict[str, Any]:
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

    return engine.run_analysis(
        "PMCC",
        user_query,
        tres,
    )


def run_chi_squared_analysis_example(
    engine: AnalysisEngine, tres: List[str]
) -> Dict[str, Any]:
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

    return engine.run_analysis(
        "chi_squared_scipy",
        user_query,
        tres,
    )


if __name__ == "__main__":
    # Example usage

    engine = (
        AnalysisEngine()
    )  # Will use TRE_FX_PROJECT from environment and TRE_FX_TOKEN from environment

    # Example: Run variance analysis first, then mean analysis on the same data
    user_query = """SELECT value_as_number FROM public.measurement 
WHERE measurement_concept_id = 3037532
AND value_as_number IS NOT NULL"""

    print("Running mean analysis...")
    mean_result = engine.run_analysis(
        analysis_type="mean",
        task_name="DEMO: mean analysis test",
        user_query=user_query,
    )

    print(f"Mean analysis result: {mean_result['result']}")

    # Show what aggregated data we have stored
    print(f"Stored aggregated data: {engine.aggregated_data}")

    # Check what other analyses we can run on this data
    # compatible_analyses = engine.get_compatible_analyses()
    # print(f"Compatible analyses: {compatible_analyses}")

    # Run mean analysis on the same stored data (no need to re-query TREs)
    # print("Running mean analysis on stored data...")
    # mean_result = engine.run_additional_analysis("mean")
   # print(f"Mean analysis result: {mean_result}")
