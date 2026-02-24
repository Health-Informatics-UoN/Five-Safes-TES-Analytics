from abc import ABC, abstractmethod
from sqlalchemy import create_engine, text
from tdigest import TDigest
import math
import json
from local_processing_base import BaseLocalProcessing

class Mean(BaseLocalProcessing):
    """
    Calculate the mean of a numeric column using SQL aggregation.
    
    Returns aggregated statistics (count and sum) that can be used to compute
    the mean across multiple TREs. The mean is calculated as sum/n on the client side.
    """
    analysis_type = "mean"

    @property
    def description(self):
        return "Calculate mean of a numeric column"

    @property
    def processing_query(self):
        return """
SELECT
  COUNT(*) AS n,
  SUM(value_as_number) AS total
FROM user_query;"""



    @property
    def user_query_requirements(self):
        return "Must select a single numeric column"

class Variance(BaseLocalProcessing):
    """
    Calculate the variance of a numeric column using SQL aggregation.
    
    Returns aggregated statistics (count, sum, and sum of squares) that can be used
    to compute variance across multiple TREs using the formula: Var = (sum_x2/n) - (sum/n)²
    """
    analysis_type = "variance"

    @property
    def description(self):
        return "Calculate variance of a numeric column"

    @property
    def processing_query(self):
        return """
SELECT
  COUNT(*) AS n,
  SUM(value_as_number * value_as_number) AS sum_x2,
  SUM(value_as_number) AS total
FROM user_query;"""



    @property
    def user_query_requirements(self):
        return "Must select a single numeric column"

class PMCC(BaseLocalProcessing):
    """
    Calculate Pearson's correlation coefficient between two numeric columns.
    
    Returns aggregated statistics (count, sums, and cross-products) needed to compute
    PMCC across multiple TREs. The correlation is calculated on the client side after
    aggregating results from all TREs.
    """
    analysis_type = "PMCC"

    @property
    def description(self):
        return "Calculate Pearson's correlation coefficient between two numeric columns"

    @property
    def processing_query(self):
        return """
SELECT
  COUNT(*) AS n,
  SUM(x) AS sum_x,
  SUM(y) AS sum_y,
  SUM(x * x) AS sum_x2,
  SUM(y * y) AS sum_y2,
  SUM(x * y) AS sum_xy
FROM user_query;"""



    @property
    def user_query_requirements(self):
        return "Must select exactly two numeric columns (x and y)"


class ContingencyTable(BaseLocalProcessing):
    """
    Build a contingency table from one or more categorical columns.
    
    Dynamically detects columns from the user query and groups by them to count
    occurrences. Returns raw counts for each combination of categorical values,
    which can be aggregated across multiple TREs.
    """
    analysis_type = "contingency_table"

    @property
    def description(self):
        return "Build a contingency table from one or more categorical columns"

    def get_columns_from_user_query(self):
        # Naively append LIMIT 0 if not present
        query = self.user_query.strip().rstrip(';')
        if 'limit' not in query.lower():
            query += ' LIMIT 0'
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            columns = result.keys()
        if not columns:
            raise ValueError("No columns found in user query.")
        return list(columns)

    @property
    def processing_query(self):
        categorical_columns = self.get_columns_from_user_query()
        group_by = ", ".join(categorical_columns)
        select = ", ".join(categorical_columns)
        query = f"""
SELECT
  {select},
  COUNT(*) AS n
FROM user_query
GROUP BY {group_by}
ORDER BY {group_by};"""
        return query



    @property
    def user_query_requirements(self):
        return "Must select one or more categorical columns"

class PercentileSketch(BaseLocalProcessing):
    """
    Calculate percentile sketch using TDigest algorithm.
    
    Uses TDigest (https://github.com/CamDavidsonPilon/tdigest) to create a compact
    representation of the data distribution. The sketch is computed in Python after
    fetching all data from SQL, and returns a TDigest dictionary that can be merged
    across multiple TREs.
    """
    analysis_type = "percentile_sketch"
        
    @property
    def description(self):
        return "Calculate percentile sketch of a numeric column"
        
    @property
    def processing_query(self):
        return None
    

    
    @property
    def user_query_requirements(self):
        return "Must select a numeric column"

    def python_analysis(self, sql_result):
        tdigest = TDigest()
        for row in sql_result.fetchall():
            ## need to filter out missing values, null or NaN. If it's missing, it should only be None, but it's technically possible for NaN to be returned.
            if row[0] is not None and not math.isnan(row[0]):
                tdigest.update(row[0])
        return tdigest.to_dict()  # Return dict, not JSON string - json.dump will handle serialization


def get_local_processing_registry():
    registry = {}
    for cls in BaseLocalProcessing.__subclasses__():
        if hasattr(cls, "analysis_type"):
            registry[cls.analysis_type] = cls
    return registry

LOCAL_PROCESSING_CLASSES = get_local_processing_registry()

