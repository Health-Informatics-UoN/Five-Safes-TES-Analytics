from abc import ABC, abstractmethod
from sqlalchemy import create_engine, text
from tdigest import TDigest
import math

def get_local_processing_registry():
    registry = {}
    for cls in BaseLocalProcessing.__subclasses__():
        if hasattr(cls, "analysis_type"):
            registry[cls.analysis_type] = cls
    return registry

LOCAL_PROCESSING_CLASSES = get_local_processing_registry()
    

class BaseLocalProcessing(ABC):
    def __init__(self, analysis_type: str = None, user_query: str = None, engine = None):
        self.analysis_type = analysis_type
        self.user_query = user_query
        self.engine = engine

    @property
    @abstractmethod
    def description(self):
        """Description of the processing step."""
        pass

    @property
    @abstractmethod
    def processing_query(self):
        """SQL fragment for the processing step."""
        pass

    @property
    @abstractmethod
    def expected_columns(self):
        """List of expected columns in the result."""
        pass

    @property
    @abstractmethod
    def user_query_requirements(self):
        """Requirements for the user query."""
        pass

    def build_query(self) -> str:
        """
        Build a complete SQL query by combining user's data selection with analysis calculations.
        """
        if self.analysis_type is None or self.processing_query is None:
            return self.user_query
        # Check if the analysis_type is supported
        if self.analysis_type not in LOCAL_PROCESSING_CLASSES:
            raise ValueError(f"Unsupported analysis type: {self.analysis_type}")
        # Combine user query with analysis part
        query = f"""WITH user_query AS (
{self.user_query}
)
{self.processing_query}"""
        return query

    def python_analysis(self, sql_result):
        """
        Optional Python-side analysis. Override in subclasses if needed.
        By default, does nothing and returns None.
        """
        return None

class Mean(BaseLocalProcessing):
    analysis_type = "mean"
    def __init__(self, user_query=None):
        super().__init__(analysis_type=self.analysis_type, user_query=user_query)

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
    def expected_columns(self):
        return ["n", "total"]

    @property
    def user_query_requirements(self):
        return "Must select a single numeric column"

class Variance(BaseLocalProcessing):
    analysis_type = "variance"
    def __init__(self, user_query=None):
        super().__init__(analysis_type=self.analysis_type, user_query=user_query)

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
    def expected_columns(self):
        return ["n", "sum_x2", "total"]

    @property
    def user_query_requirements(self):
        return "Must select a single numeric column"

class PMCC(BaseLocalProcessing):
    analysis_type = "PMCC"
    def __init__(self, user_query=None):
        super().__init__(analysis_type=self.analysis_type, user_query=user_query)

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
    def expected_columns(self):
        return ["n", "sum_x", "sum_y", "sum_x2", "sum_y2", "sum_xy"]

    @property
    def user_query_requirements(self):
        return "Must select exactly two numeric columns (x and y)"


class ContingencyTable(BaseLocalProcessing):
    analysis_type = "contingency_table"
    def __init__(self, user_query=None, engine=None):
        super().__init__(analysis_type=self.analysis_type, user_query=user_query, engine = engine)

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
    def expected_columns(self):
        return self.get_columns_from_user_query() + ["n"]

    @property
    def user_query_requirements(self):
        return "Must select one or more categorical columns"

class PercentileSketch(BaseLocalProcessing):
    analysis_type = "percentile_sketch" ## might be better to call it something to do with digests, or the specific percentile sketch algorithm.
    ## see here: https://github.com/CamDavidsonPilon/tdigest
    def __init__(self, user_query=None, engine=None):
        super().__init__(analysis_type=self.analysis_type, user_query=user_query, engine=engine)
        
    @property
    def description(self):
        return "Calculate percentile sketch of a numeric column"
        
    @property
    def processing_query(self):
        return None
    
    @property
    def expected_columns(self):
        return None
    
    @property
    def user_query_requirements(self):
        return "Must select a numeric column"

    def python_analysis(self, sql_result):
        tdigest = TDigest()
        for row in sql_result:
            ## need to filter out missing values, null or NaN. If it's missing, it should only be None, but it's technically possible for NaN to be returned.
            if row[0] is not None and not math.isnan(row[0]):
                tdigest.update(row[0])
        return tdigest.to_json()

