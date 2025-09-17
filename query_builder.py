from typing import Dict, Any, List, Optional


class QueryBuilder:
    """
    Builds SQL queries for federated analysis.
    Concatenates user data selection with statistical analysis calculations.
    """

    def __init__(self):
        """Initialize the query builder."""
        self.analysis_templates = {
            "mean": {
                "description": "Calculate mean of a numeric column",
                "user_query_requirements": "Must select a single numeric column",
                "analysis_part": """
SELECT
  COUNT(*) AS n,
  SUM(value_as_number) AS total
FROM user_query;""",
                "expected_columns": ["n", "total"],
            },
            "variance": {
                "description": "Calculate variance of a numeric column",
                "user_query_requirements": "Must select a single numeric column",
                "analysis_part": """
SELECT
  COUNT(*) AS n,
  SUM(value_as_number * value_as_number) AS sum_x2,
  SUM(value_as_number) AS total
FROM user_query;""",
                "expected_columns": ["n", "sum_x2", "total"],
            },
            "PMCC": {
                "description": "Calculate Pearson's correlation coefficient between two numeric columns",
                "user_query_requirements": "Must select exactly two numeric columns (x and y)",
                "analysis_part": """
SELECT
  COUNT(*) AS n,
  SUM(x) AS sum_x,
  SUM(y) AS sum_y,
  SUM(x * x) AS sum_x2,
  SUM(y * y) AS sum_y2,
  SUM(x * y) AS sum_xy
FROM user_query;""",
                "expected_columns": [
                    "n",
                    "sum_x",
                    "sum_y",
                    "sum_x2",
                    "sum_y2",
                    "sum_xy",
                ],
            },
            "chi_squared_scipy": {
                "description": "Chi-squared test using scipy (contingency table)",
                "user_query_requirements": "Must select categorical columns that will form a contingency table",
                "analysis_part": """
SELECT 
  category1,
  category2,
  COUNT(*) AS n
FROM user_query
GROUP BY category1, category2
ORDER BY category1, category2;""",
                "expected_columns": ["contingency_table"],
            },
            "chi_squared_manual": {
                "description": "Chi-squared test with manual calculation (contingency table)",
                "user_query_requirements": "Must select categorical columns that will form a contingency table",
                "analysis_part": """
SELECT 
  category1,
  category2,
  COUNT(*) AS n
FROM user_query
GROUP BY category1, category2
ORDER BY category1, category2;""",
                "expected_columns": ["contingency_table"],
            },
        }

    def build_query(self, analysis_type: str, user_query: Optional[str]) -> str:
        """
        Build a complete SQL query by combining user's data selection with analysis calculations.

        Args:
            analysis_type (str): Type of analysis to perform
            user_query (str): User's data selection query (without analysis calculations)

        Returns:
            str: Complete SQL query with analysis calculations

        Raises:
            ValueError: If analysis type is not supported
        """
        if analysis_type not in self.analysis_templates:
            raise ValueError(f"Unsupported analysis type: {analysis_type}")

        template = self.analysis_templates[analysis_type]
        analysis_part = template["analysis_part"]

        # Combine user query with analysis part
        complete_query = f"""WITH user_query AS (
{user_query}
)
{analysis_part}"""

        return complete_query

    def get_analysis_requirements(self, analysis_type: str) -> Dict[str, Any]:
        """
        Get the requirements for a specific analysis type.

        Args:
            analysis_type (str): Type of analysis

        Returns:
            Dict[str, Any]: Requirements including expected columns and format
        """
        if analysis_type not in self.analysis_templates:
            raise ValueError(f"Unsupported analysis type: {analysis_type}")

        template = self.analysis_templates[analysis_type]
        return {
            "description": template["description"],
            "user_query_requirements": template["user_query_requirements"],
            "expected_columns": template["expected_columns"],
        }

    def validate_query(self, query: str) -> bool:
        """
        Basic validation of SQL query structure.

        Args:
            query (str): SQL query to validate

        Returns:
            bool: True if query appears valid
        """
        # Basic checks
        if not query or not query.strip():
            return False

        # Check for basic SQL keywords
        query_upper = query.upper()
        if "SELECT" not in query_upper:
            return False

        # Check for balanced parentheses
        if query.count("(") != query.count(")"):
            return False

        # Check for semicolon at the end
        if not query.strip().endswith(";"):
            return False

        return True

    def get_supported_analysis_types(self) -> List[str]:
        """
        Get list of supported analysis types.

        Returns:
            List[str]: List of supported analysis types
        """
        return list(self.analysis_templates.keys())


