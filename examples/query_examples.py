from query_builder import QueryBuilder

# Example usage functions
def build_mean_query_example(user_query: str, column: str) -> str:
    """
    Example function showing how to build a mean analysis query.

    Args:
        user_query (str): User's data selection query
        column (str): Column name to calculate mean for

    Returns:
        str: Complete SQL query
    """
    builder = QueryBuilder()
    return builder.build_query("mean", user_query)


def build_variance_query_example(user_query: str, column: str) -> str:
    """
    Example function showing how to build a variance analysis query.

    Args:
        user_query (str): User's data selection query
        column (str): Column name to calculate variance for

    Returns:
        str: Complete SQL query
    """
    builder = QueryBuilder()
    return builder.build_query("variance", user_query)


def build_pmcc_query_example(user_query: str, x_column: str, y_column: str) -> str:
    """
    Example function showing how to build a PMCC analysis query.

    Args:
        user_query (str): User's data selection query
        x_column (str): First column name
        y_column (str): Second column name

    Returns:
        str: Complete SQL query
    """
    builder = QueryBuilder()
    return builder.build_query("PMCC", user_query)


def build_chi_squared_query_example(user_query: str, group_columns: str) -> str:
    """
    Example function showing how to build a chi-squared analysis query.

    Args:
        user_query (str): User's data selection query
        group_columns (str): Columns to group by (comma-separated)

    Returns:
        str: Complete SQL query
    """
    builder = QueryBuilder()
    return builder.build_query("chi_squared_scipy", user_query)
