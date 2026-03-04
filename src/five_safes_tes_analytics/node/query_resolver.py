from sqlalchemy import create_engine, text, engine
from decimal import Decimal
import json
import click
import sys
import re
import os
from urllib.parse import quote_plus

from . import local_processing


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

def validate_environment():
    """Validate environment variables for database connection."""
    required = ['postgresUsername', 'postgresPassword', 'postgresServer', 'postgresDatabase']
    for required_var in required:
        if not os.getenv(required_var) or os.getenv(required_var) == '':
            raise ValueError(f"Missing required env var: {required_var}. Set it or pass --db-connection.")
def parse_connection_string(connection_string: str = None) -> str:
    """
    Parse and convert a connection string to SQLAlchemy format.
    
    Handles two formats:
    1. Semicolon-separated: "Host=host:port;Username=user;Password=pass;Database=db"
    2. Already in SQLAlchemy format: "postgresql://user:pass@host:port/db"
    
    Args:
        connection_string: Connection string in either format
        
    Returns:
        Connection string in SQLAlchemy format: "postgresql://user:pass@host:port/db"
    """
    if connection_string is None:
        username = quote_plus(os.getenv('postgresUsername'))
        password = quote_plus(os.getenv('postgresPassword'))
        host = os.getenv('postgresServer')
        port = os.getenv('postgresPort') or '5432'
        database = os.getenv('postgresDatabase')
        connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        
        return connection_string
    
    # Override from command line: value is passed by Click without the option name.
    connection_string = connection_string.strip()
    # If already in SQLAlchemy format (starts with a database URI scheme), return as-is
    if re.match(r'^[a-zA-Z][a-zA-Z0-9+.-]*://', connection_string):
        return connection_string
    else:
        return parse_semicolon_format_connection_string(connection_string)
    
def parse_semicolon_format_connection_string(connection_string: str) -> str:
    """
    Parse semicolon-separated (.NET style) format connection string: Host=host:port;Username=user;Password=pass;Database=db
    and return a SQLAlchemy connection string.
    Args:
        connection_string: Connection string in semicolon-separated format
        
    Returns:
        Connection string in SQLAlchemy format: "postgresql://user:pass@host:port/db"
    """
    
    params = {}
    parts = connection_string.split(';')
    
    for part in parts:
        part = part.strip()
        if not part:
            continue
        
        if '=' not in part:
            continue
            
        key, value = part.split('=', 1)
        key = key.strip().lower()
        value = value.strip()
        
        if key == 'host':
            # Host might include port: "host:port"
            if ':' in value:
                params['host'], params['port'] = value.rsplit(':', 1)
            else:
                params['host'] = value
                params['port'] = '5432'  # Default PostgreSQL port
        elif key == 'username':
            params['username'] = value
        elif key == 'password':
            params['password'] = value
        elif key == 'database':
            params['database'] = value
        elif key == 'port':
            # Handle explicit Port parameter if Host doesn't include it
            params['port'] = value
    
    # Validate required parameters
    required = ['host', 'username', 'password', 'database']
    for param in required:
        if param not in params:
            raise ValueError(f"Missing required connection parameter: {param}")
    missing = [p for p in required if not params.get(p)]
    if missing:
        raise ValueError(f"Missing required connection parameters: {', '.join(missing)}")
    
    # Set default port if not specified
    if 'port' not in params:
        params['port'] = '5432'
    
    # URL-encode username and password to handle special characters
    username = quote_plus(params['username'])
    password = quote_plus(params['password'])
    
    # Construct SQLAlchemy connection string
    # Format: postgresql://username:password@host:port/database
    sqlalchemy_string = f"postgresql://{username}:{password}@{params['host']}:{params['port']}/{params['database']}"
    
    return sqlalchemy_string

def process_query(user_query, analysis, db_connection, output_filename, output_format):
    """Run the given analysis (SQL + optional Python) against the DB and write results to file.

    Args:
        user_query: SQL query or user selection (used by the analysis processor to build the final query).
        analysis: Analysis type (e.g. 'mean', 'variance'); must exist in LOCAL_PROCESSING_CLASSES.
        db_connection: Database connection string (SQLAlchemy URL, semicolon format, or None to use env vars).
        output_filename: Base name for the output file (extension added from output_format).
        output_format: Output format ('json' or 'csv').

    Returns:
        None. Writes result to disk. Exits with code 1 on error.
    """
    
    #### Setup
    try:    
        output_filename = output_filename + '.' + output_format
        
        # Parse and convert connection string to SQLAlchemy format if needed
        sqlalchemy_connection = parse_connection_string(db_connection)
        
        ## sample connection string: "postgresql://postgres:postgres@localhost:5432/postgres"
        sql_engine = create_engine(sqlalchemy_connection)

        registry = local_processing.LOCAL_PROCESSING_CLASSES
        if analysis not in registry:
            ## This is temporary - we'll have to send the error to the client.
            raise ValueError(f"Unsupported analysis type: {analysis}")

        processor = registry[analysis](user_query=user_query, engine=sql_engine)


        query = processor.build_query()

        ## execute query
        with sql_engine.connect() as conn:
            db_result = conn.execute(text(query))
            ## deliberately not fetching the data, so we can do python analysis on it in case it is huge.
            


        ### check if we need to do any python analysis
        python_post_query_hook = processor.python_analysis(db_result)

        if python_post_query_hook is not None:
            result = python_post_query_hook
        else:
            result = db_result

        if isinstance(result, engine.Result):
            # Store the keys before calling fetchall
            result_keys = result.keys()
            result = result.fetchall()
        else:
            result_keys = None

        ## convert to list of dictionaries, so it's easier to work with.

        if result and result_keys:
            # Convert list of tuples to list of dictionaries
            if len(result) == 1:
                # Single row - convert to single dict (for aggregations like mean, variance)
                row_values = result[0]
                result = dict(zip(result_keys, row_values))
            else:
                # Multiple rows - convert to list of dicts (for contingency tables, etc.)
                result = [dict(zip(result_keys, row)) for row in result]

        ## decide to output as json to make it easier to work with, but could also output as csv, or just the result.
        ### Write json to string for debugging
        # json_str = json.dumps(result, cls=DecimalEncoder)

        with open(output_filename, 'w') as f:
            (json.dump(result, f, cls=DecimalEncoder))

        ## if we want csv output instead, we can do it like this:
        #if result:
        #    fieldnames = result[0].keys()
        #else:
        #    fieldnames = []

        #output = io.StringIO()
        #writer = csv.DictWriter(output, fieldnames=fieldnames)
        #writer.writeheader()
        #writer.writerows(result)
        #csv_str = output.getvalue()
        #output.close()

        #print(csv_str)


    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@click.command()
@click.option('--user-query', required=True, help='SQL query to execute')
@click.option('--analysis', required=True, help='Type of analysis to perform')
@click.option('--db-connection', default=None, 
                       help='Database connection string. If not provided, will use environment variables.')
@click.option('--output-filename', help='Output filename', default='output')
@click.option('--output-format', type=click.Choice(['json', 'csv']), default='json',
                       help='Output format (json or csv)')
def main(user_query, analysis, db_connection, output_filename, output_format):
    """Click command wrapper for process_query."""
    if db_connection is None:
        validate_environment()
    process_query(user_query, analysis, db_connection, output_filename, output_format)




if __name__ == "__main__":
    import sys
    if len(sys.argv) == 1:  # No command line arguments
        # Use test values
        user_query = """
  SELECT value_as_number
  FROM measurement
  WHERE measurement_concept_id = 21490742
    AND value_as_number IS NOT NULL
"""
        analysis = 'mean'
        db_connection = "postgresql://postgres:postgres@localhost:5432/omop"
        output_filename = 'data'
        output_format = 'json'
        main(user_query, analysis, db_connection, output_filename, output_format)
    else:
        # Use Click argument parsing
        main()  