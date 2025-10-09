from sqlalchemy import create_engine, text, engine
import numpy as np
from decimal import Decimal
import local_processing
import json
import click
import sys

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

### These classes are to run on the node, to make SQL queries and perform the partial analysis to be aggregated later.

### The user query and analysis type are passed in as args.
### Analysis type is looked up in the LOCAL_PROCESSING_CLASSES registry.
### Each analysis class handles its own SQL query building and Python analysis.
### Results are returned as JSON for aggregation on the client side.


#################################################################################
#### input args from user
# Your original query - just execute it directly
#user_query = """
#SELECT value_as_number FROM public.measurement 
#WHERE measurement_concept_id = 3037532
#AND value_as_number IS NOT NULL
#"""

#analysis = 'mean'

#################################################################################
### Using dedicated classes for local processing and aggregation.
### Each analysis type has its own processing class that handles SQL and Python analysis.

@click.command()
@click.option('--user-query', required=True, help='SQL query to execute')
@click.option('--analysis', required=True, help='Type of analysis to perform')
@click.option('--db-connection', default="postgresql://postgres:postgres@localhost:5432/omop", 
                       help='Database connection string')
@click.option('--output-filename', help='Output filename', default='data')
@click.option('--output-format', type=click.Choice(['json', 'csv']), default='json',
                       help='Output format (json or csv)')
def main(user_query, analysis, db_connection, output_filename, output_format):
    #################################################################################
    #### Setup
    try:    
        output_filename = output_filename + '.' + output_format
        
        ## sample connection string: "postgresql://postgres:postgres@localhost:5432/postgres"
        sql_engine = create_engine(db_connection)

        registry = local_processing.LOCAL_PROCESSING_CLASSES
        if analysis not in registry:
            ## This is temporary - we'll have to send the error to the client.
            raise ValueError(f"Unsupported analysis type: {analysis}")

        processor = registry[analysis](user_query=user_query, engine=sql_engine)


        query = processor.build_query()
        print(f"DEBUG: Executing query: {query}")

        ## execute query
        with sql_engine.connect() as conn:
            db_result = conn.execute(text(query))
            ## deliberately not fetching the data, so we can do python analysis on it in case it is huge.
            ##db_result_data = db_result.fetchall()


        ## sample output might be:  
        ## [(5.2,), (7.8,), (4.1,)] ## from a single column.
        ## test_output = [(5.2,), (7.8,), (4.1,)]
        ## test_output = [(Decimal('65'),), (Decimal('52'),), (None,)]


        ### check if we need to do any python analysis
        python_post_query_hook = processor.python_analysis(db_result)

        if python_post_query_hook is not None:
            result = python_post_query_hook
        else:
            result = db_result

        if isinstance(result, engine.Result):
            result = result.fetchall()

        ## convert to list of dictionaries, so it's easier to work with.

        if result:
            # For aggregations, we expect only one row
            # Extract the single row values and convert to dict
            row_values = result[0]  # This is the tuple of values
            # Use the keys from the result to create the dict
            result = dict(zip(result.keys(), row_values))

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


        #################################################################################
        ### How is the result handled?
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

if __name__ == "__main__":
    import sys
    if len(sys.argv) == 1:  # No command line arguments
        # Use test values
        user_query = """
SELECT value_as_number FROM public.measurement 
WHERE measurement_concept_id = 3037532
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