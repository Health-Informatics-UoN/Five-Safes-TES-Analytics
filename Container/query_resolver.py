from sqlalchemy import create_engine, text, engine
import numpy as np
from decimal import Decimal
import statistical_analyzer
from query_builder import QueryBuilder
import local_processing
import json

### These classes are to run on the node, to make SQL queries and perform the partial analysis to be aggregated later.

### so the plan now is to have the user query and analysis type passed in as args.
### Analysis type will be looked up in a dictionary of functions, as they are now. We'll just be doing it on the node instead of the client.
### to do analysis in sql, we need to look up the sql query from the query_builder.py file..
### need to implement hooks to check if we need to do any python analysis, and do actually do it (How? a function in the query builder?)
### Client still has to have a lookup to find what the result is.
### should have a data validation step to make sure the input is a list of numbers.


#################################################################################
#### input args from user
# Your original query - just execute it directly
user_query = """
SELECT value_as_number FROM public.measurement 
WHERE measurement_concept_id = 3037532
AND value_as_number IS NOT NULL
"""

analysis = 'mean'

#################################################################################
### no longer used, but left here for reference
### now use dedicated classes for local processing and aggregation, so there's separation.
#stats = statistical_analyzer.StatisticalAnalyzer()
#query_builder = QueryBuilder()
#query = query_builder.build_query(analysis, user_query)



#################################################################################
#### Setup

local_db = "postgresql://postgres:postgres@localhost:5432/omop"
## sample connection string: "postgresql://postgres:postgres@localhost:5432/postgres"
engine = create_engine(local_db)

registry = local_processing.LOCAL_PROCESSING_CLASSES
if analysis not in registry:
    ## This is temporary - we'll have to send the error to the client.
    raise ValueError(f"Unsupported analysis type: {analysis}")

processor = registry[analysis](user_query=user_query, engine=engine)


query = processor.build_query()

## execute query
with engine.connect() as conn:
    db_result = conn.execute(text(query))
    ## deliberately not fetching the data, so we can do python analysis on it in case it is huge.
    ##db_result_data = db_result.fetchall()


## sample output might be:  
## [(5.2,), (7.8,), (4.1,)] ## from a single column.
## test_output = [(5.2,), (7.8,), (4.1,)]
## test_output = [(Decimal('65'),), (Decimal('52'),), (None,)]


### check if we need to do any python analysis
python_hook = processor.python_analysis(db_result)

if python_hook is not None:
    result = python_hook
else:
    result = db_result

if isinstance(result, engine.Result):
    result = result.fetchall()

## convert to list of dictionaries, so it's easier to work with.
result = [dict(row) for row in result]

## decide to output as json to make it easier to work with, but could also output as csv, or just the result.
json_str = json.dumps(result)
print(json_str)

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