## This is a sample query to get some measurement data to do some analysis on, for a given concept id
usr_qry = """WITH user_query AS (
  SELECT value_as_number FROM public.measurement 
  WHERE measurement_concept_id = 3037532
  AND value_as_number IS NOT NULL
)"""

# This is a sample query to get some paired data to do some analysis on, for a given concept id

usr_qry_2 = """WITH x_values AS (
  SELECT person_id, measurement_date, value_as_number AS x
  FROM public.measurement
  WHERE measurement_concept_id = 3037532
    AND value_as_number IS NOT NULL
),
y_values AS (
  SELECT person_id, measurement_date, value_as_number AS y
  FROM public.measurement
  WHERE measurement_concept_id = 3025315
    AND value_as_number IS NOT NULL
),
paired_data AS (
  SELECT
    x.x,
    y.y
  FROM x_values x
  INNER JOIN y_values y
    ON x.person_id = y.person_id
    AND x.measurement_date = y.measurement_date
)
"""

## This is a sample query to get some data for a contingency table
contingency_qry = """SELECT
  'gender_name' AS gender_name,
  'race_name'   AS race_name,
  'n'           AS n

UNION ALL

-- Data rows
SELECT
  g.concept_name,
  r.concept_name,
  CAST(COUNT(*) AS TEXT)
FROM person p
JOIN concept g ON p.gender_concept_id = g.concept_id
JOIN concept r ON p.race_concept_id = r.concept_id
WHERE p.race_concept_id IN (38003574, 38003584)
GROUP BY
  g.concept_name,
  r.concept_name;
"""


analysis_queries = {
    'mean': """SELECT
  COUNT(value_as_number) AS n,
  SUM(value_as_number) AS total
FROM user_query;""",
    
    'variance': """SELECT
  COUNT(value_as_number) AS n,
  SUM(value_as_number*value_as_number) AS sum_x2,
  SUM(value_as_number) AS total
FROM user_query;\n""",

    'PMCC': """SELECT
  COUNT(*) AS n,
  SUM(x) AS sum_x,
  SUM(y) AS sum_y,
  SUM(x * x) AS sum_x2,
  SUM(y * y) AS sum_y2,
  SUM(x * y) AS sum_xy
FROM paired_data;\n"""

}

analysis_type = "mean"
qry = usr_qry + analysis_queries[analysis_type]


#analysis_type = "PMCC"
#qry = usr_qry_2 + analysis_queries[analysis_type]

print(qry)