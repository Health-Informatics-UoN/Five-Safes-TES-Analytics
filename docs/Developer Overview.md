# Developer Overview

## Overview

The analytics are performed by an AnalysisOrchestrator object. This only takes two arguments - an auth token and a project name, but these are usually both defined in environment variables. The token can be obtained from the submission web UI, and pasted into the .env file.

The analysis orchestrator creates all the necessary objects to handle the various communications, check status, poll for results and return results to the user. It also stores the returned results, so that they can be reused for mathematically related queries, and the user can see returned results.

## Core Workflow

`AnalysisOrchestrator.run_analysis()` is the core function. This runs all the required steps sequentially. This requires the parameters of the query, including the query itself, analysis type, the TREs to query and some further optional arguments. 

## Query Processing

The code validates and generates the full query based on the user query and combines it with hardcoded queries based on the analysis type. This allows the user query to focus on data selection relevant to the research question, and abstracts the part of the query for the analysis calculations.

## Task Submission and Management

The BaseTESClient then generates the full TES message to be sent to the TES service based on the arguments of the analysis orchestrator (typically from environment variables), the complete query and the arguments of the run_analysis function. The submission of the TES task returns the task ID. Note that the task ID gives rise to subtask IDs, which are incremented from the task ID. For example, if the task ID is 179, and there are two TREs, the subtasks will be IDs 180 and 181. The subtask IDs are used in the results paths to avoid conflicts. The submission layer is responsible for generating the subtask IDs and ensuring they are saved in appropriate subfolders in the MinIO bucket.

## Status Monitoring

Once the TES task is submitted, the client application polls the submission layer for the status of the task (there is currently no way of getting the status of the subtasks except for using the submission layer UI). This is a blocking task. The important statuses are shown below:

| Status | Description                                 |
| ------ | ------------------------------------------- |
| 0      | Waiting for Child Subs To Complete          |
| 11     | Completed                                   |
| 16     | Cancelled                                   |
| 27     | Failed                                      |
| 49     | Complete but not all TREs returned a result |

Of these statuses, status 0 means that the query is being processed, but gives no information as to which stage of processing it is at. All other statuses mean that the process has finished, either successfully or unsuccessfully. A status of 49, a partial result, will still result in the analysis taking place, but on the reduced data set.

## Data Aggregation

Once the data are collected, they are aggregated. The exact method used depends on the analysis type; simple numerical analyses like means and variances have components that can simply be summed. Contingency table data is just frequencies of occurrence, so it is also aggregated by simple summation, but the summation is more complex as it requires careful handling to ensure that the correct data are added, and the labels are kept with the data.

## Statistical Analysis

Once data is aggregated, the StatisticalAnalyzer performs the final calculations. Different analysis types (mean, variance, PMCC, chi-squared) have their own analysis classes that inherit from AnalysisBase. Each analysis class handles the specific mathematical operations required for that statistical measure. The analyzer also stores aggregated data in a centralized format, enabling reuse for compatible analyses without requiring additional data collection.

## Data Reuse

Aggregated data is stored, and the final parts of the calculation are performed on the aggregated data. The aggregated data can be accessed from the object directly, and other analyses can be attempted based on previously collected aggregated data without requiring another TES query. For example, calculating the variance returns n, x^2 and the total. Calculating the mean requires n and the total, so calculating the variance requires that the information to calculate the mean has already been obtained (and been through disclosure control and approved). We can therefore calculate the mean based on the data that was already returned for the calculation of the variance, without incurring another request.








