import numpy as np
from scipy import stats
from datetime import datetime
import csv
from collections import defaultdict

import boto3
import json
from botocore.config import Config

## original test data is 65, 52
def variance(stacked_values):
    n, sum_x2, total = np.sum(stacked_values, axis=0)
    mean = total / n; ## just in case you want to return the mean as well
    return (sum_x2 - (total*total)/n) / (n-1)

def mean(stacked_values):
    n, total = np.sum(stacked_values, axis=0)
    return total / n

def PMCC(stacked_values):
    n, sum_x, sum_y, sum_xy, sum_x2, sum_y2 = np.sum(stacked_values, axis=0)
    mean_x = sum_x / n
    mean_y = sum_y / n
    std_x = np.sqrt(sum_x2 - (sum_x**2)/n)
    std_y = np.sqrt(sum_y2 - (sum_y**2)/n)
    cov = (sum_xy - (sum_x*sum_y)/n) / (n-1)
    return cov / (std_x * std_y)
    

def chi_squared_scipy(contingency_table):
    # Get both corrected and uncorrected results
    chi2_corrected, p_corrected, dof, expected = stats.chi2_contingency(contingency_table)
    chi2_uncorrected, p_uncorrected, _, _ = stats.chi2_contingency(contingency_table, correction=False)
    
    #return {
    #    'chi_squared_corrected': chi2_corrected,
    #    'chi_squared_uncorrected': chi2_uncorrected,
    #    'p_value_corrected': p_corrected,
    #    'p_value_uncorrected': p_uncorrected,
    #    'degrees_of_freedom': dof,
    #    'expected_frequencies': expected
    #}
    return chi2_uncorrected

def chi_squared_manual(contingency_table):
    # Manual calculation
    row_totals = np.sum(contingency_table, axis=1)
    col_totals = np.sum(contingency_table, axis=0)
    total = np.sum(row_totals)
    expected = np.zeros_like(contingency_table)
    for i in range(len(contingency_table)):
        for j in range(len(contingency_table[i])):
            expected[i][j] = row_totals[i] * col_totals[j] / total
    
    # Calculate chi-squared
    chi2 = np.sum((contingency_table - expected) ** 2 / expected)
    dof = (len(row_totals) - 1) * (len(col_totals) - 1)
    p_value = 1 - stats.chi2.cdf(chi2, dof)
    
    return {
        'chi_squared': chi2,
        'p_value': p_value,
        'degrees_of_freedom': dof,
        'expected_frequencies': expected
    }

def dict_to_array(contingency_dict):
    # Get unique values for each dimension from the keys
    keys = [k for k in contingency_dict.keys() if k != 'header']
    first_values = set(k.split(',')[0] for k in keys)
    second_values = set(k.split(',')[1] for k in keys)
    
    # Create empty array
    result = np.zeros((len(second_values), len(first_values)))
    
    # Fill array using the keys to determine position
    for key, value in contingency_dict.items():
        if key != 'header':
            row, col = key.split(',')
            row_idx = list(second_values).index(col)  # Race is now rows
            col_idx = list(first_values).index(row)   # Gender is now columns
            result[row_idx, col_idx] = value
    
    return result

def parse_contingency_table(csv_data):
    # Skip header row and empty rows
    rows = [row.strip() for row in csv_data.split('\n') if row.strip()]
    
    # Skip the header row
    data_rows = rows[1:]
    
    # Extract just the counts
    counts = []
    for row in data_rows:
        count = int(row.split(',')[-1])  # Get the last column (count)
        counts.append(count)
    
    return np.array(counts).reshape(2, 2)  # 2x2 for gender x race

def combine_contingency_tables(contingency_tables):
    labels = {}
    #used a dictionary to store the counts instead of a 2x2 array because rows are not guaranteed to be the same
    for table in contingency_tables:
        rows = [row.strip() for row in table.split('\n') if row.strip()]
        if not rows:  # Skip empty tables
            continue
            
        labels["header"] = rows[0] ## column order is guaranteed to be the same
        
        data_rows = rows[1:]
        for row in data_rows:
            try:
                parts = row.split(',')
                if len(parts) < 2:  # Skip rows without enough parts
                    continue
                count = int(parts[-1])  # Get count from last column
                row_without_count = ','.join(parts[:-1])  # Get rest of row without count
                if row_without_count in labels:
                    labels[row_without_count] += count
                else:
                    labels[row_without_count] = count
            except (ValueError, IndexError) as e:
                print(f"Warning: Skipping malformed row: {row}")
                continue
    
    return labels

def combine_contingency_files(filelist):
    labels = defaultdict(int)
    for file in filelist:
        with open(file, 'r') as file:
            reader = csv.reader(file)
            labels["header"] = next(reader)
            for row in reader:
                if len(row) < 2:  # Skip rows without enough parts
                    continue
                row_without_count = row_without_count = ','.join(row[:-1])  # Get rest of row without count
                labels[row_without_count] += int(row[-1])
                print(row)
            #reader = csv.DictReader(file)
            #data = [row for row in reader]
    array_table = dict_to_array(labels)
    return array_table

# ### dict of analysis types must include the return data format and the function to aggregate the data
analysis_types = {
    "variance": { ## can also easily return the mean here
        "return_format": "n, sum_x2, total",
        "aggregation_function": variance
    }, 
    "mean": {
        "return_format": "n, total",
        "aggregation_function": mean
    },
    "PMC": {
        "return_format": "n, sum_x, sum_y, sum_xy, sum_x2, sum_y2",
        "aggregation_function": PMCC
    },
    "chi_squared_scipy": {
        "return_format": "contingency_table",
        "aggregation_function": chi_squared_scipy
    },
    "chi_squared_manual": {
        "return_format": "contingency_table",
        "aggregation_function": chi_squared_manual
    }

}


### this is only true in the variance case
def import_data(input):
## take in some input data as a csv string
#n, sum_x2, total

    values = np.array([int(x) for x in input.split(",")])

    print(values)
    return values

def get_result_from_local(file):
    #results = {}
    with open(file, 'r') as file:
        ### use this if you want to read it in as a dictionary, but it shouldn't be needed - the data is always in the same order
        #reader = csv.DictReader(file)
        #data = [row for row in reader]
        
        reader = csv.reader(file)
        next(reader) ## skip the header row
        for row in reader:
            results = row

    return results

def get_result_from_s3():
    ## s3://beacon7283outputtre/output.json

    ### s3.Object('bucketName', 'keyName') so an example to get the file s3://foobarBucketName/folderA/folderB/myFile.json would be 
    ### s3.Object('foobarBucketName', 'folderA/folderB/myFile.json')
    
    # Configure MinIO endpoint
    try:
        s3 = boto3.resource('s3',
            endpoint_url='http://trefx01-v2.uksouth.cloudapp.azure.com:9000',  # Changed to API port
            aws_access_key_id='',
            aws_secret_access_key='',
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )

        # Test the connection by listing buckets
        print("Available buckets:", [bucket.name for bucket in s3.buckets.all()])
        
        content_object = s3.Object('beacon7283outputtre', 'output.json')
        file_content = content_object.get()['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)
        return json_content
    except Exception as e:
        print(f"Error connecting to MinIO: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        raise


def combine_file_data(filelist):
    data = None
    for file in filelist:
        file_data = np.array(get_result_from_local(file)).astype(float)
        if data is None:
            data = file_data.reshape(1,-1)  # First array, reshape to 2D
        else:
            data = np.vstack((data, file_data.reshape(1,-1)))  # Stack subsequent arrays
    return data


if __name__ == "__main__":
    
    ###################SETUP
    ### set some sample data - I've just used the same value for all inputs for now
    
    #filelist = ['TRE-FX Analytics/TES/outputs/output.csv', 'TRE-FX Analytics/TES/outputs/output2.csv']

    #analysis_type = "mean"
    
    #inputs = ["2,117", "2,117", "2,117"]       
    #input_data = combine_file_data(filelist)






    #analysis_type = "variance"
    #inputs = ["2,6929,117", "2,6929,117", "2,6929,117"]
    ######################################################### #########################################################

    ##########################SETUP
    ### sample data for contingency table
    analysis_type = "chi_squared_scipy"  # Change to "chi_squared_manual" for manual calculation

    #contingency_table1 = """gender_name,race_name,n
    #FEMALE,Asian Indian,2
    #FEMALE,Japanese,2
    #MALE,Asian Indian,1
    #MALE,Japanese,1
    #"""
    #contingency_table2 = """gender_name,race_name,n
    #MALE,Asian Indian,3
    #MALE,Japanese,4
    #FEMALE,Asian Indian,5
    #FEMALE,Japanese,1"""

    filelist = ['TRE-FX Analytics/TES/outputs/output contingency.csv', 'TRE-FX Analytics/TES/outputs/output contingency2.csv']

    #inputs = [contingency_table1, contingency_table2]
    ##########################PREPROCESSING
    analysis_function = analysis_types[analysis_type]["aggregation_function"]
    input_data = combine_contingency_files(filelist)
    #if analysis_types[analysis_type]["return_format"] == "contingency_table":
    #    combined_table = combine_contingency_tables(inputs)
    #    print("\nCombined Contingency Table (dictionary):")
    #    print(combined_table)
        
        # Convert to array
    #    array_table = dict_to_array(combined_table)
    #    print("\nCombined Contingency Table (array):")
    #    print(array_table)
    #    input_data = array_table
    #else:
    ###################PREPROCESSING
    #    values = [import_data(input) for input in inputs]
        
        ## creates an array of the values returned from the different sources
        #stacked_values = np.vstack(values)
        #print("\nStacked values:")
        #print(stacked_values)
        #input_data = stacked_values
    ##########################ANALYSIS

    result = analysis_function(input_data)

    ##########################OUTPUT
    print("\nResult:")
    print(result)

    #result = get_result()
    #print(result)

