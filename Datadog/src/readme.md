# Intro

This 'app' pulls data from Datadog's API and saves it to a csv file.

# Bulk data download to CSV guide

1. Create a .env file and update it with relevant datadog keys
1. Run *python .\test.py* to confirm python installation. Req packages incl: numpy python-dotenv datadog_api_client
1. Run *python .\datadog.py*, uncommenting run commands at the bottom of the file
    - query_metrics() in datadog.py will create a text file e.g. dd-ebsread-bytes-20220401-20220415.txt
1. Run *python .\parser-<service>.py* to convert the txt file to a csv

# Single pod query guide
1. Run *dd-pod-query* specifying the pod name

# Datadog api examples
https://github.com/DataDog/datadog-api-client-python/blob/master/examples/
