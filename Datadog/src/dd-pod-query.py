from datadog_api_client.v1 import ApiClient, Configuration
from datadog_api_client.v1.api.authentication_api import AuthenticationApi
from datadog_api_client.v1.api.metrics_api import MetricsApi
import os
import json
from numpy import average
from dotenv import load_dotenv
from datetime import datetime
from dateutil.relativedelta import relativedelta

load_dotenv()

configuration = Configuration()
configuration.api_key["apiKeyAuth"] = os.getenv("DD_API_KEY")
configuration.api_key["appKeyAuth"] = os.getenv("DD_APP_KEY")
configuration.server_variables["site"] = os.getenv("DATADOG_SITE")

"""
Params: pod_name, period_days, aggregation_secs
Return: pod cpu, mem; util and limits
"""

def validate_api():
    with ApiClient(configuration) as api_client:
        api_instance = AuthenticationApi(api_client)
        response = api_instance.validate()
        print(response)

def get_metric_metadata(metric_name):
    with ApiClient(configuration) as api_client:
        api = MetricsApi(api_client)
        thread = api.get_metric_metadata(metric_name, async_req=True)
        result = thread.get()
        print(result)


def query_metrics(metric_name):
    with ApiClient(configuration) as api_client:
        api = MetricsApi(api_client)
        response = api.query_metrics(
            _from=int((datetime.now() + relativedelta(days=-1)).timestamp()),
            to=int(datetime.now().timestamp()),
            query=metric_name,  # e.g. system.cpu.idle{*},
        )
        return response


def epoch_to_datetime(epoch_time_secs):
    date_time = datetime.datetime.fromtimestamp(epoch_time_secs)
    return date_time

def format_dd_query_resp(input_str, pct_format = False):
    # clean resp
    input_str = str(input_str).replace("'\n                           '", "")
    input_str = str(input_str).replace("'\n          '", "")
    input_str = str(input_str).replace("'", '"')
    input_str = str(input_str).replace("None", "null")
    # print(input_str)
    json_object = json.loads(input_str)
    # print(type(json_object))

    for series_obj in json_object["series"]:  # series object should only have 1 entry
        metric = series_obj["metric"]
        # tag_set = series_obj["tag_set"]
        # scope = series_obj["scope"]
        # start_time = series_obj["start"]
        # end_time = series_obj["end"]

        # determine count,max,avg of datadog record
        pointlist = series_obj["pointlist"]
        p_builder = []
        for p in pointlist:
            if not (p[1] is None):
                p_builder.append(p[1])
        metric_len = len(p_builder)
        metric_max = max(p_builder)
        metric_avg = average(p_builder)

        return [
            metric,
            metric_len,
            "{:.2%}".format(metric_max) if pct_format else metric_max,
            "{:.2%}".format(metric_avg) if pct_format else metric_avg
        ]

## Python program to print the data
def print_table_data(table_data):
    col_headings = ["Metric", "Count", "Max", "Avg"]
    # data = [
    #      ["max:kubernetes.memory.usage_pct", 1, 7, 8],
    #      ["min:kubernetes.memory.usage_pct", 9, 3, 2],
    # ]
    format_row = "{:>0} {:>35} {:>8} {:>20}  {:>20} "
    print(format_row.format("", *col_headings))
    for k, row in zip(col_headings, table_data):
        print(format_row.format("", *row))


def get_util(pod_name, period_days=1, aggregation_secs=3600):
    print("starting get util ...")
    data_holder = []

    mem_pct_query = "max:kubernetes.memory.usage_pct{{pod_name:{pod_name}}} by {{pod_name}}.rollup(max, {aggregation_secs})".format(
        pod_name=pod_name, aggregation_secs=aggregation_secs
    )
    mem_pct_response = format_dd_query_resp(repr(query_metrics(mem_pct_query)), True)
    data_holder.append(mem_pct_response)

    mem_limit_query = "max:kubernetes.memory.limits{{pod_name:{pod_name}}} by {{pod_name}}.rollup(max, {aggregation_secs})".format(
        pod_name=pod_name, aggregation_secs=aggregation_secs
    )
    mem_limit_response = format_dd_query_resp(repr(query_metrics(mem_limit_query)))
    data_holder.append(mem_limit_response)

    # print(data_holder)
    print_table_data(data_holder)

get_util("podName")
