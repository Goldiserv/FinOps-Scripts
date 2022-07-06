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


def format_dd_query_resp(input_str, pct_format=False):
    # clean resp
    input_str = str(input_str).replace("'\n                           '", "")
    input_str = str(input_str).replace("'\n          '", "")
    input_str = str(input_str).replace("'", '"')
    input_str = str(input_str).replace("None", "null")
    print(input_str)
    json_object = json.loads(input_str)
    # print(type(json_object))

    result_holder = []

    # series object should have 1 entry per metric request
    for series_obj in json_object["series"]:

        metric = series_obj["metric"]
        # tag_set = series_obj["tag_set"]
        # scope = series_obj["scope"]
        # start_time = series_obj["start"]
        # end_time = series_obj["end"]
        unit_info = series_obj["unit"]
        scale_factor = unit_info[0]["scale_factor"]

        # determine count,max,avg of datadog record
        pointlist = series_obj["pointlist"]
        p_builder = []
        for p in pointlist:
            if not (p[1] is None):
                p_builder.append(p[1])
        metric_len = len(p_builder)
        metric_max = max(p_builder) * scale_factor
        metric_avg = average(p_builder) * scale_factor

        result_holder.append(
            [
                metric,
                metric_len,
                "{:.2%}".format(metric_max) if pct_format else metric_max,
                "{:.2%}".format(metric_avg) if pct_format else metric_avg,
            ]
        )

    return result_holder


def print_table_data(table_data):
    col_headings = ["Metric", "Count", "Max", "Avg"]
    print(table_data)
    # data = [
    #      ["max:kubernetes.memory.usage_pct", 1, 7, 8],
    #      ["min:kubernetes.memory.usage_pct", 9, 3, 2],
    # ]
    format_row = "{:>0} {:>35} {:>8} {:>25}  {:>25} "
    print(format_row.format("", *col_headings))
    for row in table_data:
        print(format_row.format("", *row))


def get_cpu_util(cpu_usage_data, cpu_requests_data):
    # ['kubernetes.cpu.usage.total', 15, 4.778767578125001e-06, 3.4960713867187503e-06]
    # ['kubernetes.cpu.requests', 15, 0.0010000000474974513, 0.0010000000474974513]
    # col_headings = ["Metric", "Count", "Max", "Avg"]
    try:
        max_util = cpu_usage_data[2] / cpu_requests_data[2]
    except ZeroDivisionError:
        max_util = 0

    try:
        avg_util = cpu_usage_data[3] / cpu_requests_data[3]
    except ZeroDivisionError:
        avg_util = 0

    return ["kubernetes.cpu.usage_pct", cpu_usage_data[1], max_util * 100, avg_util * 100]


def get_util(pod_name, period_days=1, aggregation_secs=3600):
    print("starting get util ...")

    dd_query = "max:kubernetes.memory.usage_pct{{pod_name:{pod_name}}} by {{pod_name}}.rollup(max, {aggregation_secs}), \
                     max:kubernetes.memory.usage{{pod_name:{pod_name}}} by {{pod_name}}.rollup(max, {aggregation_secs}), \
                     max:kubernetes.memory.limits{{pod_name:{pod_name}}} by {{pod_name}}.rollup(max, {aggregation_secs}), \
                     max:kubernetes.cpu.usage.total{{pod_name:{pod_name}}} by {{pod_name}}.rollup(max, {aggregation_secs}), \
                     max:kubernetes.cpu.requests{{pod_name:{pod_name}}} by {{pod_name}}.rollup(max, {aggregation_secs}) \
    ".format(
        pod_name=pod_name, aggregation_secs=aggregation_secs
    )
    dd_resp = query_metrics(dd_query)
    response_formatted = format_dd_query_resp(repr(dd_resp), False)
    # print(response_formatted)
    cpu_util_row = get_cpu_util(response_formatted[3], response_formatted[4])
    # print(cpu_util_row)
    response_formatted.append(cpu_util_row)
    # print(response_formatted)

    # response_formatted_sample = [
    #     ["kubernetes.memory.usage_pct", 15, 0.5947113037109375, 0.5947113037109375],
    #     ["kubernetes.memory.usage", 15, 6385664.0, 6385664.0],
    #     ["kubernetes.memory.limits", 15, 1073741824.0, 1073741824.0],
    #     [
    #         "kubernetes.cpu.usage.total",
    #         15,
    #         4.778767578125001e-06,
    #         3.4960713867187503e-06,
    #     ],
    #     ["kubernetes.cpu.requests", 15, 0.0010000000474974513, 0.0010000000474974513],
    #     ["kubernetes.cpu.usage_pct", 15, 0.004778767351145731, 0.0034960712206642775],
    # ]

    print_table_data(response_formatted)


get_util(os.getenv("POD_NAME"))
