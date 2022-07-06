from datadog_api_client.v1 import ApiClient, Configuration
from datadog_api_client.v1.api.authentication_api import AuthenticationApi
from datadog_api_client.v1.api.aws_integration_api import AWSIntegrationApi
from datadog_api_client.v1.api.notebooks_api import NotebooksApi
from datadog_api_client.v1.api.metrics_api import MetricsApi
import os
import importlib

file_name_mgr = importlib.import_module("file-name-mgr-ec")
from dotenv import load_dotenv

load_dotenv()

from datetime import datetime
from dateutil.relativedelta import relativedelta

configuration = Configuration()
configuration.api_key["apiKeyAuth"] = os.getenv("DD_API_KEY")
configuration.api_key["appKeyAuth"] = os.getenv("DD_APP_KEY")
configuration.server_variables["site"] = os.getenv("DATADOG_SITE")


def check_user():
    with ApiClient(configuration) as api_client:
        api_instance = AuthenticationApi(api_client)
        response = api_instance.validate()
        print(response)


def validate_api():
    with ApiClient(configuration) as api_client:
        api_instance = AuthenticationApi(api_client)
        response = api_instance.validate()
        print(response)


def list_aws_tag_filters():
    with ApiClient(configuration) as api_client:
        api_instance = AWSIntegrationApi(api_client)
        response = api_instance.list_aws_tag_filters(
            account_id=os.getenv("AWS_ACCT_ID_01"),
            # account_id=os.getenv("AWS_ACCT_ID_02"),
        )

        print(response)

        """ Sample response
        {'filters': [{'namespace': 'elb', 'tag_filter_str': None},
             {'namespace': 'application_elb', 'tag_filter_str': None},
             {'namespace': 'sqs', 'tag_filter_str': None},
             {'namespace': 'rds', 'tag_filter_str': None},
             {'namespace': 'custom', 'tag_filter_str': None},
             {'namespace': 'network_elb', 'tag_filter_str': None},
             {'namespace': 'lambda', 'tag_filter_str': None}]}
        """


def list_integrations():
    with ApiClient(configuration) as api_client:
        api_instance = AWSIntegrationApi(api_client)
        response = api_instance.list_aws_accounts()
        print(response)


def list_notebooks():
    with ApiClient(configuration) as api_client:
        api = NotebooksApi(api_client)
        result = api.list_notebooks()
        print(result)


def get_notebook(NOTEBOOK_DATA_ID):
    with ApiClient(configuration) as api_client:
        api = NotebooksApi(api_client)
        result = api.get_notebook(
            notebook_id=int(NOTEBOOK_DATA_ID),
        )
        print(result)


def get_metric_metadata(metric_name):
    with ApiClient(configuration) as api_client:
        api = MetricsApi(api_client)
        thread = api.get_metric_metadata(metric_name, async_req=True)
        result = thread.get()
        print(result)


def query_metrics(metric_name, file_name):
    with ApiClient(configuration) as api_client:
        api = MetricsApi(api_client)
        # 1648774800 20220401 1am
        # 1649898000 20220414 1am
        # 1649983800 20220415 12:50am
        # 1649984400 20220415 1am
        response = api.query_metrics(
            _from=int((datetime.now() + relativedelta(days=-1)).timestamp()),
            to=int(datetime.now().timestamp()),
            query=metric_name,  # e.g. system.cpu.idle{*},
        )
        write_to_file(repr(response), file_name)
        # print(result)
        # print(repr(result))

        ## TypeError: Object of type MetricsQueryMetadata is not JSON serializable
        # jsonStr = json.dumps(result.__dict__)
        # write_to_file(repr(result))


def write_to_file(data_str, file_name):
    data_file = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "data", file_name)
    )
    with open(data_file, "w") as o:
        o.write(data_str)
        o.close()


# Uncomment any line below to run
### Notebooks
# list_notebooks()
# get_notebook(33423)

### Query
queryStr = "max:kubernetes.memory.usage_pct{*} by {pod_name}.rollup(max, 3600)"
query_metrics(queryStr, "max.kubernetes.memory.usage_pct - 20220705-20220706.txt")

# query_metrics("max:aws.ec2.cpuutilization.maximum{*} by {team}.rollup(max, 3600)")
# query_metrics("max:aws.ec2.cpuutilization.maximum{*} by {team,name,instance_id}")

## EC2
# CPU
# query_metrics("max:aws.ec2.cpuutilization.maximum{*} by {team,name,instance_id}.rollup(max, 86400)")
# NW
# query_metrics("max:aws.ec2.network_in.maximum{*} by {team,name,instance_id}.rollup(max, 86400)")
# query_metrics("max:aws.ec2.network_out.maximum{*} by {team,name,instance_id}.rollup(max, 86400)")
# Disk
# query_metrics("max:aws.ec2.ebsread_ops{*} by {team,name,instance_id}.as_rate().rollup(max, 86400)")
# query_metrics("max:aws.ec2.ebswrite_ops{*} by {team,name,instance_id}.as_rate().rollup(max, 86400)")
# query_metrics("max:aws.ec2.ebsread_bytes{*} by {team,name,instance_id}.as_rate().rollup(max, 86400)")
# query_metrics("max:aws.ec2.ebswrite_bytes{*} by {team,name,instance_id}.as_rate().rollup(max, 86400)")

## RDS
# for x in range(7,8):
# x = 3
# query_str = (
#     file_name_mgr.agg(x)
#     + ":aws.elasticache." #ec2, rds
#     + file_name_mgr.dd_metric_name(x)
#     + "{*} by {team,name,envtype,cache_node_type,cacheclusterid}" #dbinstanceidentifier, cacheclusterid
#     + file_name_mgr.as_rate_or_not(x)
#     + file_name_mgr.rollup(x)
# )
# save_file_name = file_name_mgr.file_name(x,".txt")
# print(query_str)
# print(save_file_name)
# query_metrics(query_str, save_file_name)

# query_metrics("aws.rds.read_latency")
