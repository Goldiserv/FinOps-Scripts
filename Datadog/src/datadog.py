# from datadog_api_client.v1 import ApiClient, Configuration
# from datadog_api_client.v1.api.monitors_api import MonitorsApi
# from datadog_api_client.v1.model.monitor import Monitor
# from datadog_api_client.v1.model.monitor_type import MonitorType

from datadog_api_client.v1 import ApiClient, Configuration
from datadog_api_client.v1.api.authentication_api import AuthenticationApi
from datadog_api_client.v1.api.aws_integration_api import AWSIntegrationApi
from datadog_api_client.v1.api.notebooks_api import NotebooksApi
from datadog_api_client.v1.api.metrics_api import MetricsApi
import os
from dotenv import load_dotenv

load_dotenv()

configuration = Configuration()
configuration.api_key["apiKeyAuth"] = os.getenv("DD_API_KEY")
configuration.api_key["appKeyAuth"] =  os.getenv("DD_APP_KEY")
configuration.server_variables["site"] = "datadoghq.com"

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
        thread = api.list_notebooks(async_req=True)
        result = thread.get()
        print(result)

def get_notebook(notebook_id):
    with ApiClient(configuration) as api_client:
        api = NotebooksApi(api_client)
        thread = api.get_notebook(notebook_id, async_req=True)
        result = thread.get()
        print(result)

def get_metric_metadata(metric_name):
    with ApiClient(configuration) as api_client:
        api = MetricsApi(api_client)
        thread = api.get_metric_metadata(metric_name, async_req=True)
        result = thread.get()
        print(result)

def query_metrics(metric_name):
    with ApiClient(configuration) as api_client:
        api = MetricsApi(api_client)
        # 1648774800 20220401 1am
        # 1649898000 20220414 1am
        # 1649983800 20220415 12:50am
        # 1649984400 20220415 1am
        thread = api.query_metrics(1648774800, 1649984400, metric_name, async_req=True)
        result = thread.get()
        # print(result)
        # print(repr(result))
        write_to_file(repr(result))
        
def write_to_file(data_str):
    with open("Datadog\data\output4.txt", "a") as o:
        o.write(data_str)
        o.close()

# max:aws.ec2.cpuutilization.maximum{*} by {team}
# list_notebooks()
# get_notebook(2275429)

# query_metrics("max:aws.ec2.cpuutilization.maximum{*} by {team}.rollup(max, 3600)")
# query_metrics("max:aws.ec2.cpuutilization.maximum{*} by {team,name,instance_id}")
query_metrics("max:aws.ec2.cpuutilization.maximum{*} by {team,name,instance_id}.rollup(max, 86400)")

# query_metrics("aws.rds.read_latency")

# avg:aws.ec2.cpuutilization.maximum{*}