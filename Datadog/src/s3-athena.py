import boto3
import os
from dotenv import load_dotenv
import importlib

file_name_mgr = importlib.import_module("file-name-mgr-elasticache")

load_dotenv()

session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)


def upload_to_s3(input_file_path, bucket_name, s3_key):
    s3 = session.resource("s3")
    print("uploading from " + input_file_path + "\nto " + bucket_name + "\\" + s3_key)
    s3.meta.client.upload_file(Filename=input_file_path, Bucket=bucket_name, Key=s3_key)


def delete_from_s3(bucket_name, s3_key):
    s3 = session.resource("s3")
    print("deleting " + bucket_name + "/" + s3_key)
    s3.meta.client.delete_object(Bucket=bucket_name, Key=s3_key)


def query_builder_create_table(instance_identifier, table_name, x, s3_folder, use_max):
    prefix = file_name_mgr.prefix(x)
    string_builder = "CREATE EXTERNAL TABLE IF NOT EXISTS " + table_name + " (\n"
    string_builder += instance_identifier + " string\n"
    string_builder += ", scope string\n"
    string_builder += "   , " + prefix + "_count int\n"
    string_builder += (
        "   , " + prefix + "_" + ("max" if use_max == True else "min") + " double\n"
    )
    string_builder += "   , " + prefix + "_avg double\n"

    for i in range(1, 15):
        string_builder += "   , " + prefix + "_d" + str(i) + " string\n"

    string_builder += """
    , tags_1 string
    , tags_2 string
    , tags_3 string
    , tags_4 string
    , tags_5 string
    , tags_6 string
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '\\\"',
    'escapeChar' = '\\\\'
    )
    """

    string_builder += 'LOCATION "s3://sg-customer-2022-sb/' + s3_folder + '"\n'
    string_builder += """
    TBLPROPERTIES (
        'has_encrypted_data'='false'
        ,'skip.header.line.count' = '1'
    );
    """
    # print(string_builder)
    return string_builder


def query_builder_create_join(instance_identifier, table_name, range_start, range_end):
    string_builder = "CREATE OR REPLACE VIEW " + table_name + " AS\n"
    string_builder += "SELECT a." + instance_identifier
    string_builder += """
    ,a.scope
    ,a.tags_1
    ,a.tags_2
    ,a.tags_3
    ,a.tags_4
    ,a.tags_5
    ,a.tags_6

    ,cost.line_item_usage_account_id
    ,cost.line_item_resource_id
    ,cost.product_instance_type
    ,cost.product_usagetype
    ,cost.product_region

    ,cost.line_item_usage_amount
    ,cost.line_item_unblended_cost
    ,cost.pricing_public_on_demand_cost

    ,cost.product_memory as memory
    ,cost.product_network_performance as nw_capacity
    ,cost.product_dedicated_ebs_throughput as dedicated_ebs_throughput

    ,cost.product_vcpu as vcpu
    """
    prefix = file_name_mgr.prefix(0)
    string_builder += ", a." + prefix + "_" + file_name_mgr.agg(0) + "\n"
    string_builder += ", a." + prefix + "_avg\n"
    string_builder += ", a." + prefix + "_count\n"

    for x in range(range_start + 1, range_end):
        prefix = file_name_mgr.prefix(x)
        string_builder += ", " + prefix + "." + prefix + "_" + file_name_mgr.agg(x) + "\n"
        string_builder += ", " + prefix + "." + prefix + "_avg" + "\n"

    string_builder += (
        '\nFROM \n(SELECT * FROM "sg-customer-2022-sb"."'
        + file_name_mgr.athena_table_name(0)
        + '") as a\n'
    )

    for x in range(range_start + 1, range_end):
        prefix = file_name_mgr.prefix(x)
        string_builder += "LEFT JOIN (SELECT " + instance_identifier + "\n"
        string_builder += ", " + prefix + "_" + file_name_mgr.agg(x) + "\n"
        string_builder += ", " + prefix + "_avg\n"
        string_builder += (
            ' FROM "sg-customer-2022-sb"."'
            + file_name_mgr.athena_table_name(x)
            + '") as '
            + prefix  + "\n"
        )
        string_builder += (
            " on a." + instance_identifier + " = " + prefix + "." + instance_identifier
        )

    string_builder += """
    LEFT JOIN 
    (
    SELECT
    line_item_usage_account_id
    ,line_item_resource_id
    ,product_instance_type
    ,product_usagetype
    ,product_vcpu
    ,product_memory
    ,product_dedicated_ebs_throughput
    ,product_network_performance
    ,product_region

    -- ,line_item_line_item_type
    -- ,line_item_usage_type
    -- ,pricing_term
    -- ,product_marketoption
    -- ,line_item_line_item_description
    -- ,product_operation

    ,sum(CAST(line_item_usage_amount as double)) as line_item_usage_amount
    ,sum(CAST(line_item_unblended_cost as double)) as line_item_unblended_cost
    ,sum(CAST(pricing_public_on_demand_cost as double)) as pricing_public_on_demand_cost

    FROM
        "cur_athena"  --"athena_db_name".cur_tbl_cost_management
    WHERE true
        -- and line_item_operation like '%RunInstances%'
        -- and product_servicename = 'Amazon Relational Database Service'
        and product_servicename = 'Amazon ElastiCache'
        and product_instance_type <> ''
    GROUP BY 1,2,3,4,5,6,7,8,9
    ) as cost
    on (cost.line_item_resource_id like 'arn:aws:elasticache%' and cost.line_item_resource_id like concat('%cluster:',a."""
    # on (cost.line_item_resource_id like 'arn:aws:rds%' and cost.line_item_resource_id like concat('%db:',a."""
    string_builder += instance_identifier + "));"

    # print(string_builder)
    return string_builder


def run_athena(db_name, output_location, query_str):
    client = boto3.client("athena", region_name="ap-southeast-1")
    response = client.start_query_execution(
        QueryString=query_str,
        # ClientRequestToken='string',
        QueryExecutionContext={
            "Database": db_name,
            # 'Catalog': 'string'
        },
        ResultConfiguration={
            "OutputLocation": output_location,
            # 'EncryptionConfiguration': {
            #     'EncryptionOption': 'SSE_S3'|'SSE_KMS'|'CSE_KMS',
            #     'KmsKey': 'string'
            # },
            # 'ExpectedBucketOwner': 'string',
            # 'AclConfiguration': {
            #     'S3AclOption': 'BUCKET_OWNER_FULL_CONTROL'
            # }
        },
        WorkGroup="primary",
    )
    print(response)


for x in range(1, 5):
    # upload to s3
    # input_file_path = os.path.abspath(
    #     os.path.join(
    #         os.path.dirname(__file__),
    #         "..",
    #         "data",
    #         file_name_mgr.file_name(x, "-new.csv"),
    #     )
    # )
    # upload_to_s3(
    #     input_file_path, "sg-customer-2022-sb", file_name_mgr.s3_key(x, "-new.csv")
    # )

    # delete from s3
    # delete_from_s3("sg-customer-2022-sb", file_name_mgr.s3_key(x,"-new.csv"))

    #     ## athena create table
    # query_str = query_builder_create_table("cacheclusterid", file_name_mgr.athena_table_name(x), x, file_name_mgr.s3_folder(x), file_name_mgr.agg(x)=="max")
    # print(query_str)
    # run_athena("sg-customer-2022-sb", "s3://sg-customer-2022-sb/Unsaved/", query_str)

    #     ## athena drop table
    # query_str = "DROP TABLE IF EXISTS " + file_name_mgr.athena_table_name(x)
    # run_athena("sg-customer-2022-sb", "s3://sg-customer-2022-sb/Unsaved/", query_str)
    print("done")

str = query_builder_create_join("cacheclusterid", "ec_cost_and_util", 0, 5)
print(str)
