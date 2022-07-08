-- Maps CUR service name to Cost Explorer service name

-- CREATE OR REPLACE VIEW "athena_db_name".cur_tbl1_v01_cur_to_ce_svc_mapping AS
-- SELECT
--     product_code, ce_svc
-- FROM (
--     VALUES
--         (MAP(ARRAY['CurSvc1','CurSvc2'], ARRAY['CostExpSvc1','CostExpSvc2']))
-- ) AS x (product_code_ce_svc)
-- CROSS JOIN UNNEST(product_code_ce_svc) AS t (product_code, ce_svc);

CREATE OR REPLACE VIEW "athena_db_name".cur_tbl1_v01_cur_to_ce_svc_map AS
SELECT
    product_code, ce_svc
FROM (
    VALUES
        (MAP(
          ARRAY[
            'AmazonApiGateway','AmazonAthena','AmazonCloudFront','AmazonCloudWatch','AmazonCognito','AmazonConnect','AmazonDocDB','AmazonDynamoDB','AmazonEC2','AmazonECR','AmazonECS','AmazonEFS','AmazonEKS','AmazonElastiCache','AmazonES','AmazonETS','AmazonFSx','AmazonGlacier','AmazonInspector','AmazonKinesis','AmazonKinesisAnalytics','AmazonKinesisFirehose','AmazonLightsail','AmazonMCS','AmazonMQ','AmazonMSK','AmazonMWAA','AmazonPinpoint','AmazonQuickSight','AmazonRDS','AmazonRedshift','AmazonRegistrar','AmazonRekognition','AmazonRoute53','AmazonS3','AmazonSageMaker','AmazonSES','AmazonSimpleDB','AmazonSNS','AmazonStates','AmazonTextract','AmazonVPC','AmazonWorkDocs','AmazonWorkSpaces','APNFee','AWSAmplify','AWSBackup','AWSBudgets','AWSCloudMap','AWSCloudShell','AWSCloudTrail','AWSCodeArtifact','AWSCodeCommit','AWSCodePipeline','AWSConfig','AWSCostExplorer','AWSDatabaseMigrationSvc','AWSDataSync','AWSDataTransfer','AWSDirectConnect','AWSDirectoryService','AWSELB','AWSElementalMediaStore','AWSEvents','AWSGlobalAccelerator','AWSGlue','AWSIoT','awskms','AWSLambda','AWSQueueService','AWSSecretsManager','AWSSecurityHub','AWSServiceCatalog','AWSShield','AWSStorageGateway','AWSSystemsManager','AWSTransfer','awswaf','AWSXRay','CodeBuild','comprehend','ComputeSavingsPlans','ContactCenterTelecomm','datapipeline','ElasticMapReduce','OCBPremiumSupport'], 
          ARRAY[
            'API Gateway($)','Athena($)','CloudFront($)','CloudWatch($)','Cognito($)','Connect($)','DocumentDB (with MongoDB compatibility)($)','DynamoDB($)','EC2-Other($)','EC2 Container Registry (ECR)($)','Elastic Container Service($)','Elastic File System($)','Elastic Container Service for Kubernetes($)','ElastiCache($)','Elasticsearch Service($)','Elastic Transcoder($)','FSx($)','Glacier($)','Inspector($)','Kinesis($)','Kinesis Analytics($)','Kinesis Firehose($)','Lightsail($)','Managed Apache Cassandra Service($)','Managed Apache Cassandra Service($)','Managed Streaming for Apache Kafka($)','Managed Workflows for Apache Airflow($)','Pinpoint($)','QuickSight($)','Relational Database Service($)','Redshift($)','Registrar($)','Rekognition($)','Route 53($)','S3($)','SageMaker($)','SES($)','SimpleDB($)','SNS($)','Step Functions($)','Textract($)','VPC($)','WorkDocs($)','WorkSpaces($)','APN Annual Program Fee($)','Amplify($)','Backup($)','Budgets($)','Cloud Map($)','CloudShell($)','CloudTrail($)','CodeArtifact($)','CodeCommit($)','CodePipeline($)','Config($)','Cost Explorer($)','DMS($)','DataSync($)','Data Transfer($)','Direct Connect($)','Directory Service($)','EC2-ELB($)','Elemental MediaStore($)','CloudWatch Events($)','Global Accelerator($)','Glue($)','IoT($)','Key Management Service($)','Lambda($)','SQS($)','Secrets Manager($)','Security Hub($)','Service Catalog($)','Shield($)','Storage Gateway($)','Systems Manager($)','Transfer Family($)','WAF($)','X-Ray($)','CodeBuild($)','Comprehend($)','Savings Plans for  Compute usage($)','Contact Center Telecommunications (service sold by AMCS, LLC)($)','Data Pipeline($)','EMR($)','Premium Support($)']
          ))
) AS x (product_code_ce_svc)
CROSS JOIN UNNEST(product_code_ce_svc) AS t (product_code, ce_svc);
