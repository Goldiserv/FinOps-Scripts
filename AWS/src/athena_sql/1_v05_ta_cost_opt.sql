-- Feeds Optimisation/Trusted Advisor section of QS dashboard

CREATE OR REPLACE VIEW "athena_db_name".cur_tbl1_v05_ta_cost_opt AS
SELECT
checkname AS ta_check_name
, accountid AS line_item_usage_account_id
, accountname AS account_name
, from_unixtime(CAST(substr("timestamp",1,10) AS bigint)) AS "date_time"
, CASE WHEN checkname = 'Amazon RDS Idle DB Instances' THEN sum(CAST(replace(replace("estimated monthly savings (on demand)", '$'), ',') AS double))
  WHEN checkname = 'AWS Lambda Functions with High Error Rates' THEN sum(CAST("average daily invokes" AS double)) * 0.0000167 -- assumed cost per invoke
  WHEN checkname = 'Unassociated Elastic IP Addresses' THEN count(*) * 3.6
  WHEN checkname = 'Underutilized Amazon EBS Volumes' THEN sum(CAST(replace("monthly storage cost", '$') AS double)) * 0.2 --assume 20% saving
  ELSE sum(CAST(replace(replace("estimated monthly savings", '$'), ',') AS double))
  END AS est_monthly_savings 
FROM "athena_db_name".trustedadvisor
WHERE true
  and category = 'Cost Optimizing'
  and checkname NOT IN ('Amazon EC2 Reserved Instances Optimization'
    ,'Amazon ElastiCache Reserved Node Optimization'
    ,'Amazon OpenSearch Service Reserved Instance Optimization'
    ,'Amazon Relational Database Service (RDS) Reserved Instance Optimization'
    ,'Amazon Route 53 Latency Resource Record Sets'
    ,'Savings Plan')
  and issuppressed <> 'true'
GROUP BY 1,2,3,4
-- LIMIT 20
