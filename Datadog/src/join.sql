SELECT a.*,
nw_in_count,nw_in_max,nw_in_avg,nw_in_d1,nw_in_d2,nw_in_d3,nw_in_d4,nw_in_d5,nw_in_d6,nw_in_d7,nw_in_d8,nw_in_d9,nw_in_d10,nw_in_d11,nw_in_d12,nw_in_d13,nw_in_d14
, cost.*
FROM 
(SELECT * FROM "sg-customer-2022-sb"."dd_ec2_cpu_max_2weeks" limit 5) as a
LEFT JOIN 
(SELECT 
instance_id,nw_in_count,nw_in_max,nw_in_avg,nw_in_d1,nw_in_d2,nw_in_d3,nw_in_d4,nw_in_d5,nw_in_d6,nw_in_d7,nw_in_d8,nw_in_d9,nw_in_d10,nw_in_d11,nw_in_d12,nw_in_d13,nw_in_d14
FROM "sg-customer-2022-sb"."dd_ec2_nw_in_2weeks") as b
on a.instance_id = b.instance_id
LEFT JOIN 
(
SELECT
line_item_usage_account_id
,line_item_resource_id
,line_item_line_item_type
,line_item_usage_type
,product_instance_type
,line_item_usage_amount
,line_item_unblended_cost
,pricing_public_on_demand_cost
,pricing_term
,product_marketoption
,line_item_line_item_description
,product_vcpu
,product_dedicated_ebs_throughput
,product_memory
,product_network_performance
,product_region
,product_operation
,product_usagetype
FROM
	"cur_athena"  --"athena_db_name".cur_tbl_cost_management
WHERE true
	and line_item_operation like '%RunInstances%'
) as cost
on a.instance_id = cost.line_item_resource_id
;