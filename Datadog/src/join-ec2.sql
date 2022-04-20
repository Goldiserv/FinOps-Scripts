--remove limit 5 if not trial

SELECT 

a.instance_id
,a.scope
,a.tags_1
,a.tags_2
,a.tags_3
,a.tags_4

,cost.line_item_usage_account_id
,cost.line_item_resource_id
,cost.product_instance_type
,cost.product_usagetype
,cost.product_vcpu
,cost.product_dedicated_ebs_throughput
,cost.product_memory
,cost.product_network_performance
,cost.product_region

,cost.product_vcpu as vcpu
,a.cpu_max
,a.cpu_avg
,a.cpu_count

,cost.product_memory as memory

,cost.product_network_performance as nw
,nw_in_max
,nw_in_max * 8 / 60 * 2 as nw_in_max_gib_per_sec
,nw_in_avg
,nw_in_count
,nw_out_max
,nw_out_max * 8 / 60 * 2 as nw_out_max_gib_per_sec
,nw_out_avg
,nw_out_count

,cost.product_dedicated_ebs_throughput as dedicated_ebs_throughput
,ebs_read_bytes_max as ebs_read_bytes_per_10sec_max
,ebs_read_bytes_max * 8 / 10 as ebs_read_bits_per_sec_max
,ebs_read_bytes_avg
,ebs_read_bytes_avg * 8 / 10 as ebs_read_bits_per_sec_avg
,ebs_read_bytes_count
,ebs_write_bytes_max as ebs_write_bytes_per_10sec_max
,ebs_write_bytes_max * 8 / 10 as ebs_write_bits_per_sec_max
,ebs_write_bytes_avg
,ebs_write_bytes_avg * 8 / 10 as ebs_write_bits_per_sec_avg
,ebs_write_bytes_count

FROM 
(SELECT * FROM "sg-customer-2022-sb"."dd_ec2_cpu_max_2weeks" limit 5) as a
LEFT JOIN 
(SELECT 
instance_id,nw_in_count,nw_in_max,nw_in_avg
-- ,nw_in_d1,nw_in_d2,nw_in_d3,nw_in_d4,nw_in_d5,nw_in_d6,nw_in_d7,nw_in_d8,nw_in_d9,nw_in_d10,nw_in_d11,nw_in_d12,nw_in_d13,nw_in_d14
FROM "sg-customer-2022-sb"."dd_ec2_nw_in_2weeks") as nw_in
on a.instance_id = nw_in.instance_id
LEFT JOIN 
(SELECT 
instance_id,nw_out_count,nw_out_max,nw_out_avg
-- ,nw_out_d1,nw_out_d2,nw_out_d3,nw_out_d4,nw_out_d5,nw_out_d6,nw_out_d7,nw_out_d8,nw_out_d9,nw_out_d10,nw_out_d11,nw_out_d12,nw_out_d13,nw_out_d14
FROM "sg-customer-2022-sb"."dd_ec2_nw_out_2weeks") as nw_out
on a.instance_id = nw_out.instance_id

LEFT JOIN 
(SELECT 
instance_id,ebs_read_bytes_count,ebs_read_bytes_max,ebs_read_bytes_avg
-- ,ebs_read_bytes_d1,ebs_read_bytes_d2,ebs_read_bytes_d3,ebs_read_bytes_d4,ebs_read_bytes_d5,ebs_read_bytes_d6,ebs_read_bytes_d7,ebs_read_bytes_d8,ebs_read_bytes_d9,ebs_read_bytes_d10,ebs_read_bytes_d11,ebs_read_bytes_d12,ebs_read_bytes_d13,ebs_read_bytes_d14
FROM "sg-customer-2022-sb"."dd_ec2_ebs_read_bytes_2weeks") as ebs_read_bytes
on a.instance_id = ebs_read_bytes.instance_id

LEFT JOIN 
(SELECT 
instance_id,ebs_write_bytes_count,ebs_write_bytes_max,ebs_write_bytes_avg
-- ,ebs_write_bytes_d1,ebs_write_bytes_d2,ebs_write_bytes_d3,ebs_write_bytes_d4,ebs_write_bytes_d5,ebs_write_bytes_d6,ebs_write_bytes_d7,ebs_write_bytes_d8,ebs_write_bytes_d9,ebs_write_bytes_d10,ebs_write_bytes_d11,ebs_write_bytes_d12,ebs_write_bytes_d13,ebs_write_bytes_d14
FROM "sg-customer-2022-sb"."dd_ec2_ebs_write_bytes_2weeks") as ebs_write_bytes
on a.instance_id = ebs_write_bytes.instance_id

LEFT JOIN 
(
SELECT
line_item_usage_account_id
,line_item_resource_id
,product_instance_type
,product_usagetype
,product_vcpu
,product_dedicated_ebs_throughput
,product_memory
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
	and line_item_operation like '%RunInstances%'
	and product_servicename = 'Amazon Elastic Compute Cloud'
GROUP BY 1,2,3,4,5,6,7,8,9
) as cost
on a.instance_id = cost.line_item_resource_id
;