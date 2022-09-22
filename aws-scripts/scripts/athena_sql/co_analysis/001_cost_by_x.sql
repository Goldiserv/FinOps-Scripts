--extracts cost data for a month, potentially large csv file

SELECT 
line_item_usage_account_id
,line_item_line_item_type
,line_item_product_code
,line_item_usage_type
,line_item_operation
,line_item_availability_zone
,line_item_resource_id
,line_item_unblended_rate
,line_item_line_item_description
,product_availability_zone
,product_cache_engine
,product_calling_type
,product_database_engine
,product_dedicated_ebs_throughput
,product_deployment_option
,product_description
,product_instance_type
,product_marketoption
,product_memory
,product_network_performance
,product_operating_system
,product_operation
,product_region
,product_servicename
,product_usagetype
,product_vcpu
,pricing_term
,resource_tags_user_area
,resource_tags_user_create_method
,resource_tags_user_env
,resource_tags_user_envtype
,resource_tags_user_project
,resource_tags_user_service
,resource_tags_user_team
,year
,month
,sum(line_item_usage_amount) as line_item_usage_amount
,sum(line_item_unblended_cost) as line_item_unblended_cost
,sum(reservation_effective_cost) as reservation_effective_cost
,sum(discount_edp_discount) as discount_edp_discount
,sum(discount_private_rate_discount) as discount_private_rate_discount
,sum(discount_total_discount) as discount_total_discount
,sum(pricing_public_on_demand_cost) as pricing_public_on_demand_cost
,sum(savings_plan_savings_plan_effective_cost) as savings_plan_savings_plan_effective_cost
,sum(savings_plan_net_amortized_upfront_commitment_for_billing_period) as savings_plan_net_amortized_upfront_commitment_for_billing_period
,sum(savings_plan_net_recurring_commitment_for_billing_period) as savings_plan_net_recurring_commitment_for_billing_period
FROM
	"athenacurcfn_s_b_c_u_r"."sb_cur_new"  --"athena_db_name".cur_tbl_cost_management
WHERE true
	and year = '2022'
	and month = '3'
GROUP BY 
1
,2
,3
,4
,5
,6
,7
,8
,9
,10
,11
,12
,13
,14
,15
,16
,17
,18
,19
,20
,21
,22
,23
,24
,25
,26
,27
,28
,29
,30
,31
,32
,33
,34
,35
,36