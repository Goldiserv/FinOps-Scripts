SELECT 
	ce_service_map
, line_item_line_item_type
, line_item_product_code
, sum(line_item_unblended_cost) as "line_item_unblended_cost"
FROM "athena_db_name"."cur_to_ce_service_map" 
WHERE true
and year = '2021'
and month = '8'
GROUP BY 1,2,3
-- limit 1000;