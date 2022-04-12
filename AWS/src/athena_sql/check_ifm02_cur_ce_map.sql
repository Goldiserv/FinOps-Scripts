SELECT 
 ce_svc
, ce_service_map_2
, line_item_line_item_type
, line_item_product_code
, month
, sum(line_item_unblended_cost) as "line_item_unblended_cost"
FROM "athena_db_name"."cur_tbl02_cur_ce_map" 
WHERE true
and year = '2021'
and (month = '8' OR month = '7')
GROUP BY 1,2,3,4,5
-- limit 1000;