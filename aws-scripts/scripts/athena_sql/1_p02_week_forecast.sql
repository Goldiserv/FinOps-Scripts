SELECT 
	-- (DAY_OF_WEEK(max("line_item_usage_start_date")) * 1.0 + hour(max("line_item_usage_start_date")) / 24.0) / 7.0 as pct_of_week_superseded, this gives monday = 1 and sunday = 7
	, ( (CASE WHEN DAY_OF_WEEK(max("line_item_usage_start_date")) = 7 THEN 0 ELSE DAY_OF_WEEK(max("line_item_usage_start_date")) END) * 1.0 + hour(max("line_item_usage_start_date")) / 24.0) / 7.0 as pct_of_week
FROM
	"athena_db_name".cur_tbl_cost_management
WHERE true
	and year = CAST(year(current_date) as varchar) 
	and month = CAST(month(current_date) as varchar) 
	and product_product_name = 'Amazon QuickSight'
-- Amazon Quicksight used to determine max("line_item_usage_start_date") as it provides a consistent result