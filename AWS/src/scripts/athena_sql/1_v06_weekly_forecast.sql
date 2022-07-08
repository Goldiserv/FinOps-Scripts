-- Determines forecast cost for the week
-- Works by taking the last 3 days of usage and extrapolating out to 1 week
-- Ignores the first hour of each day to avoid counting 1st of month cost spikes

CREATE OR REPLACE VIEW "athena_db_name".cur_tbl1_v06_weekly_forecast AS
SELECT 
account_name
, resource_tags_user_app
, sum(tbl_a.public_od_cost_adj) / (72-3) * (7*24) as public_od_cost_adj_wk_forecast
-- sum of cost over 3 days excl 12am extrapolated out to 1 week 
FROM
"athena_db_name".cur_tbl1_v02_cur_mod as tbl_a

WHERE 
line_item_usage_start_date > date_add('day', -7, current_date)

AND line_item_usage_start_date < (SELECT
    max("line_item_usage_start_date") as date_before
FROM "athena_db_name".cur_tbl_cost_management
WHERE product_product_name = 'Amazon QuickSight')

AND line_item_usage_start_date >= (SELECT
    date_add('day', -3, max("line_item_usage_start_date")) as date_on_or_after
FROM "athena_db_name".cur_tbl_cost_management
WHERE product_product_name = 'Amazon QuickSight')

AND hour(line_item_usage_start_date) <> 0

GROUP BY 1,2