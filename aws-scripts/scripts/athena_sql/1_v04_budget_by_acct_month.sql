-- Feeds Budget section of QS dashboard 

CREATE OR REPLACE VIEW "athena_db_name".cur_tbl1_v04_budget_by_acct_month AS
SELECT a.*, try_cast(b.budget AS double) as budget, c.account_name FROM
(
SELECT 
  line_item_usage_account_id
  , bill_billing_period_start_date
  , year
  , month
  , Concat(year,LPAD(month, 2, '0')) as yyyymm
  , sum(public_od_cost_adj) AS public_od_cost_adj
FROM (  
  SELECT 
    line_item_usage_account_id
    , bill_billing_period_start_date
    , year
    , month
    , CASE
        WHEN pricing_public_on_demand_cost < line_item_unblended_cost THEN line_item_unblended_cost
        ELSE pricing_public_on_demand_cost
        END AS "public_od_cost_adj"
  FROM
    "athena_db_name".cur_tbl_cost_management
  WHERE true
    and bill_billing_period_start_date >= timestamp '2021-06-01 00:00:00.000'
    --  and year = '2021'
    --  and month = '8'
)
GROUP BY 1,2,3,4,5
-- LIMIT 20
) as a
LEFT JOIN cur_tbl_aws_budget as b
ON (a.yyyymm = b.yyyymm and a.line_item_usage_account_id = b.account_id)
LEFT JOIN cur_tbl_aws_accounts as c
ON a.line_item_usage_account_id = c.account_id
