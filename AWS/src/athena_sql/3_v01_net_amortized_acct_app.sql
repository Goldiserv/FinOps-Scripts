-- Produces data required for net amortized cost reporting by account and app
-- Includes 1month, 3m, 11m lookback

CREATE OR REPLACE VIEW "athena_db_name".cur_tbl3_v01_net_amortized_acct_app AS
(
  SELECT a.*, tbl_acct_names.account_name as app_or_acct_name, a.account_id as sort_col
  FROM
  (
    SELECT 
        line_item_usage_account_id as account_id
        , sum(net_amortized_cost) AS monthly_cost
      FROM "athena_db_name".cur_tbl2_v01_net_amortized
      WHERE true
        and year = cast(year(DATE_ADD('month', -1, date_trunc('month',(current_date)))) as varchar)
        and month = cast(month(DATE_ADD('month', -1, date_trunc('month',(current_date)))) as varchar)
        and line_item_usage_account_id <> 'AWS_ACCT_ID' -- cur_tbl acct
      GROUP BY 1
      -- LIMIT 1
  ) a        
  -- account names
  LEFT JOIN
  (SELECT 
    account_id
    , account_name
  FROM "athena_db_name".cur_tbl_aws_accounts
  ) tbl_acct_names
  ON a.account_id = tbl_acct_names.account_id
  -- ORDER BY account_id, M0_Annualised DESC
)
UNION ALL
(
  SELECT 
    'AWS_ACCT_ID' as account_id
    , sum(net_amortized_cost) AS monthly_cost
    , CASE WHEN resource_tags_user_app = '' THEN 'No Tagkey: App' ELSE resource_tags_user_app END AS app_or_acct_name
    , '999999999999' as sort_col
  FROM "athena_db_name".cur_tbl2_v01_net_amortized 
  WHERE true
    and year = cast(year(DATE_ADD('month', -1, date_trunc('month',(current_date)))) as varchar)
    and month = cast(month(DATE_ADD('month', -1, date_trunc('month',(current_date)))) as varchar)
    and line_item_usage_account_id = 'AWS_ACCT_ID' -- cur_tbl acct
  GROUP BY resource_tags_user_app
  -- LIMIT 2
)
ORDER BY sort_col ASC