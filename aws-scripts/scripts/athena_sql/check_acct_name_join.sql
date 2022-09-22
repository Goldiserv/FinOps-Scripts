SELECT *
FROM
(SELECT 
  line_item_usage_account_id
FROM "athena_db_name".cur_tbl_cost_management 
WHERE true
GROUP BY 1) a
LEFT JOIN
(SELECT 
  account_id
  , account_name
FROM "athena_db_name".cur_tbl_aws_accounts
) tbl_acct_names
ON a.line_item_usage_account_id = tbl_acct_names.account_name
ORDER BY account_id DESC
