-- Produces data required for net amortized cost reporting by app (in the cur_tbl account)
-- Includes 1month, 3m, 11m lookback

CREATE OR REPLACE VIEW "athena_db_name".cur_tbl2_v04_net_amortized_cur_tbl_app AS
SELECT 
  CASE WHEN a.resource_tags_user_app = '' THEN 'No Tagkey: App' ELSE a.resource_tags_user_app END AS resource_tags_user_app
  , CAST('' as varchar) AS comments
  , service_total_12m
  , M0_Annualised 
  , (M0_Annualised - service_total_12m) / service_total_12m AS service_total_12m_pct
  , Mn12_daily_cost
  , Mn3_daily_cost
  , Mn1_daily_cost
  , a.daily_cost AS M0_daily_cost
  , M0_vs_Mn1
  , M0_vs_Mn1_monthly
  , M0_vs_Mn1_annual
  , M0_vs_Mn1_pct
  , M0_vs_Mn3
  , M0_vs_Mn3_monthly
  , M0_vs_Mn3_annual
  , M0_vs_Mn3_pct
  , M0_vs_Mn12
  , M0_vs_Mn12_monthly
  , M0_vs_Mn12_annual
  , M0_vs_Mn12_pct
  , Mn12_daily_cost * 365.0 AS Mn12_annual_cost
  , M0_monthly
FROM
(
  SELECT a.*
    , a.daily_cost * 365.0 M0_Annualised
    , coalesce(tbl_Mn1.daily_cost,0.0) Mn1_daily_cost
    , a.daily_cost - coalesce(tbl_Mn1.daily_cost,0.0) M0_vs_Mn1
    , (a.daily_cost - coalesce(tbl_Mn1.daily_cost,0.0)) * 365.0 / 12.0 M0_vs_Mn1_monthly
    , (a.daily_cost - coalesce(tbl_Mn1.daily_cost,0.0)) * 365.0 M0_vs_Mn1_annual
    , CASE WHEN coalesce(tbl_Mn1.daily_cost,0.0) = 0.0 THEN 1.0 ELSE (a.daily_cost - tbl_Mn1.daily_cost) / tbl_Mn1.daily_cost END AS M0_vs_Mn1_pct

    , coalesce(tbl_Mn3.daily_cost,0.0) Mn3_daily_cost
    , a.daily_cost - coalesce(tbl_Mn3.daily_cost,0.0) M0_vs_Mn3 
    , (a.daily_cost - coalesce(tbl_Mn3.daily_cost,0.0)) * 365.0 / 12.0 M0_vs_Mn3_monthly
    , (a.daily_cost - coalesce(tbl_Mn3.daily_cost,0.0)) * 365.0 M0_vs_Mn3_annual
    , CASE WHEN coalesce(tbl_Mn3.daily_cost,0.0) = 0.0 THEN 1.0 ELSE (a.daily_cost - tbl_Mn3.daily_cost) / tbl_Mn3.daily_cost END AS M0_vs_Mn3_pct

    , coalesce(tbl_Mn12.daily_cost,0.0) Mn12_daily_cost
    , a.daily_cost - coalesce(tbl_Mn12.daily_cost,0.0) M0_vs_Mn12 
    , (a.daily_cost - coalesce(tbl_Mn12.daily_cost,0.0)) * 365.0 / 12.0 M0_vs_Mn12_monthly
    , (a.daily_cost - coalesce(tbl_Mn12.daily_cost,0.0)) * 365.0 M0_vs_Mn12_annual
    , CASE WHEN coalesce(tbl_Mn12.daily_cost,0.0) = 0.0 THEN 1.0 ELSE (a.daily_cost - tbl_Mn12.daily_cost) / tbl_Mn12.daily_cost END AS M0_vs_Mn12_pct
    , monthly_cost AS M0_monthly
  FROM
    (
      SELECT 
        tbl_apps.*
        , coalesce(tbl_cost.monthly_cost, 0.0 ) AS monthly_cost
        , coalesce(tbl_cost.monthly_cost, 0.0 ) / tbl_dates.days_in_month AS daily_cost
        , tbl_dates.*
      FROM 
      (
        -- all cur_tbl acct tags
        SELECT 
          resource_tags_user_app
        FROM "athena_db_name".cur_tbl_cost_management
        WHERE true
          and line_item_usage_account_id = 'AWS_ACCT_ID' -- cur_tbl acct
        GROUP BY 1
      ) tbl_apps
      -- join spend
      LEFT JOIN
      (
        SELECT 
          resource_tags_user_app
          , sum(net_amortized_cost) AS monthly_cost
        FROM "athena_db_name".cur_tbl2_v01_net_amortized 
        WHERE true
          and year = cast(year(DATE_ADD('month', -1, date_trunc('month',(current_date)))) as varchar)
          and month = cast(month(DATE_ADD('month', -1, date_trunc('month',(current_date)))) as varchar)
          and line_item_usage_account_id = 'AWS_ACCT_ID' -- cur_tbl acct
        GROUP BY 1
      ) tbl_cost
      ON tbl_apps.resource_tags_user_app = tbl_cost.resource_tags_user_app
      -- join dates
      LEFT JOIN 
      (
        SELECT 
          DATE_ADD('month', -1, bill_billing_period_start_date) as date_Mn1
          , DATE_ADD('month', -3, bill_billing_period_start_date) as date_Mn3
          , DATE_ADD('month', -11, bill_billing_period_start_date) as date_Mn12 -- actually comparing 11 months prior to prev month, not 12
          , DAY(DATE_ADD('day', -1, bill_billing_period_end_date)) AS days_in_month
        FROM "athena_db_name".cur_tbl_cost_management 
        WHERE true
          and year = cast(year(DATE_ADD('month', -1, date_trunc('month',(current_date)))) as varchar)
          and month = cast(month(DATE_ADD('month', -1, date_trunc('month',(current_date)))) as varchar)
        LIMIT 1
      ) tbl_dates
      ON true
    ) a

    -- 1 month ago
    LEFT JOIN  
    (SELECT 
      resource_tags_user_app
      , bill_billing_period_start_date
      , sum(net_amortized_cost) / max(DAY(DATE_ADD('day', -1, bill_billing_period_end_date))) AS daily_cost
    FROM "athena_db_name".cur_tbl2_v01_net_amortized 
    WHERE true
      and year = cast(year(DATE_ADD('month', -2, date_trunc('month',(current_date)))) as varchar)
      and month = cast(month(DATE_ADD('month', -2, date_trunc('month',(current_date)))) as varchar)
      and line_item_usage_account_id = 'AWS_ACCT_ID' -- cur_tbl acct
    GROUP BY 1,2) tbl_Mn1
    ON a.date_Mn1 = tbl_Mn1.bill_billing_period_start_date
    and a.resource_tags_user_app = tbl_Mn1.resource_tags_user_app

    -- 3 months ago
    LEFT JOIN  
    (SELECT 
      resource_tags_user_app
      , bill_billing_period_start_date
      , sum(net_amortized_cost) / max(DAY(DATE_ADD('day', -1, bill_billing_period_end_date))) AS daily_cost
    FROM "athena_db_name".cur_tbl2_v01_net_amortized 
    WHERE true
      and year = cast(year(DATE_ADD('month', -4, date_trunc('month',(current_date)))) as varchar)
      and month = cast(month(DATE_ADD('month', -4, date_trunc('month',(current_date)))) as varchar)
      and line_item_usage_account_id = 'AWS_ACCT_ID' -- cur_tbl acct
    GROUP BY 1,2) tbl_Mn3
    ON a.date_Mn3 = tbl_Mn3.bill_billing_period_start_date
    and a.resource_tags_user_app = tbl_Mn3.resource_tags_user_app

    -- 12 months ago
    LEFT JOIN  
    (SELECT 
      resource_tags_user_app
      , bill_billing_period_start_date
      , sum(net_amortized_cost) / max(DAY(DATE_ADD('day', -1, bill_billing_period_end_date))) AS daily_cost
    FROM "athena_db_name".cur_tbl2_v01_net_amortized 
    WHERE true
      and year = cast(year(DATE_ADD('month', -12, date_trunc('month',(current_date)))) as varchar)
      and month = cast(month(DATE_ADD('month', -12, date_trunc('month',(current_date)))) as varchar) -- actually comparing 11 months prior to prev month, not 12
      and line_item_usage_account_id = 'AWS_ACCT_ID' -- cur_tbl acct
    GROUP BY 1,2) tbl_Mn12
    ON a.date_Mn12 = tbl_Mn12.bill_billing_period_start_date
    and a.resource_tags_user_app = tbl_Mn12.resource_tags_user_app
) a
-- 12 months annual spend
LEFT JOIN  
(SELECT 
  resource_tags_user_app
  -- , bill_billing_period_start_date
  , sum(net_amortized_cost) AS service_total_12m
FROM "athena_db_name".cur_tbl2_v01_net_amortized 
WHERE true
  and bill_billing_period_start_date between DATE_ADD('month', -12, date_trunc('month',(current_date))) and DATE_ADD('day', -1, date_trunc('month',(current_date)))
  and line_item_usage_account_id = 'AWS_ACCT_ID' -- cur_tbl acct
GROUP BY 1
) tbl_12m_total
ON a.resource_tags_user_app = tbl_12m_total.resource_tags_user_app

ORDER BY M0_Annualised DESC