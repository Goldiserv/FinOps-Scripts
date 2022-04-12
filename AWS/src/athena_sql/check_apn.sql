  SELECT 
    year
    , month
    , line_item_usage_account_id
    , bill_billing_period_start_date
    , bill_billing_period_end_date
    , line_item_line_item_description
    , bill_bill_type
    , line_item_product_code
    , bill_payer_account_id
    , bill_billing_entity
    , sum(
      line_item_net_unblended_cost 
      + reservation_net_effective_cost 
      + reservation_net_unused_amortized_upfront_fee_for_billing_period
      + reservation_net_unused_recurring_fee
      + savings_plan_net_savings_plan_effective_cost
      ) net_amortized_cost
  FROM "athena_db_name".cur_tbl_cost_management
  WHERE true
    and year IN ('2020','2021','2022','2023','2024','2025','2026')
    -- and month IN ('5')
    -- and (account_name = 'examples' or line_item_usage_account_id = '238053348499')
    and line_item_line_item_type <> 'Tax'
    and line_item_line_item_type <> 'Credit'
    and line_item_line_item_type NOT IN ('SavingsPlanRecurringFee', 'SavingsPlanUpfrontFee')
    and line_item_line_item_description NOT LIKE 'Sign up charge for subscription%' 
    -- and (line_item_line_item_description NOT LIKE 'Sign up charge for subscription%' OR line_item_product_code = 'APNFee') -- includes APN fee, $2.5k in Dec.
    and product_product_name <> 'New Relic One: the Observability Platform - Annual Pool of Funds'
    -- and line_item_usage_account_id = 'AWS_ACCT_ID'
  GROUP BY 1,2,3,4,5,6,7,8,9,10

-- , sum(line_item_net_unblended_cost) line_item_net_unblended_cost
-- , sum(line_item_unblended_cost) line_item_unblended_cost
-- , sum(pricing_public_on_demand_cost) pricing_public_on_demand_cost
-- , sum(reservation_amortized_upfront_cost_for_usage) reservation_amortized_upfront_cost_for_usage
-- , sum(reservation_amortized_upfront_fee_for_billing_period) reservation_amortized_upfront_fee_for_billing_period
-- , sum(reservation_effective_cost) reservation_effective_cost
-- , sum(reservation_net_amortized_upfront_cost_for_usage) reservation_net_amortized_upfront_cost_for_usage
-- , sum(reservation_net_amortized_upfront_fee_for_billing_period) reservation_net_amortized_upfront_fee_for_billing_period
-- , sum(reservation_net_effective_cost) reservation_net_effective_cost
-- , sum(reservation_net_recurring_fee_for_usage) reservation_net_recurring_fee_for_usage
-- , sum(reservation_net_unused_amortized_upfront_fee_for_billing_period) reservation_net_unused_amortized_upfront_fee_for_billing_period
-- , sum(reservation_net_unused_recurring_fee) reservation_net_unused_recurring_fee
-- , sum(reservation_net_upfront_value) reservation_net_upfront_value
-- , sum(reservation_recurring_fee_for_usage) reservation_recurring_fee_for_usage
-- , sum(reservation_unused_amortized_upfront_fee_for_billing_period) reservation_unused_amortized_upfront_fee_for_billing_period
-- , sum(reservation_unused_recurring_fee) reservation_unused_recurring_fee
-- , sum(reservation_upfront_value) reservation_upfront_value
-- , sum(savings_plan_amortized_upfront_commitment_for_billing_period) savings_plan_amortized_upfront_commitment_for_billing_period
-- , sum(savings_plan_net_amortized_upfront_commitment_for_billing_period) savings_plan_net_amortized_upfront_commitment_for_billing_period
-- , sum(savings_plan_net_recurring_commitment_for_billing_period) savings_plan_net_recurring_commitment_for_billing_period
-- , sum(savings_plan_net_savings_plan_effective_cost) savings_plan_net_savings_plan_effective_cost
-- , sum(savings_plan_recurring_commitment_for_billing_period) savings_plan_recurring_commitment_for_billing_period
-- , sum(savings_plan_savings_plan_effective_cost) savings_plan_savings_plan_effective_cost
-- , sum(savings_plan_used_commitment) savings_plan_used_commitment
