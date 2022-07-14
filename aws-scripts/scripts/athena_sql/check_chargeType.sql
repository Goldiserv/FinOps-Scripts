-- Check match between line_item_line_item_type grouping and cost explorer's 'Charge Type'
SELECT 
	line_item_line_item_type
, sum(line_item_unblended_cost) as "line_item_unblended_cost"
FROM "athena_db_name"."cur_tbl_cost_management" 
WHERE TRUE
AND year = '2021'
AND month = '8'
GROUP BY 1
limit 100;

-- matches CE for month of Aug 2021. Only diff is line_item_line_item_type = 'fee' matches both support and other out of cycle fees https://console.aws.amazon.com/cost-management/home?#/custom?groupBy=RecordType&hasBlended=false&hasAmortized=false&excludeDiscounts=true&excludeTaggedResources=false&excludeCategorizedResources=false&excludeForecast=false&timeRangeOption=Custom&granularity=Monthly&reportName=&reportType=CostUsage&isTemplate=true&startDate=2021-08-01&endDate=2021-08-31&filter=%5B%5D&forecastTimeRangeOption=None&usageAs=usageQuantity&chartStyle=Stack
