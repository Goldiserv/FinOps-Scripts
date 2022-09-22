SELECT * FROM
  (
  SELECT * 
  FROM "athena_db_name".cur_tbl_cost_management 
  WHERE true
  and line_item_product_code IN ('4a9locqnp90c8riprmo8q6dn7', 'AmazonCloudWatch')
  and year = '2021'
  and month = '8'
  -- LIMIT 200
  ) as a
LEFT JOIN cur_tbl1_v01_cur_to_ce_svc_map as b
ON a.line_item_product_code = b.product_code
WHERE ce_svc IS NOT NULL AND TRIM(ce_svc) <> ' '
LIMIT 100