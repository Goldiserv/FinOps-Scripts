//Refreshes "athena_db_name".cur_tbl1_v02_cur_mod upon a new CUR drop

const AWS = require('aws-sdk');
const AthenaExpress = require("athena-express");

const athenaExpressConfig = {
	aws: AWS,
	s3: "s3://aws-athena-query-results-AWS_ACCT_ID-ap-southeast-2/cur_tbl-refresh-athena-upon-new-cur",
	getStats: false
};

const athenaExpress = new AthenaExpress(athenaExpressConfig);
// relies on https://www.npmjs.com/package/athena-express v7.1.0 which needs to be added as a lambda layer

exports.handler = async () => {
	async function queryAthena() {
		let myQuery = {
			sql: `CREATE OR REPLACE VIEW "athena_db_name".cur_tbl1_v02_cur_mod AS
						SELECT tbl_a.* 
						FROM
						(
							SELECT
								* 
								, CASE
										WHEN line_item_line_item_type = 'Tax' THEN 'Tax($)'		
										WHEN line_item_product_code = 'AWSSupportBusiness' THEN 'Support (Business)($)'
										WHEN line_item_product_code = 'AWSSupportEnterprise' THEN 'Premium Support($)'
										WHEN line_item_product_code = 'AWSSupportDeveloper' THEN 'Support (Developer)($)'
										WHEN 
											line_item_product_code = 'AmazonEC2'
											AND line_item_operation LIKE 'RunInstances%' --includes box, spot, and ec2 data transfer
											THEN 'EC2-Instances($)'
										WHEN product_product_name = 'Amazon Elastic Compute Cloud' 
											OR line_item_product_code = 'AmazonEC2' THEN 'EC2-Other($)'
										-- credits are mapped to svc name, not 'credit'
										-- edp is mapped to svc name, not 'edp
										WHEN ce_svc_temp IS NOT NULL THEN ce_svc_temp
										ELSE CONCAT(REPLACE(REPLACE(product_product_name, 'AWS ', ''), 'Amazon ', ''),'($)')
										END AS "ce_service"
								, CASE
										WHEN pricing_public_on_demand_cost < line_item_unblended_cost THEN line_item_unblended_cost
										ELSE pricing_public_on_demand_cost
										END AS "public_od_cost_adj"
								, SUBSTR(CAST(date_parse(CONCAT(year, '/', month),'%Y/%m') AS VARCHAR),1,7) AS "year_month"
							FROM (
								SELECT a.*, b.ce_svc as "ce_svc_temp", c.account_name FROM
									(
									SELECT * 
									FROM "athena_db_name".cur_tbl_cost_management 
									WHERE true
										-- line_item_product_code IN ('4a9locqnp90c8riprmo8q6dn7', 'AmazonCloudWatch') -- for testing
										and year IN ('2020','2021','2022','2023','2024','2025','2026','2027','2028','2029')
										-- and (month = '8' OR month = '7')
									) as a
								LEFT JOIN cur_tbl1_v01_cur_to_ce_svc_map as b
								ON a.line_item_product_code = b.product_code
								LEFT JOIN cur_tbl_aws_accounts as c
								ON a.line_item_usage_account_id = c.account_id
								-- WHERE ce_svc IS NOT NULL AND TRIM(ce_svc) <> ' '
								-- LIMIT 100
							)
						) tbl_a
						`,
			db: "athena_db_name"
		};

		try {
			let results = await athenaExpress.query(myQuery);
			console.log(results);
		} catch (error) {
			console.log(error);
		}
	}
	await queryAthena();
};
