-- Feeds Quicksight dropdown for selecting an app

CREATE OR REPLACE VIEW "athena_db_name".cur_tbl1_v03_app_selector AS
SELECT 
	distinct(resource_tags_user_app)
FROM "athena_db_name"."cur_tbl_cost_management"