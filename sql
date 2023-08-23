WITH cte_gcp_cost  AS
  
(
SELECT 
'GCP' as cloud,
'No Tag' as databand_tag,
billing_gcp_labeled.invoice_month as billing_date,
EXTRACT(date FROM billing_gcp_labeled.usage_start_time) as date,
sum(billing_gcp_labeled.cost) as gcp_cost
FROM `cloud.billing_gcp_labeled` billing_gcp_labeled
WHERE billing_gcp_labeled.cost_allocation = 'training'
AND billing_gcp_labeled.invoice_month >= '2021-01-01'
GROUP BY 
billing_gcp_labeled.invoice_month, 
billing_gcp_labeled.usage_start_time),

cte_aws_cost_network AS
(SELECT
FORMAT_DATETIME('%Y-%m-01',billing_aws_labeled.bill_billing_period_start_date) as billing_date,
FORMAT_DATETIME('%Y-%m-%d',DATE(substring(billing_aws_labeled.identity_time_interval,1,10))) as date,
'AWS' as cloud,
'Network' as databand_tag,
SUM(CASE
      WHEN (billing_aws_labeled.line_item_line_item_type = 'SavingsPlanCoveredUsage') THEN
      savings_plan_savings_plan_effective_cost
      WHEN (billing_aws_labeled.line_item_line_item_type = 'SavingsPlanRecurringFee') THEN
      (billing_aws_labeled.savings_plan_total_commitment_to_date - billing_aws_labeled.savings_plan_used_commitment)
      WHEN (billing_aws_labeled.line_item_line_item_type = 'SavingsPlanNegation') THEN
      0
      WHEN (billing_aws_labeled.line_item_line_item_type = 'SavingsPlanUpfrontFee') THEN
      0
      WHEN (billing_aws_labeled.line_item_line_item_type = 'DiscountedUsage') THEN
      billing_aws_labeled.reservation_effective_cost
      WHEN (billing_aws_labeled.line_item_line_item_type = 'RIFee') THEN
      (billing_aws_labeled.reservation_unused_amortized_upfront_fee_for_billing_period + billing_aws_labeled.reservation_unused_recurring_fee)
      WHEN ((billing_aws_labeled.line_item_line_item_type = 'Fee')
          AND (billing_aws_labeled.reservation_reservation_a_r_n <> '')) THEN
      0
      ELSE billing_aws_labeled.line_item_unblended_cost END) as total_cost_aws
FROM cloud.billing_aws_labeled billing_aws_labeled
WHERE 1=1
AND FORMAT_DATETIME('%Y-%m-01',billing_aws_labeled.bill_billing_period_start_date) >= '2021-01-01'
AND billing_aws_labeled.line_item_usage_account_id = '619597279328'
AND billing_aws_labeled.product_servicename = 'AWS Data Transfer'
AND (billing_aws_labeled.line_item_usage_type LIKE '%DataTransfer-Out-Bytes%'
OR  billing_aws_labeled.line_item_usage_type LIKE '%AWS-Out-Bytes%'
OR  billing_aws_labeled.line_item_usage_type LIKE '%AWS-Out-ABytes%')
AND billing_aws_labeled.line_item_product_code = 'AmazonS3'
AND billing_aws_labeled.bill_payer_account_id = '619597279328'
WHERE billing_aws_labeled.cost_allocation = 'training' -- added by me, it was not part of the original view
GROUP BY
billing_date,
date
cloud)

cte_aws_cost_others AS
(SELECT 
FORMAT_DATETIME('%Y-%m-01',billing_aws_labeled.bill_billing_period_start_date) as billing_date_aws_actual,
FORMAT_DATETIME('%Y-%m-%d',DATE(substring(billing_aws_labeled.identity_time_interval,1,10))) as date,
'AWS' as cloud,
CASE when lower(billing_aws_labeled.resource_tags_user_name) LIKE '%research%' THEN 'Research'
when lower(billing_aws_labeled.resource_tags_user_service) LIKE '%research%' THEN 'Research'
when lower(billing_aws_labeled.resource_tags_user_name) LIKE '%prod-serving-training%' THEN 'Prod_Serving'
when lower(billing_aws_labeled.resource_tags_user_name) LIKE '%engine-dev%' THEN 'Engine_Dev'
when lower(billing_aws_labeled.resource_tags_user_name) LIKE '%autocoach%' THEN 'Autodeploy'
when lower(billing_aws_labeled.resource_tags_user_name) LIKE '%databand%' THEN 'Autodeploy'
when lower(billing_aws_labeled.resource_tags_user_service) LIKE '%databand%' THEN 'Autodeploy'
when lower(billing_aws_labeled.resource_tags_user_name) LIKE '%simon%' THEN 'Autodeploy'
when lower(billing_aws_labeled.resource_tags_user_name) LIKE '%tester%' THEN 'Autodeploy'
when lower(billing_aws_labeled.resource_tags_user_name) LIKE '%training%' THEN 'Autodeploy'
when lower(billing_aws_labeled.resource_tags_user_service) LIKE '%training%' THEN 'Autodeploy'
when lower(billing_aws_labeled.resource_tags_user_service) LIKE '%trainer%' THEN 'Research'
when lower(billing_aws_labeled.resource_tags_user_service) LIKE '%trainer%' THEN 'Research'
when lower(billing_aws_labeled.resource_tags_user_service) LIKE '%prod-autocoach-simon-cpu-on-demand-eks_asg%' THEN 'Platform Training Cost'
when lower(billing_aws_labeled.resource_tags_user_service) LIKE '%prod-autocoach%' THEN 'Platform Training Cost'
ELSE 'Platform Training Cost' END AS databand_tag,
SUM(CASE
      WHEN (billing_aws_labeled.line_item_line_item_type = 'SavingsPlanCoveredUsage') THEN
      savings_plan_savings_plan_effective_cost
      WHEN (billing_aws_labeled.line_item_line_item_type = 'SavingsPlanRecurringFee') THEN
      (billing_aws_labeled.savings_plan_total_commitment_to_date - savings_plan_used_commitment)
      WHEN (billing_aws_labeled.line_item_line_item_type = 'SavingsPlanNegation') THEN
      0
      WHEN (billing_aws_labeled.line_item_line_item_type = 'SavingsPlanUpfrontFee') THEN
      0
      WHEN (billing_aws_labeled.line_item_line_item_type = 'DiscountedUsage') THEN
      billing_aws_labeled.reservation_effective_cost
      WHEN (billing_aws_labeled.line_item_line_item_type = 'RIFee') THEN
      (billing_aws_labeled.reservation_unused_amortized_upfront_fee_for_billing_period + billing_aws_labeled.reservation_unused_recurring_fee)
      WHEN ((billing_aws_labeled.line_item_line_item_type = 'Fee')
          AND (billing_aws_labeled.reservation_reservation_a_r_n <> '')) THEN
      0
      ELSE billing_aws_labeled.line_item_unblended_cost END) as total_cost_aws
FROM cloud.billing_aws_labeled billing_aws_labeled
WHERE billing_aws_labeled.cost_allocation = 'training'
AND FORMAT_DATETIME('%Y-%m-01',bill_billing_period_start_date) >= '2021-01-01'
GROUP BY 1,2,3,4)

cte_aws_cost_total
  AS
(billing_date,
 date,
 cloud,
 databand_tag
 total_cost_aws
  from cte_aws_cost_network
  UNION ALL
  (select 
 billing_date,
 date,
 cloud,
 databand_tag
 total_cost_aws
from cte_aws_cost_others)),

cte_aws_cost AS
  (SELECT 
cte_aws_cost_total.billing_date,
SUM(IF (cte_aws_cost_total.databand_tag = 'Autodeploy', cte_aws_cost_total.total_cost_aws , 0)) as autodeploy_cost,
SUM(cte_aws_cost_total.total_cost_aws) as training_cost
FROM cte_aws_cost_total
GROUP BY billing_date)

cte_task_run_info AS
(
SELECT
dbnd_task_run_v2.run_id,
dbnd_task_run_v2.id 
FROM databand_public.dbnd_task_run_v2  dbnd_task_run_v2 
JOIN databand_public.dbnd_task_run_attempt dbnd_task_run_attempt  on dbnd_task_run_attempt.task_run_id=dbnd_task_run_v2.id
JOIN (select dbnd_error.uid from databand_public.dbnd_error dbnd_error  where dbnd_error.msg != 'exiting band due to: Some of datasets created are not valid, look for logs for more info') dbnd_error on dbnd_task_run_attempt.latest_error_uid = dbnd_error.uid),

cte_autodeploy AS
(
SELECT 
dbnd_run.state,
dbnd_run.cloud,
dbnd_run.user,
FORMAT_DATETIME('%Y-%m-01',dbnd_run.start_time) as billing_date,
FORMAT_DATETIME('%Y-%m-%d',dbnd_run.start_time) as date,
extract(hour from SUM(dbnd_run.end_time - dbnd_run.start_time)) as number_of_running_hours,
COUNT(dbnd_run.id) as number_of_runs
FROM 
`trax-ortal-prod.databand_public.dbnd_run`  dbnd_run
LEFT JOIN cte_task_run_info on cte_task_run_info.run_id=dbnd_run.id
WHERE dbnd_run.user = 'auto_deploy'
AND dbnd_run.dag_id = 'AutoTrainPipeline'
AND (dbnd_run.state !='failed' or (dbnd_run.state ='failed' and cte_task_run_info.run_id is not null))
AND dbnd_run.start_time >=  '2021-01-01'
GROUP BY 
dbnd_run.state,
dbnd_run.cloud,
dbnd_run.user,
billing_date,
date
ORDER BY
billing_date,
date
) 






