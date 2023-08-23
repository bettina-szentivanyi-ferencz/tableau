SELECT 
gcp_cost.billing_date,
autodeploy_cost + (discount_in_aws * percent_autodeploy_aws) as autodeploy_aws,
(gcp_cost.gcp_cost * percent_autodeploy_aws) as autodeploy_estimation_gcp,
autodeploy_cost + (gcp_cost.gcp_cost * percent_autodeploy_aws) + (discount_in_aws * percent_autodeploy_aws) as autodeploy_cost,
number_of_running_hours,
number_of_runs,
(autodeploy_cost + (discount_in_aws * percent_autodeploy_aws) + (gcp_cost.gcp_cost * percent_autodeploy_aws))/number_of_running_hours as cost_per_successful_training_running_time,
(autodeploy_cost + (discount_in_aws * percent_autodeploy_aws) + (gcp_cost.gcp_cost * percent_autodeploy_aws))/number_of_runs as  cost_per_successful_training_count_runs 
FROM
(SELECT 
aws_cost.billing_date,
autodeploy_cost,
autodeploy_cost/training_cost as percent_autodeploy_aws,
IFNULL(discount_per_label,0) as discount_in_aws,
gcp_cost
FROM
(SELECT 
billing_date,
SUM(IF (databand_tag = 'Autodeploy', total_cost_aws , 0)) as autodeploy_cost,
SUM(total_cost_aws) as training_cost
FROM `trax-ortal-prod.cloud.daily_training_aws`
GROUP BY 1) aws_cost
LEFT JOIN 
(SELECT 
billing_date,
SUM(total_cost) as gcp_cost
FROM `trax-ortal-prod.cloud.daily_training_gcp`
GROUP BY 1) gcp_cost ON STRING(gcp_cost.billing_date) = aws_cost.billing_date
LEFT JOIN 
(SELECT
billing_date,
discount_per_label
FROM `trax-ortal-prod.cloud.edp_discount_aws_cppp_training` as edp_dact
WHERE cost_allocation = 'training') discount_training ON (discount_training.billing_date = aws_cost.billing_date)
)
LEFT JOIN 
(SELECT 
billing_date,
SUM(number_of_running_hours) AS number_of_running_hours,
SUM(number_of_runs) AS number_of_runs
FROM `trax-ortal-prod.cloud.databand_agg`
WHERE user = 'auto_deploy'
AND state = 'success'
GROUP BY 1) autodeploy ON autodeploy.billing_date = STRING(gcp_cost.billing_date)
