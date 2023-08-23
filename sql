WITH cte_gcp_cost  AS
  
(
SELECT 
billing_gcp_labeled.invoice_month as billing_date,
EXTRACT(date FROM billing_gcp_labeled.usage_start_time) as date,
sum(billing_gcp_labeled.cost) as gcp_cost
FROM `cloud.billing_gcp_labeled` billing_gcp_labeled
WHERE cost_allocation = 'training'
AND invoice_month >= '2021-01-01'
GROUP BY invoice_month, usage_start_time),

cte_autodeploy

SELECT 
billing_date,
SUM(number_of_running_hours) AS number_of_running_hours,
SUM(number_of_runs) AS number_of_runs
FROM `trax-ortal-prod.cloud.databand_agg`
WHERE user = 'auto_deploy'
AND state = 'success'
GROUP BY 1) autodeploy ON autodeploy.billing_date = STRING(gcp_cost.billing_date)

select
dr.state,
cloud,
user,
FORMAT_DATETIME('%Y-%m-01',start_time) as billing_date,
FORMAT_DATETIME('%Y-%m-%d',start_time) as date,
extract(hour from SUM(end_time - start_time)) as number_of_running_hours,
COUNT(id) as number_of_runs
from databand_public.dbnd_run dr
left join (
select distinct dtrv2.run_id from (select run_id,id from databand_public.dbnd_task_run_v2 where is_system=false ) dtrv2
join databand_public.dbnd_task_run_attempt dtra on dtra.task_run_id=dtrv2.id
join (select uid from databand_public.dbnd_error  where msg != 'exiting band due to: Some of datasets created are not valid, look for logs for more info') de on dtra.latest_error_uid = de.uid
) a on a.run_id=dr.id
where 1=1 -- dr.user = 'auto_deploy'
and dag_id = 'AutoTrainPipeline'
and (dr.state !='failed' or (dr.state ='failed' and a.run_id is not null))
and dr.start_time >=  '2021-01-01'
GROUP BY 1,2,3,4,5
ORDER BY 4,5



