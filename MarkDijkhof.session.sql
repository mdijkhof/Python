select distinct deal_uuid from sandbox.unagi
where report_date = (select max(report_date) from sandbox.unagi)
sample 50
