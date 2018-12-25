

select time_stamp
from rat_log.rat_log_normalized
where
    dt >= ${hiveconf:start}
    and dt < ${hiveconf:end}
    and service_type = 'ichiba_jp'
    and event_type = 'pv'
    and complemented_easy_id > 0
    and device_code = 0
    and lower(user_agent) not like '%ichibaapp-jp%'
limit 10
;