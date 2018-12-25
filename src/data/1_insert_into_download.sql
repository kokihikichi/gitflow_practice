SET mapred.job.name=bharat_gms;
SET mapred.job.queue.name=bis;
SET hive.cli.print.header=TRUE;
SET hive.vectorized.execution.enabled = TRUE;
SET hive.vectorized.execution.reduce.enabled = TRUE;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.groupby.orderby.position.alias = true;
SET hive.exec.max.dynamic.partitions=1000000;
SET hive.exec.max.dynamic.partitions.pernode=1000000;
SET hive.exec.max.created.files=500000;
SET hive.metastore.client.socket.timeout = 60000;

insert into prabhakarbha01.log_rat_agg_sess
partition(dt)
select
    session_id,
    seq,
    purchase_ind,
    basket_ind,
    cnt,
    dt
from (
        select 
            session_id,
            regexp_replace(concat_ws(' ', sort_array(collect_list(concat_ws('_____', cast(unix_timestamp(time_stamp) as string), concat_ws(" ", page_key, duration_key_v2))))), '[0-9]+_____', '') as seq,
            max(purchase_ind) as purchase_ind,
            max(basket_ind) as basket_ind,
            count(1) as cnt,
            dt
        from 
            (
                select 
                    *,
                    case
                        when next_time_stamp is NULL then "time_sessend"
                        when unix_timestamp(next_time_stamp)-unix_timestamp(time_stamp) < 1 then "time_00_01"
                        when 1 <= unix_timestamp(next_time_stamp)-unix_timestamp(time_stamp) and unix_timestamp(next_time_stamp)-unix_timestamp(time_stamp) < 5 then "time_01_05"
                        when 5 <= unix_timestamp(next_time_stamp)-unix_timestamp(time_stamp) and unix_timestamp(next_time_stamp)-unix_timestamp(time_stamp) < 10 then "time_05_10"
                        when 10 <= unix_timestamp(next_time_stamp)-unix_timestamp(time_stamp) and unix_timestamp(next_time_stamp)-unix_timestamp(time_stamp) < 15 then "time_10_15"
                        when 15 <= unix_timestamp(next_time_stamp)-unix_timestamp(time_stamp) and unix_timestamp(next_time_stamp)-unix_timestamp(time_stamp) < 30 then "time_15_30"
                        when 30 <= unix_timestamp(next_time_stamp)-unix_timestamp(time_stamp) and unix_timestamp(next_time_stamp)-unix_timestamp(time_stamp) < 60 then "time_30_60"
                        when 60 <= unix_timestamp(next_time_stamp)-unix_timestamp(time_stamp) then "time_60_00"
                        else "time_unk" end as duration_key_v2
                from 
                    prabhakarbha01.log_rat_genre 
                where 
                    dt >= ${hiveconf:start_date} 
                    and dt < ${hiveconf:end_date}
            ) as a
        group by 
            session_id, dt
    ) as a
where
    cnt > 4
;


