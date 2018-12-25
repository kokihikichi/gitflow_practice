SET mapred.job.name=bharat_gms;
SET mapred.job.queue.name=bis-batch;
SET hive.cli.print.header=TRUE;
SET hive.vectorized.execution.enabled = TRUE;
SET hive.vectorized.execution.reduce.enabled = TRUE;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.groupby.orderby.position.alias = true;
SET hive.exec.max.dynamic.partitions=1000000;
SET hive.exec.max.dynamic.partitions.pernode=1000000;
SET hive.exec.max.created.files=500000;
SET hive.metastore.client.socket.timeout = 60000;


insert into table prabhakarbha01.log_rat_genre
partition (dt, pid)
select
    a.easy_id,
    a.session_id,
    a.page_key,
    case
        when next_time_stamp is NULL then "time_sessend"
        when unix_timestamp(next_time_stamp)-unix_timestamp(time_stamp) < 1 then "time_00_01"
        when 1 <= unix_timestamp(next_time_stamp)-unix_timestamp(time_stamp) and unix_timestamp(next_time_stamp)-unix_timestamp(time_stamp) < 5 then "time_01_05"
        when 5 <= unix_timestamp(next_time_stamp)-unix_timestamp(time_stamp) and unix_timestamp(next_time_stamp)-unix_timestamp(time_stamp) < 10 then "time_05_10"
        when 10 <= unix_timestamp(next_time_stamp)-unix_timestamp(time_stamp) and unix_timestamp(next_time_stamp)-unix_timestamp(time_stamp) < 15 then "time_10_15"
        when 15 <= unix_timestamp(next_time_stamp)-unix_timestamp(time_stamp) and unix_timestamp(next_time_stamp)-unix_timestamp(time_stamp) < 30 then "time_15_30"
        when 30 <= unix_timestamp(next_time_stamp)-unix_timestamp(time_stamp) and unix_timestamp(next_time_stamp)-unix_timestamp(time_stamp) < 60 then "time_30_60"
        when 60 <= unix_timestamp(next_time_stamp)-unix_timestamp(time_stamp) then "time_60_00"
        else "time_unk" end as duration_key,
    a.purchase_ind,
    a.basket_ind,
    a.purchase_time_stamp,
    a.browsing_itemid,
    a.price,
    a.search_word,
    unix_timestamp(next_time_stamp)-unix_timestamp(time_stamp) as duration,
    a.page_name,
    a.domain,
    a.time_stamp,
    a.url,
    a.genre_path,
    a.genre_cnt,
    a.item_cnt,
    a.n_reviews,
    a.n_items,
    a.n_searches,
    a.n_bookings,
    a.n_baskets,
    a.genre_cnt/a.item_cnt as genre_purity,
    a.next_page_key,
    a.prev_page_key,
    a.next_time_stamp,
    a.prev_time_stamp,
    a.dt,
    a.easy_id%100 as pid
from
    (
        select
            a.*,
            sum(case when a.genre_path is not null then 1 else 0 end) over (partition by a.session_id) as genre_cnt,
            sum(case when a.browsing_itemid is not null then 1 else 0 end) over (partition by a.session_id) as item_cnt,
            sum(case when page_key = "review" then 1 else 0 end) over (partition by a.session_id) as n_reviews,
            sum(case when page_key = "item" then 1 else 0 end) over (partition by a.session_id) as n_items,
            sum(case when page_key like "%search%" then 1 else 0 end) over (partition by a.session_id) as n_searches,
            sum(case when page_name in ("Cart:Purchase", "sp_Cart:Purchase", "step5_purchase_complete") then 1 else 0 end) over (partition by a.session_id) as n_bookings,
            sum(case when page_key like "%step%" then 1 else 0 end) over (partition by a.session_id) as n_baskets,
            lead(a.page_key, 1) over (partition by a.session_id order by time_stamp asc) as next_page_key,
            lag(a.page_key, 1) over (partition by a.session_id order by time_stamp asc) as prev_page_key,
            lead(a.time_stamp, 1) over (partition by a.session_id order by time_stamp asc) as next_time_stamp,
            lag(a.time_stamp, 1) over (partition by a.session_id order by time_stamp asc) as prev_time_stamp
        from
            (select * from prabhakarbha01.log_rat_raw where dt >= ${hiveconf:start_date} and dt < ${hiveconf:end_date}) as a
    ) as a
where a.genre_cnt/a.item_cnt >= 0.5
;