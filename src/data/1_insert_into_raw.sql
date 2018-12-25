SET mapred.job.name=bharat_batch;
SET mapred.job.queue.name=bis-batch;
SET hive.cli.print.header=TRUE;
SET hive.vectorized.execution.enabled = TRUE;
SET hive.vectorized.execution.reduce.enabled = TRUE;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.groupby.orderby.position.alias = true;
SET hive.metastore.client.socket.timeout = 600;

insert into table prabhakarbha01.log_rat_raw
partition (dt)
select
    complemented_easy_id as easy_id,
    `30min_session_id` as session_id,
    case
        when page_name = "ichiba_top" then "ichiba_top"
        when page_name like "%campaign%" then "campaign"
        when domain = "item.rakuten.co.jp" and split(url, "/")[4] in ("c","s") then "in_shop_category"
        when page_name in ('Normal_item', 'Reserve_item', 'reserve_item', 'Regular_item', 'regular_item', 'Multi_Normal_item', 'multi_normal_item', 'shop_item', 'normal_item', 'item') or domain = "item.rakuten.co.jp" then "item"
        when lower(page_name) like "%category%" then "category"
        when page_name in ("step0_shopping_basket", "step1_login_register", "step2_shipping_address", "step3_payment_delivery", "step4_order_confirmation", "step5_purchase_complete") then page_name
        when domain = "basket.step.rakuten.co.jp" then "basket_others"
        when domain like "%review%" or page_name = "review" then "item_review"
        when domain like "%ranking%" then "ranking"
        when (page_name like "%search%" or domain = "search.rakuten.co.jp" or page_type like "%search%") and (custom_parameter["rpgn"] = "1") then "search_pg1"
        when (page_name like "%search%" or domain = "search.rakuten.co.jp" or page_type like "%search%") and (custom_parameter["rpgn"] = "2") then "search_pg2"
        when (page_name like "%search%" or domain = "search.rakuten.co.jp" or page_type like "%search%") and (custom_parameter["rpgn"] = "3") then "search_pg3"
        when (page_name like "%search%" or domain = "search.rakuten.co.jp" or page_type like "%search%") and (custom_parameter["rpgn"] = "4") then "search_pg4"
        when (page_name like "%search%" or domain = "search.rakuten.co.jp" or page_type like "%search%") then "search_pg5"
        when domain = "event.rakuten.co.jp" then "event"
        when domain = "my.bookmark.rakuten.co.jp" then "mybookmark"
        when page_type like "%gold%" or page_name like "%gold%" then "gold_shop"
        when domain = "order.my.rakuten.co.jp" then "order_related"
        when domain = "kuji.rakuten.co.jp" then "kuji"
        when domain = "www.rakuten.co.jp" then "ichiba_top"
        else "XXX"
        end as page_key,
    case
        when duration < 5 then "time_00_05"
        when 5 <= duration < 15 then "time_05_15"
        when 15 <= duration < 30 then "time_15_30"
        when 30 <= duration < 60 then "time_30_60"
        when 60 <= duration then "time_60_00"
        else "time_unk"
        end as duration_key,
    case
        when page_name in ("Cart:Purchase", "sp_Cart:Purchase", "step5_purchase_complete") then 1
        else 0
        end as purchase_ind,
    case
        when page_name like "%step%" then 1
        else 0
        end as basket_ind,
    case
        when page_name in ("Cart:Purchase", "sp_Cart:Purchase", "step5_purchase_complete") then time_stamp
        else NULL
        end as purchase_time_stamp,
    browsing_itemid[0] as browsing_itemid,
    b.price as price,
    search_word,
    duration,
    page_name,
    domain,
    time_stamp,
    url,
    b.genre_path,
    a.dt
from
    rat_log.rat_log_normalized as a
left outer join
    prabhakarbha01.shoes_master_men as b
on
    a.browsing_itemid[0] = b.rat_key
where
    a.dt >= ${hiveconf:start_date}
    and a.dt < ${hiveconf:end_date}
    and service_type = 'ichiba_jp'
    and event_type = 'pv'
    and complemented_easy_id > 0
    and device_code = 0
    and lower(user_agent) not like '%ichibaapp-jp%'
;