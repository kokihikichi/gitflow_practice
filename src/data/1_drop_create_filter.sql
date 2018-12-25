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

drop table if exists prabhakarbha01.log_rat_genre;
create table prabhakarbha01.log_rat_genre (
    easy_id int,
    session_id string,
    page_key string,
    duration_key string,
    purchase_ind int,
    basket_ind int,
    purchase_time_stamp string,
    browsing_itemid string,
    price int,
    search_word string,
    duration int,
    page_name string,
    domain string,
    time_stamp string,
    url string,
    genre_path string,
    genre_cnt int,
    item_cnt int,
    n_reviews int,
    n_items int,
    n_searches int,
    n_bookings int,
    n_baskets int,
    genre_purity float,
    next_page_key string,
    prev_page_key string,
    next_time_stamp string,
    prev_time_stamp string
    )
partitioned by (dt string, pid int)
clustered by (easy_id) INTO 2048 buckets
stored as ORC;