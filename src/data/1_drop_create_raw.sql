SET mapred.job.name=bharat_batch;
SET mapred.job.queue.name=bis;
SET hive.cli.print.header=TRUE;
SET hive.vectorized.execution.enabled = TRUE;
SET hive.vectorized.execution.reduce.enabled = TRUE;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.groupby.orderby.position.alias = true;
SET hive.metastore.client.socket.timeout = 600;

drop table if exists prabhakarbha01.log_rat_raw;
create table prabhakarbha01.log_rat_raw (
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
    genre_path string)
partitioned by (dt string)
clustered by (easy_id) INTO 1024 buckets
stored as ORC;