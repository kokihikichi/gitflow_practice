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

drop table if exists prabhakarbha01.log_rat_agg_sess;
create table prabhakarbha01.log_rat_agg_sess (
    session_id string,
    seq string,
    purchase_ind int,
    basket_ind int,
    cnt int
    )
partitioned by (dt string)
stored as ORC;
