use bigbench;
--ref:https://cwiki.apache.org/confluence/display/Hive/HBaseBulkLoad
set mapred.reduce.tasks=143;
create table test_targetid as select ws_item_sk as targetid FROM web_sales ws, item i
    WHERE ws.ws_item_sk = i.i_item_sk
    AND i.i_category_id IS NOT NULL;
create external table test_range_keys(p bigint)
row format serde
"org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe"
stored as
inputformat
"org.apache.hadoop.mapred.TextInputFormat"
outputformat
"org.apache.hadoop.hive.ql.io.HiveNullValueSequenceFileOutputFormat"
location "/tmp/range_key_list";
set mapred.reduce.tasks=1;
add jar /opt/cloudera/parcels/CDH/jars/hive-contrib-1.1.0-cdh5.8.3.jar;
create temporary function row_sequence as 'org.apache.hadoop.hive.contrib.udf.UDFRowSequence';
insert overwrite table test_range_keys
select targetid from
(select row_sequence() num, targetid
from test_targetid
tablesample(BUCKET 1 OUT OF 10000 ON targetid) a
limit 143000) x
where num%1000=0
order by targetid
limit 142;
