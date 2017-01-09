--ref:https://cwiki.apache.org/confluence/display/Hive/HBaseBulkLoad
create external table range_keys(p int)
row format serde
"org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe"
stored as
inputformat
"org.apache.hadoop.mapred.TextInputFormat"
outputformat
"org.apache.hadoop.hive.ql.io.HiveNullValueSequenceFileOutputFormat"
location "/tmp/range_key_list";
set mapred.reduce.tasks=1;
add jar /opt/cloudera/parcels/CDH/jars/hive-contrib-1.1.0-cdh5.9.0.jar;
create temporary function row_sequence as 'org.apache.hadoop.hive.contrib.udf.UDFRowSequence';
insert overwrite table range_keys
select targetid from
(select row_sequence() num, targetid
from `as` tablesample(BUCKET 1 OUT OF 10000 ON targetid) a
limit 13000) x
where num%1000=0
order by targetid
limit 12;