#!/usr/bin/python
import commands
import re

ps=(str(float(i+1)/143) for i in range(142))
ps=','.join(ps)
query="use bigbench;"
query+="insert overwrite table test_range_keys_1 "
query+="select explode(percentile(targetid,array(%s))) from test_targetid;" %ps
query+="set mapred.reduce.tasks=1;"
#query+="create external table if not exists test_range_keys(p bigint) row format serde 'org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe' stored as inputformat 'org.apache.hadoop.mapred.TextInputFormat' outputformat 'org.apache.hadoop.hive.ql.io.HiveNullValueSequenceFileOutputFormat' location '/tmp/range_key_list';"
query+="insert overwrite table test_range_keys select distinct(cast (targetid as bigint)) from test_range_keys_1;"
query+="!rm -rf /tmp/_partition.lst;dfs -get /tmp/range_key_list/000000_0 /tmp/_partition.lst;"
print query
(status, output)=commands.getstatusoutput("hive -e '%s'" %query)
ptsNum=re.compile(r'.*numRows=(\d+)', re.DOTALL).match(output).group(1)
print ptsNum
