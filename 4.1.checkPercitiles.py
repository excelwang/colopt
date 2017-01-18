#!/usr/bin/python
import commands
import re
import sys

partitionNum=23
table=sys.argv[1] if len(sys.argv)>1 else 'test_joined_keys'
key=sys.argv[2] if len(sys.argv)>2 else 'k'

ps=(str(float(i+1)/partitionNum) for i in range(partitionNum-1))
ps=','.join(ps)
query="use bigbench;"
query+="create table if not exists test_par_lst_(p bigint);insert overwrite table test_par_lst_ "
query+="select 1+pts.p from (select explode(percentile(%s,array(%s))) as p from %s) pts;" %(key,ps,table)
query+="set mapred.reduce.tasks=1;"
query+="insert overwrite table test_par_lst select distinct(p) from test_par_lst_;"
query+="!rm -rf /tmp/_partition.lst;dfs -get /tmp/range_key_list/000000_0 /tmp/_partition.lst;"
print query
(status, output)=commands.getstatusoutput("hive -e '%s'" %query)
print output
ptsNum=re.compile(r'.*numRows=(\d+)', re.DOTALL).match(output).group(1)
print 'partition point number is:',ptsNum
