#!/usr/bin/python
import commands
import re
import sys

db='bigbench'
reducerNum=23

keys=sys.argv[1:]
if len(keys)!=0:
	if len(keys)==1:
		print "error joined key number"
		exit(1)
else:
	keys=['web_sales.ws_item_sk','item.i_item_sk']
for ki in range(len(keys)):
	keys[ki]=keys[ki].split('.')

js=[]
ts=[]
for ki in range(len(keys)-1):
	js.append("%s.%s=%s.%s" %(keys[ki][0],keys[ki][1],keys[ki+1][0],keys[ki+1][1]))
	ts.append(keys[ki][0])
ts.append(keys[-1][0])
jquery="SELECT %s from %s where %s;" %(keys[0][1],','.join(ts),' and '.join(js))

query="use %s;" %db
query+="set mapred.reduce.tasks=%d;" %reducerNum
query+="insert overwrite table test_joined_keys "+jquery
query+="set hive.mapred.partitioner=org.apache.hadoop.mapred.lib.TotalOrderPartitioner;"
query+="add file /tmp/_partition.lst;"
query+="create temporary table test_tmp_%s as %s" %('_'.join(ts),jquery)
print query
(status, output)=commands.getstatusoutput("hive -e '%s'" %query)
print output
