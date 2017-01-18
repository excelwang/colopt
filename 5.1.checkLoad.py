#!/usr/bin/python
import commands
import re
import sys

db='bigbench'
table=sys.argv[1] if len(sys.argv)>1 else 'test_joined_keys'
key=sys.argv[2] if len(sys.argv)>2 else 'k'

(status, output)=commands.getstatusoutput("hive -e 'use %s;select * from test_par_lst_;'" %db)
output=re.compile(r'^.*OK\n(.*?)\nTime taken', re.DOTALL).match(output).group(1)
parLst=[-9999999999999999]
for line in output.split("\n"):
	parLst.append(long(line))
parLst.append(9999999999999999)
query="select par,count(par) from (select (CASE"
for pi in range(len(parLst)-1):
	query+="\n\tWHEN %s>=%d and %s<%d THEN %d" %(key,parLst[pi],key,parLst[pi+1],pi)
query+="\n\tEND) par from %s) pars group by par order by par;" %table
print "query: %s\n" %query
(status, output)=commands.getstatusoutput("hive -e 'use %s;%s'" %(db,query))
#print output
pars=re.compile(r'.*OK\n(.*)\nTime taken', re.DOTALL).match(output).group(1).split("\n")
print "load:"
for pi in range(len(pars)):
	print "\t[%d,%d):%s" %(parLst[pi],parLst[pi+1],pars[pi].split()[1])
