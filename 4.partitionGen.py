#!/usr/bin/python
import re
import os
import json

#TODO: and join need to re-grep rowindex
#web_sales.ws_order_number=web_returns.wr_order_number and web_sales.ws_item_sk = web_returns.wr_item_sk #q16
#customer_address.ca_address_sk = customer.c_current_addr_sk and customer.c_customer_sk = store_sales.ss_customer_sk #q07
#TODO: date keys
#joinedFields=["web_sales.ws_sold_date_sk","date_dim.d_date_sk"] #q16\q11
#joinedFields=["store_returns.sr_returned_date_sk","date_dim.d_date_sk"] #q21

#normal keys
#joinedFields=["web_sales.ws_item_sk","item.i_item_sk"] #q29\q24\q16:
joinedFields=["web_sales.ws_item_sk","item.i_item_sk"] #q29:
#joinedFields=["store_sales.ss_item_sk","item.i_item_sk"] #q26\q12\q01\q07
#joinedFields=["web_sales.ws_warehouse_sk","warehouse.w_warehouse_sk"] #q16
#joinedFields=["web_clickstreams.wcs_item_sk","item.i_item_sk"] #q12
#joinedFields=["web_clickstreams.wcs_user_sk","store_sales.ss_customer_sk"] #q12
#joinedFields=["product_reviews.pr_item_sk","web_sales.ws_item_sk"] #q11

bucketsFolder='stats/buckets/'
reduceNum=140
debug=True

totalWorkload=0
#all bucket
buckets=[]
for f in joinedFields:
    with open(bucketsFolder+f) as jsonFile:
        stats=json.load(jsonFile)
        totalWorkload+=stats['cnt'] #todo: mapjoin switcher
        print f+" contributed load: "+str(stats['cnt'])+". From "+str(stats['min'])+" to "+str(stats['max'])
        buckets.append([float("-inf"),stats['min'],0,0])
        buckets.extend(stats['buckets'])
buckets.sort(key=lambda p:p[1])

#split at better level, by clipping the shuffled cards
splitedBuckets=[]
for i in range(len(buckets)-1,0,-1):
    clippedPart=[buckets[i-1][1],buckets[i][1],0]
    if clippedPart[0]<=buckets[i][0]: #to fix:may induce larger empty range
        clippedPart[2]=buckets.pop(i)[2]
    else:
	if buckets[i][1]==buckets[i-1][1]:
		continue
        clippedPart[2]=buckets[i][3]*(buckets[i][1]-buckets[i-1][1])
        buckets[i][2]-=clippedPart[2]
        buckets[i][1]=clippedPart[0]
    for j in range(len(buckets)-1,i,-1):
        if clippedPart[0]<=buckets[j][0]:
            clippedPart[2]+=buckets.pop(j)[2]
            continue
        clippedLoad=buckets[j][3]*(buckets[j][1]-clippedPart[0])
        clippedPart[2]+=clippedLoad
        buckets[j][2]-=clippedLoad
        buckets[j][1]=clippedPart[0]
    splitedBuckets.append(clippedPart)

if debug:
	sload=0
	for sbi in range(0,len(splitedBuckets)-1):
    		sload+=splitedBuckets[sbi][2]
		if splitedBuckets[sbi+1][0]>splitedBuckets[sbi][0]:
			print 'error in splitedBuckets:'+str(sbi)
			print splitedBuckets[sbi]
			print splitedBuckets[sbi+1]
	error=(totalWorkload-sload+splitedBuckets[-1][2])/totalWorkload
	print "Debug:splitedBuckets error rate:"+str(error)
	if abs(error)>0.01:
		print "Splited load error rate too big:"+str(error)
		exit(1)

#generate the partition ranges:
parlst=[]
parSum=0
avgLoad=totalWorkload/reduceNum
print "Average load: "+str(avgLoad)
for b in reversed(splitedBuckets):
    parSum+=b[2]
    remain=parSum-avgLoad
    if remain<0:
        continue
    b[0]=b[1]-long(remain/(float(b[2])/(b[1]-b[0])))
    b[2]=remain
    parlst.append(b[0])
    parSum=b[2]
if len(parlst)==reduceNum-1:
    rate=parSum/avgLoad
    if rate<0.99:
        print "the last partition is not balanced: "+str(100*rate)+"% of the average load"
        exit(1)
elif len(parlst)==reduceNum:
    parlst.pop()
    rate=1+parSum/avgLoad
    print "the last partition is "+str(100*rate)+"% of average load"
    if rate>1.01:
        print "the last partition is not balanced: "+str(100*rate)+"% of the average load"
        exit(1)
else:
    print "Error! partiton num. is %d, while reducer num. is %d" %(len(parlst),reduceNum)
    exit(1)
if debug:
	tlst=[splitedBuckets[-1][0]]
	tlst.extend(parlst)
	tlst.append(splitedBuckets[0][1])
	ranges=[]
	for i in range(0,len(tlst)-1):
		print "Partition %d: [%d,%d)" %(i+1,tlst[i],tlst[i+1])
		ranges.append(tlst[i+1]-tlst[i])
	ranges.sort()
	print "partition ranges: ",ranges
exit(0)

loadString='\n'.join('\t'.join(str(num) for num in p) for p in pars)
print loadString
loaddir="parlsts/load/"
plstdir="parlsts/lst/%s" %queryname
print os.system("mkdir -p %s" %loaddir)
loadFile=open("%s%s" %(loaddir,queryname),"w")
loadFile.write(loadString)
#test the HiveKey.Comparator
#pars=[(0,0,0),(1,0,0),(2,0,0),(4,0,0),(9,0,0),(800000,0,0),(1234567,0,0),(1234568,0,0),(6066543,0,0),(19876543,0,0),(29876543,0,0),(100876540,0,0),(1000076540,0,0)]
plst=[]
for i in range(len(pars)-1):
    #partiton points list: if a,b,c: then [-inf,a),[a+1?,b)...
    plst.append("("+str(pars[i+1][0])+")")
print "Preprocess finished!\n\n\n"

queryPlst=""
queryPlst+="set hive.mapred.partitioner=org.apache.hadoop.hive.ql.io.DefaultHivePartitioner;"
queryPlst+="!sleep 60;select imageid,count(*) from target t,as a where t.id=a.targetid group by imageid;"
queryPlst+="!sleep 60;select imageid,count(*) from target t,as a where t.id=a.targetid group by imageid;"
queryPlst+="!sleep 60;select imageid,count(*) from target t,as a where t.id=a.targetid group by imageid;"
queryPlst+="!sleep 60;select imageid,count(*) from target t,as a where t.id=a.targetid group by imageid;"
queryPlst+="!sleep 60;select imageid,count(*) from target t,as a where t.id=a.targetid group by imageid;"
queryPlst+="add file /tmp/_partition.lst;"
queryPlst+="!sleep 60;set hive.mapred.partitioner=org.apache.hadoop.mapred.lib.TotalOrderPartitioner;"
queryPlst+="!sleep 60;select imageid,count(*) from target t,as a where t.id=a.targetid group by imageid;"
queryPlst+="!sleep 60;select imageid,count(*) from target t,as a where t.id=a.targetid group by imageid;"
queryPlst+="!sleep 60;select imageid,count(*) from target t,as a where t.id=a.targetid group by imageid;"
queryPlst+="!sleep 60;select imageid,count(*) from target t,as a where t.id=a.targetid group by imageid;"
queryPlst+="!sleep 60;select imageid,count(*) from target t,as a where t.id=a.targetid group by imageid;"
queryPlst+="!sleep 60;set mapreduce.totalorderpartitioner.naturalorder=false;"
queryPlst+="!sleep 60;select imageid,count(*) from target t,as a where t.id=a.targetid group by imageid;"
queryPlst+="!sleep 60;select imageid,count(*) from target t,as a where t.id=a.targetid group by imageid;"
queryPlst+="!sleep 60;select imageid,count(*) from target t,as a where t.id=a.targetid group by imageid;"
queryPlst+="!sleep 60;select imageid,count(*) from target t,as a where t.id=a.targetid group by imageid;"
queryPlst+="!sleep 60;select imageid,count(*) from target t,as a where t.id=a.targetid group by imageid;"
#todo: explode or set total.order.partitioner.path/mapreduce.totalorderpartitioner.path, http://stackoverflow.com/questions/36302424/unable-to-use-totalorderpartitioner-with-hive-cant-read-partitions-file
queryPlst+="CREATE TABLE IF NOT EXISTS partition_lst(p bigint) STORED AS SEQUENCEFILE;INSERT OVERWRITE TABLE partition_lst VALUES "+','.join(plst)+';'
queryPlst+="INSERT OVERWRITE LOCAL DIRECTORY '%s' row format serde 'org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe' stored as inputformat 'org.apache.hadoop.hive.ql.io.CombineHiveInputFormat' outputformat 'org.apache.hadoop.hive.ql.io.HiveNullValueSequenceFileOutputFormat' select * from partition_lst;" %plstdir
queryPlst+="!cp -f %s/000000_0 /tmp/_partition.lst;" %plstdir
queryPlst+="add file /tmp/_partition.lst;"
queryPlst+="set hive.mapred.partitioner=org.apache.hadoop.mapred.lib.TotalOrderPartitioner;"
#queryPlst+="set hive.mapred.partitioner=org.apache.hadoop.hive.ql.exec.HiveTotalOrderPartitioner;"# java.io.IOException: wrong key class: org.apache.hadoop.io.BytesWritable is not class org.apache.hadoop.hive.ql.io.HiveKey at org.apache.hadoop.io.SequenceFile$Reader.next(SequenceFile.java:2336)
#queryPlst+="!sleep 60;set mapreduce.totalorderpartitioner.naturalorder=false;"#disabe trie is unnesscery
queryPlst+="select imageid,count(*) from target t,as a where t.id=a.targetid group by imageid;"
print os.system("hive -e \"%s\"" %queryPlst)
