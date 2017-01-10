#!/usr/bin/python
#import os
'''
#!/bin/bash
li=1
ri=2
ltable=target
rtable=as

echo ''>l.stat
for i in {0..1}
do
    hive --orcfiledump --rowindex $li "hdfs://bigdata122:8020/user/hive/warehouse/$ltable/00000${i}_0" >>l.stat
done

echo ''>r.stat
for i in {0..2}
do
    hive --orcfiledump --rowindex $ri "hdfs://bigdata122:8020/user/hive/warehouse/$rtable/00000${i}_0" >>r.stat
done
'''
import re
import os
queryname='q1'
statFiles=('l.stat','r.stat')
#Entry 0:count: 10000 hasNull: false min: 65946577 max: 65956576 sum: 659515765000 positions: 0,0,0
enpattern=re.compile("^\s+Entry\s+\d+:count:\s+(\d+).*min:\s+(\S+)\s+max:\s+(\S+)\s+sum:\s(\S+)")
ghist={}#group hist

def fetchHist(statFile):
    stat=open(statFile)
    ghist[statFile]=[]
    for line in stat:
        enmatch=enpattern.match(line)
        if enmatch:
            #min,max,cnt,sum
            #todo fix int
            ghist[statFile].append((int(enmatch.group(2)),int(enmatch.group(3)),int(enmatch.group(1)),int(enmatch.group(2))))          
    #ghist[statFile].sort(key=lambda p:int(p[2]))#int
    #sorted(hist,key=lambda p:p[2])#string

bucketSize=10000# target record num. per bucket
bucketStep=0# key range size for bucket partition
fhist={}#file hist
#fix int
fmin=0
fmax=0
for t in statFiles:
    fetchHist(t)
    fhist[t]={"min":0,"max":0,"cnt":0,"sum":0}#todo fix min,max
    for e in ghist[t]:
        fhist[t]['cnt']+=e[2]#fix long
        fhist[t]['sum']+=e[3]#fix long
        if e[0]<fhist[t]['min']:
            fhist[t]['min']=e[0]
        if e[1]>fhist[t]['max']:
            fhist[t]['max']=e[1]
    fhist[t]['step']=int((fhist[t]['max']-fhist[t]['min']+1)/float(fhist[t]['cnt'])*bucketSize)
    if bucketStep<fhist[t]['step']:
        bucketStep=fhist[t]['step']
    if fmin>fhist[t]['min']:
        fmin=fhist[t]['min']
    if fmax<fhist[t]['max']:
        fmax=fhist[t]['max']
b0=fmin/bucketStep
bucketCnt=fmax/bucketStep-b0+1
bhist={}#bucket hist
for t in statFiles:
    bhist[t]=[0 for x in range(bucketCnt)]#bucket histogram
    for e in ghist[t]:
        lb=e[0]/bucketStep
        hb=e[1]/bucketStep
        if lb==hb:
            bhist[t][lb-b0]+=e[2]
        else:
            eweight=float(e[2])/(e[1]-e[0]+1)
            lbslice=eweight*((lb+1)*bucketStep-e[0])
            hbslice=eweight*(e[1]-hb*bucketStep+1)
            bhist[t][lb-b0]+=lbslice
            bhist[t][hb-b0]+=hbslice
            rbCnt=hb-lb-1
            if rbCnt>0:
                bslice=(e[2]-hbslice-lbslice)/rbCnt
                for bi in range(lb+1,hb):
                    bhist[t][bi-b0]+=bslice

whist=[]#worload hist for each bucket.
totalWorkload=0
for i in range(bucketCnt):
    #workload of bucket i. todo: use weighted intermidate kv.
    bw=bhist[statFiles[0]][i]+bhist[statFiles[1]][i]
    whist.append(bw)
    totalWorkload+=bw

###generate the partition ranges:
pars=[]
reduceNum=13
#totoal reduce input, 
avgLoad=totalWorkload/reduceNum
wSum=0
for i in range(len(whist)):
    wSum+=whist[i]
    if wSum>avgLoad:
        p=i
        needRetreat=wSum-avgLoad>avgLoad-wSum+whist[i] and len(pars)!=0 and i!=pars[-1][0]
        if needRetreat:
            wSum-=whist[i]
            p-=1
        rangeH=(p+1)*bucketStep-1#todo fix int
        rangeL=(fmin if len(pars)==0 else pars[-1][1]+1)
        pars.append((rangeL,rangeH,wSum))
        if needRetreat:
            wSum=whist[i]
        else:
            wSum=0
    elif i==len(whist)-1:
        pars.append((pars[-1][1]+1,fmax,wSum))
if len(pars)!=reduceNum:
    print "Error! partiton num. is %d, while reducer num. is %d" %(len(pars),reduceNum)
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