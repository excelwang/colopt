#!/bin/bash
jarname="hive-exec-1.1.0-cdh5.9.0.jar"
targeDir="/opt/cloudera/parcels/CDH/jars"
for i in {101..126}
do
        host="10.0.82.${i}"
        echo $host
        #ssh root@$host "mv ${targeDir}/${jarname} ${targeDir}/${jarname}.bak"
        ssh root@$host "scp root@10.0.82.101:/root/$jarname ${targeDir}/"
done
rm -rf /root/$jarname