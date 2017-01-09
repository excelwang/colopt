#!/usr/bin/python
import re
import os

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