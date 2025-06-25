#!/bin/sh

# 
#  Copyright (C) 2025 Volt Active Data Inc.
# 
#  Use of this source code is governed by an MIT
#  license that can be found in the LICENSE file or at
#  https://opensource.org/licenses/MIT.
# 

. $HOME/.profile

cd ../jars 

USERCOUNT=100000
KAF=1
DURATION=3600
VDBHOSTS=`cat $HOME/.vdbhostnames`
KAFKAHOSTS=vdb1
RUNSTART=`date '+%y%m%d_%H%M'`
KAFKAPORT=9092

for j in 2 5 10 20 30 40  50 60 70 80 90 100
do 
	echo $j
	echo "delete from cdr_dupcheck;" | sqlcmd --servers=${VDBHOSTS}
	sleep 60
	for i in 0 100000 200000 300000 400000 500000 600000 700000 800000 900000
	do
       		nohup java  ${JVMOPTS} -jar voltdb-aggdemo-client.jar ${VDBHOSTS} ${KAFKAHOSTS} ${USERCOUNT} $j ${DURATION} 1000 1000 1000 10000 $i ${KAF} ${KAFKAPORT} 10000 > ${RUNSTART}_${j}_${i}.out &
		sleep 1
	done
	wait
	sleep 60
done

