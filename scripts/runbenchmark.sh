#!/bin/sh

cd ../jars 

USERCOUNT=100000
KAF=1
DURATION=600
VDBHOSTS=`cat $HOME/.vdbhostnames`
RUNSTART=`date '+%y%m%d_%H%M'`
KAFKAPORT=9092
START_TPS=10
INC_TPS=5
MAX_TPS=200
BUSY_CACHE=10000

CURRENT_TPS=${START_TPS}

echo "upsert into mediation_parameters (parameter_name ,parameter_value) VALUES ('AGG_QTYCOUNT',1);"| sqlcmd --servers=${VDBHOSTS}

while
	[ "${CURRENT_TPS}" -le "${MAX_TPS}" ]
do 
	echo ${CURRENT_TPPS}
	echo "delete from cdr_dupcheck;" | sqlcmd --servers=${VDBHOSTS}
	sleep 6
	for i in 0 
	do
		OUTFILE=${RUNSTART}_${CURRENT_TPS}_${i}_${KAF}.out
		rm  ${OUTFILE} 2> /dev/null
       		java -jar voltdb-aggdemo-client.jar ${VDBHOSTS} ${USERCOUNT} ${CURRENT_TPS} ${DURATION} 1000 1000 1000 10000 $i ${KAF} ${KAFKAPORT} ${BUSY_CACHE} >  ${OUTFILE}

		grep UNABLE_TO_MEET_REQUESTED_TPS ${OUTFILE} > /tmp/$$.tmp

		if 
			[ "$?" != "0"  -o -s /tmp/$$.tmp ]
		then
			echo Unable to do requested TPS
			rm /tmp/$$.tmp
			break
		else
			rm /tmp/$$.tmp
		fi
	done
	sleep 6

	CURRENT_TPS=`expr ${CURRENT_TPS} + ${INC_TPS}`
done
