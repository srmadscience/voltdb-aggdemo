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
DURATION=600
VDBHOSTS=`cat $HOME/.vdbhostnames`
KAFKAHOSTS=vdb1
RUNSTART=`date '+%y%m%d_%H%M'`
KAFKAPORT=9092
START_TPS=10
INC_TPS=5
MAX_TPS=200
TC=5


TMPFILE=/tmp/$$.tmp

echo "upsert into mediation_parameters (parameter_name ,parameter_value) VALUES ('AGG_QTYCOUNT',1);"| sqlcmd --servers=${VDBHOSTS}

for KAF in 0 1
do

	echo Use Kafka is $KAF

	STATFILE=${HOME}/log/${RUNSTART}_${KAF}_stat.txt
	CURRENT_TPS=${START_TPS}

	while
		[ "${CURRENT_TPS}" -le "${MAX_TPS}" ]
	do 
		echo ${CURRENT_TPS}
		echo "delete from cdr_dupcheck;" | sqlcmd --servers=${VDBHOSTS}
		sleep 6

		ACTUAL_TPS_THIS_RUN=`expr ${CURRENT_TPS} / ${TC}`
		ACTUAL_USERCOUNT_THIS_RUN=`expr ${USERCOUNT} / ${TC}`

		CURRENT_TC=1

		while
			[ "${CURRENT_TC}" -le "${TC}" ]
		do
			ACTUAL_OFFSET_THIS_RUN=`expr ${CURRENT_TC} \* ${ACTUAL_USERCOUNT_THIS_RUN}`
			OUTFILE=${HOME}/log/${RUNSTART}_${CURRENT_TPS}_${ACTUAL_OFFSET_THIS_RUN}_${KAF}.out
			rm  ${OUTFILE} 2> /dev/null
       			java ${JVMOPTS} -jar voltdb-aggdemo-client.jar ${VDBHOSTS} ${KAFKAHOSTS} ${USERCOUNT} ${ACTUAL_TPS_THIS_RUN} ${DURATION} 1000 1000 1000 10000 ${ACTUAL_OFFSET_THIS_RUN} ${KAF} ${KAFKAPORT} 10000  >  ${OUTFILE} &
			sleep 1

			CURRENT_TC=`expr $CURRENT_TC + 1`
		done

		wait

		grep GREPABLE  ${OUTFILE} >> ${STATFILE}

		grep UNABLE_TO_MEET_REQUESTED_TPS ${OUTFILE} > ${TMPFILE}

		if 
			[ -s ${TMPFILE} ]
		then
			echo Unable to do requested TPS
			cat ${TMPFILE}
			rm ${TMPFILE}
			break
		else
			rm ${TMPFILE}
		fi

		CURRENT_TPS=`expr ${CURRENT_TPS} + ${INC_TPS}`

	done
	sleep 6

done

