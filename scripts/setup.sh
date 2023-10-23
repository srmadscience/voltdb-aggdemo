#!/bin/sh

cd /home/ubuntu
. ./.profile

cd voltdb-aggdemo/scripts

sleep 120
sqlcmd --servers=`cat $HOME/.vdbhostnames` < ../ddl/voltdb-aggdemo-createDB.sql

$HOME/bin/reload_dashboards.sh aggdemo.json

java -jar $HOME/bin/addtodeploymentdotxml.jar `cat $HOME/.vdbhostnames` deployment topics.xml

cd
git clone https://github.com/srmadscience/volt-activeSD101.git
