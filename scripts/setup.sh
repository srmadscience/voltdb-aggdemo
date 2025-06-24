#!/bin/sh

# 
#  Copyright (C) 2025 Volt Active Data Inc.
# 
#  Use of this source code is governed by an MIT
#  license that can be found in the LICENSE file or at
#  https://opensource.org/licenses/MIT.
# 

cd /home/ubuntu
. ./.profile

cd voltdb-aggdemo/scripts

sleep 120
sqlcmd --servers=`cat $HOME/.vdbhostnames` < ../ddl/voltdb-aggdemo-createDB.sql

$HOME/bin/reload_dashboards.sh aggdemo.json

java ${JVMOPTS} -jar $HOME/bin/addtodeploymentdotxml.jar `cat $HOME/.vdbhostnames` deployment topics.xml

cd
git clone https://github.com/srmadscience/volt-activeSD101.git
