#!/bin/sh 

#-------------------------------------------------------------------------------
# Copyright 2014 Umesh Kanitkar
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#-------------------------------------------------------------------------------

uuid=`uuidgen`
echo $uuid;
endDirInclude=/tmp/${uuid}-last-copy-to-hdfs-dirname; 
startDirExclude=/tmp/${uuid}-last-merge-to-mvdb-dirname; 
cd ${ETL_DIR}
mvn -Dlogback.configurationFile=${BIGDATA_DIR}/configuration/logback.xml exec:java -Dexec.mainClass="com.mvdb.etl.actions.FetchConfigValue" -Dexec.args="--customer alpha --name last-copy-to-hdfs-dirname -outputFile $endDirInclude"
mvn -Dlogback.configurationFile=${BIGDATA_DIR}/configuration/logback.xml exec:java -Dexec.mainClass="com.mvdb.etl.actions.FetchConfigValue" -Dexec.args="--customer alpha --name last-merge-to-mvdb-dirname -outputFile $startDirExclude"
echo "startDirExclude:`cat $startDirExclude`"
echo "endDirInclude:`cat $endDirInclude`"
hadoop jar /home/umesh/work/BigData/mvdb/target/mvdb-0.0.1.jar  /data/alpha `cat $startDirExclude` `cat $endDirInclude`
cd ${BIGDATA_DIR}/usage-details
exit 0
