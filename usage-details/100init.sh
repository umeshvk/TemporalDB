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

touch ~/.mvdb/status.init.sh.started
mkdir -p ~/.mvdb
rm -f ~/.mvdb/status.*
rm -rf ~/.mvdb/etl/data
mkdir -p ~/.mvdb/etl/data
x=`hadoop fs -ls / | grep "^d.*/data$" | wc -l`
#echo "x=$x"
hadoop fs -rmr /data
rm -f ~/.mvdb/etl.init.properties
touch ~/.mvdb/etl.init.properties
echo "Minimally you will need a relational database and hdfs with hadoop setup for this project."
echo "Edit all the properties written to ~/.mvdb/etl.init.properties as per your environment before proceeding to next steps."
echo "data.root=~/.mvdb/etl/data" >> ~/.mvdb/etl.init.properties
echo "db.user=umesh" >> ~/.mvdb/etl.init.properties
echo "db.password=password" >> ~/.mvdb/etl.init.properties
echo "db.url=jdbc:postgresql:udb" >> ~/.mvdb/etl.init.properties
echo "hadoop.home=/home/umesh/ops/hadoop-1.2.0" >> ~/.mvdb/etl.init.properties
echo "hdfs.root=hdfs://localhost:9000" >> ~/.mvdb/etl.init.properties
echo "action.chain.status.file=ActionChainStatusFile" >> ~/.mvdb/etl.init.properties
touch ~/.mvdb/status.init.sh.complete
echo ">>init script completed"
