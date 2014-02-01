/*******************************************************************************
 * Copyright 2014 Umesh Kanitkar
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.mvdb.etl.actions;

public class ConfigurationKeys
{
    //Keys in configuration table
    public static final String GLOBAL_CUSTOMER = "global"; 
    public static final String GLOBAL_DB_URL = "db.url"; 
    public static final String GLOBAL_DB_USER = "db.user"; 
    public static final String GLOBAL_DB_PASSWORD = "db.password"; 
    
    public static final String GLOBAL_LOCAL_DATA_ROOT = "data.root"; 
    public static final String GLOBAL_HDFS_ROOT = "hdfs.root"; 
    public static final String GLOBAL_HADOOP_HOME = "hadoop.home"; 
    public static final String GLOBAL_ACTION_CHAIN_STATUS_FILE = "action.chain.status.file"; 

    //Use when implementing locking
    public static final String REFRESH_LOCK = "refresh-lock";     
    public static final String COPY_TO_HDFS_LOCK = "copy-to-hdfs-lock";
    public static final String MERGE_TO_HDFS_LOCK = "merge-to-mvdb-lock"; 
    
    //Process marker
    public static final String LAST_REFRESH_TIME = "last-refresh-time"; 
    public static final String LAST_COPY_TO_HDFS_DIRNAME = "last-copy-to-hdfs-dirname"; 
    public static final String LAST_MERGE_TO_MVDB_DIRNAME = "last-merge-to-mvdb-dirname"; 
    public static final String LAST_USED_END_TIME = "last-used-end-time"; 
    
    
    
    /*
    //Keys in  ~/.mvdb/etl.init.properties
    public static final String DataRootKey = "data.root"; 
    public static final String HdfsHomeKey = "hdfs.home"; 
    public static final String HdfsRootKey = "hdfs.root"; 
    */
    //public static final String ActionChainStatusFile = "hdfs.actionchain";


}
