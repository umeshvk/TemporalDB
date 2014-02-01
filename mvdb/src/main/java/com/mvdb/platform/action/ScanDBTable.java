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
package com.mvdb.platform.action;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mvdb.platform.data.MultiVersionRecord;
import com.mvdb.etl.actions.ScanDBChanges;
import com.mvdb.etl.data.GenericDataRecord;

public class ScanDBTable
{
    private static Logger logger = LoggerFactory.getLogger(ScanDBChanges.class);
    /**
     * @param args
     */
    public static void main(String[] args)
    {
        ScanDBTable.scan("/home/umesh/.mvdb/etl/data/alpha/db/tmp-62103/dataorders-r-00000"); 
    }

    
    
    public static boolean scan(String dataFileName)
    {
        File dataFile = new File(dataFileName);
        String hadoopLocalFS = "file:///";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hadoopLocalFS);
//        String dataFileName = "data-" + objectName + ".dat";
//        File dataFile = new File(snapshotDirectory, dataFileName);
        Path path = new Path(dataFile.getAbsolutePath());

        FileSystem fs;
        try
        {
            fs = FileSystem.get(conf);
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);

            Text key = new Text(); 
            BytesWritable value = new BytesWritable(); 
            while (reader.next(key, value))
            {
                byte[] bytes = value.getBytes();
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                Object object = ois.readObject();
                if(object instanceof GenericDataRecord)
                {
                    GenericDataRecord dr = (GenericDataRecord) object;
                    System.out.println(dr.toString());
                }
                if(object instanceof MultiVersionRecord)
                {
                    MultiVersionRecord mvr = (MultiVersionRecord) object;
                    System.out.println(mvr.toString());
                }
            }

            IOUtils.closeStream(reader);
        } catch (IOException e)
        {
            logger.error("scan2():", e);
            return false;
        } catch (ClassNotFoundException e)
        {
            logger.error("scan2():", e);
            return false;
        }

        return true;
    }
}
