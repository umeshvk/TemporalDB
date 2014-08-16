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
package com.mvdb.etl.consumer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mvdb.etl.data.DataRecord;

public class SequenceFileConsumer implements GenericConsumer
{
    private static Logger logger = LoggerFactory.getLogger(SequenceFileConsumer.class);
    File                  file;
    FileOutputStream      fos;

    boolean               good;
    boolean               done;
    SequenceFile.Writer   writer;

    public SequenceFileConsumer(File dataFile)
    {
        String hadoopLocalFS = "file:///";
        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", hadoopLocalFS);
        FileSystem fs;
        try
        {
            fs = FileSystem.get(conf);
            if (conf != null)
            {
                Path path = new Path(dataFile.getAbsolutePath());
                writer = SequenceFile.createWriter(fs, conf, path, Text.class, BytesWritable.class);
            }
            good = true;
        } catch (IOException e)
        {
            logger.error("SequenceFileConsumer constructor:", e);
            good = false;
            return;
        }

    }


    @Override
    public boolean consume(DataRecord dataRecord)
    {
        if (done == true)
        {
            throw new ConsumerException("Consumer closed for output file:" + file.getAbsolutePath());
        }
        if (good == false)
        {
            throw new ConsumerException("Check log for prior error. Consumer unusable for output file:"
                    + file.getAbsolutePath());
        }

        try
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(dataRecord);
            oos.flush();
            BytesWritable value = new BytesWritable(bos.toByteArray());
            Text key = new Text(dataRecord.getMvdbKeyValue());
            writer.append(key, value);
            return true;
        } catch (IOException e)
        {
            e.printStackTrace();
            throw new ConsumerException("Consumer failed to consume for output file:" + file.getAbsolutePath()
                    + ", and DataRecord:" + dataRecord.toString());
        }

    }

    @Override
    public boolean flushAndClose()
    {
        if (writer != null)
        {
            IOUtils.closeStream(writer);
        }
        return true;
    }

}
