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
package com.mvdb.scratch;



import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.mvdb.etl.App;

/**
 * This class handles interactions with Hadoop.
 * 
 * @author nbashir
 * 
 */
@Component
public class HadoopClient {

   private static Configuration conf = new Configuration();
   private static Logger logger = LoggerFactory.getLogger(HadoopClient.class);

   public static void readSequenceFile(String sequenceFileName, String hadoopFS) throws IOException
   {
       Path path = new Path(sequenceFileName);
       conf.set("fs.defaultFS", hadoopFS);
       FileSystem fs = FileSystem.get(conf);

       SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);

       IntWritable key = new IntWritable(); // this could be the wrong type
       BytesWritable value = new BytesWritable(); // also could be wrong

       while (reader.next(key, value))
       {
           System.out.println(key + ":" + new String(value.getBytes()));
       }
       
       IOUtils.closeStream(reader);
   }
   
   /**
    * Convert the lines of text in a file to binary and write to a Hadoop
    * sequence file.
    * 
    * @param dataFile File containing lines of text
    * @param sequenceFileName Name of the sequence file to create
    * @param hadoopFS Hadoop file system
    * 
    * @throws IOException
    */
   public static void writeToSequenceFile(File dataFile, String sequenceFileName, String hadoopFS) throws IOException {

      IntWritable key = null;
      BytesWritable value = null;

      conf.set("fs.defaultFS", hadoopFS);
      FileSystem fs = FileSystem.get(conf);
      Path path = new Path(sequenceFileName);

      if ((conf != null) && (dataFile != null) && (dataFile.exists())) {
         SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path,
               IntWritable.class, BytesWritable.class);

         List<String> lines = FileUtils.readLines(dataFile);

         for (int i = 0; i < lines.size(); i++) {
            value = new BytesWritable(lines.get(i).getBytes());
            key = new IntWritable(i);
            writer.append(key, value);
         }
         IOUtils.closeStream(writer);
      }
   }

   /**
    * Read a Hadoop sequence file on HDFS.
    * 
    * @param sequenceFileName Name of the sequence file to read
    * @param hadoopFS Hadoop file system
    * 
    * @throws IOException
    */
   /*
   public static void readSequenceFile(String sequenceFileName, String hadoopFS) throws IOException {
      conf.set("fs.defaultFS", hadoopFS);
      Path path = new Path(sequenceFileName);
      SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
      IntWritable key = (IntWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
      BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
      while (reader.next(key, value)) {
         logger.info("key : " + key + " - value : " + new String(value.getBytes()));
      }
      IOUtils.closeStream(reader);
   }
   */

   /**
    * Copy a local sequence file to a remote file on HDFS.
    * 
    * @param from Name of the sequence file to copy
    * @param to Name of the sequence file to copy to
    * @param remoteHadoopFS HDFS host URI
    * 
    * @throws IOException
    */
   public static void copySequenceFile(String from, String to, String remoteHadoopFS) throws IOException {
      conf.set("fs.defaultFS", remoteHadoopFS);
      FileSystem fs = FileSystem.get(conf);

      Path localPath = new Path(from);
      Path hdfsPath = new Path(to);
      boolean deleteSource = true;

      fs.copyFromLocalFile(deleteSource, localPath, hdfsPath);
      logger.info("Copied SequenceFile from: " + from + " to: " + to);
   }

   /**
    * Print all the values in Hadoop HDFS configuration object.
    * 
    * @param conf
    */
   public static void listHadoopConfiguration(Configuration conf) {
      int i = 0;
      logger.info("------------------------------------------------------------------------------------------");
      Iterator iterator = conf.iterator();
      while (iterator.hasNext()) {
         i++;
         iterator.next();
         logger.info(i + " - " + iterator.next());
      }
      logger.info("------------------------------------------------------------------------------------------");
   }
}

