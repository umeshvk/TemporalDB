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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mvdb.etl.actions.ActionUtils;
import com.mvdb.etl.actions.ConfigurationKeys;
import com.mvdb.etl.data.GenericDataRecord;
import com.mvdb.platform.data.MultiVersionRecord;

public class VersionMerge
{
    private static Logger logger = LoggerFactory.getLogger(VersionMerge.class);

    public static class VersionMergeMapper extends Mapper<Text, BytesWritable, MergeKey, BytesWritable>
    {
        MergeKey mergeKey= new MergeKey();
        public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException
        {
            System.out.println(ManagementFactory.getRuntimeMXBean().getName());
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            if(filename.startsWith("data-") == false)
            {
                return;
            }
            String timestamp = fileSplit.getPath().getParent().getName();
            String customer = fileSplit.getPath().getParent().getParent().getName();
            System.out.println("File name "+filename);
            System.out.println("Directory and File name"+fileSplit.getPath().toString());
            
            mergeKey.setCompany(customer);
            String fn = filename.replaceAll("-", ""); 
            fn = fn.replaceAll(".dat", "");
            mergeKey.setTable(fn);
            mergeKey.setId(key.toString());
            
            context.write(mergeKey, value);
        }
    }


    
    public static class VersionMergeReducer extends Reducer<MergeKey, BytesWritable, Text, BytesWritable>
    {

        MultipleOutputs<Text, BytesWritable> mos; 
        
        public void setup(Context context)
        {
             mos = new MultipleOutputs<Text, BytesWritable>(context);
        }
        
        protected void cleanup(Context context) throws IOException, InterruptedException 
        {
            mos.close();
        }
        
        public void reduce(MergeKey mergeKey, Iterable<BytesWritable> values, Context context) throws IOException,
                InterruptedException
        {

            System.out.println(ManagementFactory.getRuntimeMXBean().getName());
            Iterator<BytesWritable> itr = values.iterator();
            List<GenericDataRecord> gdrList = new ArrayList<GenericDataRecord>();
            MultiVersionRecord mvr = null;
            int mvrCount = 0; 
            while (itr.hasNext())
            {
                BytesWritable bw = itr.next();

                byte[] bytes = bw.getBytes();
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);

                try
                {
                    Object record = ois.readObject();
                    if(record instanceof MultiVersionRecord)
                    {
                        mvrCount++;
                        if(mvrCount > 1)
                        {
                            System.out.println("!!!ERROR!!!: Found two or more MultiVersionRecords in reducer for key:" + mergeKey.toString());
                            System.out.println(mvr.toString());
                        }
                        mvr = (MultiVersionRecord)record;
                        System.out.println(mvr.toString());
                    }
                    if(record instanceof GenericDataRecord)
                    {
                        GenericDataRecord gdr = (GenericDataRecord)record;
                        gdrList.add(gdr);
                        System.out.println(gdr.toString());
                    }
                } catch (ClassNotFoundException e)
                {
                    e.printStackTrace();
                }

            }
            
            if(mvr == null) { 
                mvr = new MultiVersionRecord();
            }                

            
            Collections.sort(gdrList);
            for(GenericDataRecord gdr : gdrList)
            {
                Object keyValue = gdr.getKeyValue();                
                System.out.println("gdr keyValue:" + keyValue);
                long timestamp = gdr.getTimestampLongValue();
                System.out.println("gdr timestamp:" + timestamp);
                mvr.addLatestVersion(gdr);                   
            }
            
            
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(mvr);
            oos.flush();
            BytesWritable bwOut = new BytesWritable(bos.toByteArray());            
            context.write(new Text(mergeKey.getId()), bwOut);
            mos.write(mergeKey.getTable(), new Text(mergeKey.getId()), bwOut);
            //mos.write("output2", new Text(mergeKey.getId()), bwOut);
        }
    }


    public static void main(String[] args) throws Exception
    {        
        logger.error("error1");
        logger.warn("warning1");
        logger.info("info1");
        logger.debug("debug1");
        logger.trace("trace1");
        ActionUtils.setUpInitFileProperty();
//        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
//        StatusPrinter.print(lc);

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //Also add  lastMergedTimeStamp and  mergeUptoTimestamp and passive db name which would be mv1 or mv2
        if (otherArgs.length != 3)
        {
            System.err.println("Usage: versionmerge <customer-directory>");
            System.exit(2);
        }
        //Example: file:/home/umesh/.mvdb/etl/data/alpha
        //Example: hdfs://localhost:9000/data/alpha
        String customerDirectory = otherArgs[0];
        String lastMergedDirName = otherArgs[1];
        String lastCopiedDirName = otherArgs[2];
        
        org.apache.hadoop.conf.Configuration conf1 = new org.apache.hadoop.conf.Configuration();
        //conf1.addResource(new Path("/home/umesh/ops/hadoop-1.2.0/conf/core-site.xml"));
        FileSystem hdfsFileSystem = FileSystem.get(conf1);
        
        Path topPath = new Path(customerDirectory);
        
        //Clean scratch db
        Path passiveDbPath = new Path(topPath, "db/mv1");
        Path tempDbPath = new Path(topPath, "db/tmp-" + (int)(Math.random() * 100000));
        if(hdfsFileSystem.exists(tempDbPath))
        {
            boolean success = hdfsFileSystem.delete(tempDbPath, true);
            if(success == false)
            {
                System.err.println(String.format("Unable to delete temp directory %s", tempDbPath.toString()));
                System.exit(1);
            }
        }
        //last three parameters are hardcoded and  the nulls must be replaced later after changing inout parameters. 
        Path[] inputPaths = getInputPaths(hdfsFileSystem, topPath, lastMergedDirName, lastCopiedDirName, null);
        Set<String> tableNameSet = new HashSet<String>();
        for(Path path: inputPaths)
        {
            tableNameSet.add(path.getName());
        }    
        
        Job job = new Job(conf, "versionmerge");
        job.setJarByClass(VersionMerge.class);
        job.setMapperClass(VersionMergeMapper.class);
        job.setReducerClass(VersionMergeReducer.class);
        job.setMapOutputKeyClass(MergeKey.class);
        job.setMapOutputValueClass(BytesWritable.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        String lastDirName = null;
        if(inputPaths != null && inputPaths.length > 1)
        {
            lastDirName = inputPaths[(inputPaths.length)-2].getParent().getName();
        }
        for(Path inputPath : inputPaths)
        {
            FileInputFormat.addInputPath(job, inputPath);
        }
        FileOutputFormat.setOutputPath(job, tempDbPath);
        
       
        for(String table: tableNameSet)
        {
            if(table.endsWith(".dat") == false)
            {
                continue;
            }
            table = table.replaceAll("-", "");
            table = table.replaceAll(".dat", "");
            MultipleOutputs.addNamedOutput(job, table, SequenceFileOutputFormat.class , Text.class, BytesWritable.class);
        }
        boolean success = job.waitForCompletion(true);     
        System.out.println("Success:" + success);
        System.out.println(ManagementFactory.getRuntimeMXBean().getName());
        if(success && lastDirName != null)
        {
            ActionUtils.setConfigurationValue(new Path(customerDirectory).getName(), ConfigurationKeys.LAST_MERGE_TO_MVDB_DIRNAME, lastDirName);
        }
        //hdfsFileSystem.delete(passiveDbPath, true);
        //hdfsFileSystem.rename(tempDbPath, passiveDbPath);
        System.exit(success ? 0 : 1);
    }
    
    
    /**           
     * @param hdfsFileSystem
     * @param topPath
     * @return
     * @throws IOException
     */

    private static Path[] getInputPaths(FileSystem hdfsFileSystem, Path topPath, String lastMergedDirName, String lastcopiedDirName, Path passiveDbPathT) throws IOException
    {
        Path passiveDbPath = new Path(topPath, "db/mv1");  
        if(hdfsFileSystem.exists(passiveDbPath) == false)
        {
            hdfsFileSystem.mkdirs(passiveDbPath);
        }
        List<Path> pathList = new ArrayList<Path>();        
        buildInputPathList(hdfsFileSystem, topPath, pathList, lastMergedDirName, lastcopiedDirName);
        pathList.add(passiveDbPath);
        Path[] inputPaths = pathList.toArray(new Path[0]);        
        return inputPaths;
    }

    private static void buildInputPathList(FileSystem fileSystem, Path topPath, List<Path> pathList, String lastMergedDirName, String lastcopiedDirName) throws IOException
    {
        FileStatus topPathStatus = fileSystem.getFileStatus(topPath);
        if(topPathStatus.isDir() == false)
        {
            String topPathFullName = topPath.toString(); 
            String[] tokens = topPathFullName.split("/");
            String fileName = tokens[tokens.length-1];
            if(fileName.startsWith("data-") && fileName.endsWith(".dat"))
            {
                String timeStamp = tokens[tokens.length-2];
                if(timeStamp.compareTo(lastMergedDirName) > 0 && timeStamp.compareTo(lastcopiedDirName) <= 0) 
                {
                    pathList.add(topPath);
                }
            }
            return; //This is a leaf
        }
        
        FileStatus[] fsArray = fileSystem.listStatus(topPath);
        for(FileStatus fileStatus: fsArray)
        {
            Path path = fileStatus.getPath();            
            buildInputPathList(fileSystem, path, pathList, lastMergedDirName, lastcopiedDirName);          
        }
    }
    
}


