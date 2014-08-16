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

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.FileUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.mvdb.etl.consumer.OrderJsonFileConsumer;
import com.mvdb.etl.dao.ConfigurationDAO;
import com.mvdb.etl.dao.GenericDAO;
import com.mvdb.etl.dao.OrderDAO;
import com.mvdb.etl.data.ColumnMetadata;
import com.mvdb.etl.model.Configuration;

public class ExtractDBChanges  implements IAction
{
    private static Logger logger = LoggerFactory.getLogger(ExtractDBChanges.class);


    public static void main(String[] args) throws JSONException
    {

        ActionUtils.setUpInitFileProperty();
//        boolean success = ActionUtils.markActionChainBroken("Just Testing");        
//        System.exit(success ? 0 : 1);
        ActionUtils.assertActionChainNotBroken();
        ActionUtils.assertEnvironmentSetupOk();
        ActionUtils.assertFileExists("~/.mvdb", "~/.mvdb missing. Existing.");
        ActionUtils.assertFileExists("~/.mvdb/status.InitCustomerData.complete", "300init-customer-data.sh not executed yet. Exiting");
        //This check is not required as data can be modified any number of times
        //ActionUtils.assertFileDoesNotExist("~/.mvdb/status.ModifyCustomerData.complete", "ModifyCustomerData already done. Start with 100init.sh if required. Exiting");
        
        ActionUtils.createMarkerFile("~/.mvdb/status.ExtractDBChanges.start", true);
        
        //String schemaDescription = "{ 'root' : [{'table' : 'orders', 'keyColumn' : 'order_id', 'updateTimeColumn' : 'update_time'}]}";

                
        String customerName = null;
        final CommandLineParser cmdLinePosixParser = new PosixParser();
        final Options posixOptions = constructPosixOptions();
        CommandLine commandLine;
        try
        {
            commandLine = cmdLinePosixParser.parse(posixOptions, args);
            if (commandLine.hasOption("customer"))
            {
                customerName = commandLine.getOptionValue("customer");
            }
        } catch (ParseException parseException) // checked exception
        {
            System.err
                    .println("Encountered exception while parsing using PosixParser:\n" + parseException.getMessage());
        }

        if (customerName == null)
        {
            System.err.println("Could not find customerName. Aborting...");
            System.exit(1);
        }

        
        
        ApplicationContext context = Top.getContext();
        
        final OrderDAO orderDAO = (OrderDAO) context.getBean("orderDAO");
        final ConfigurationDAO configurationDAO = (ConfigurationDAO) context.getBean("configurationDAO");
        final GenericDAO genericDAO = (GenericDAO)context.getBean("genericDAO");
        File snapshotDirectory = getSnapshotDirectory(configurationDAO, customerName);
        try
        {
            FileUtils.writeStringToFile(new File("/tmp/etl.extractdbchanges.directory.txt"), snapshotDirectory.getName(), false);
        } catch (IOException e)
        {
            e.printStackTrace();
            System.exit(1);
            return;
        }
        long currentTime = new Date().getTime();
        Configuration lastRefreshTimeConf = configurationDAO.find(customerName, "last-refresh-time");
        Configuration schemaDescriptionConf = configurationDAO.find(customerName, "schema-description");
        long lastRefreshTime = Long.parseLong(lastRefreshTimeConf.getValue());
        OrderJsonFileConsumer orderJsonFileConsumer = new OrderJsonFileConsumer(snapshotDirectory);
        Map<String, ColumnMetadata> metadataMap = orderDAO.findMetadata();
        //write file schema-orders.dat in snapshotDirectory
        genericDAO.fetchMetadata("orders", snapshotDirectory);
        //writes files: header-orders.dat, data-orders.dat in snapshotDirectory
        JSONObject json = new JSONObject(schemaDescriptionConf.getValue());
        JSONArray rootArray = json.getJSONArray("root");
        int length = rootArray.length();
        for(int i=0;i<length;i++)
        {
            JSONObject jsonObject = rootArray.getJSONObject(i);
            String  table = jsonObject.getString("table");
            String  keyColumnName = jsonObject.getString("keyColumn");
            String  updateTimeColumnName = jsonObject.getString("updateTimeColumn");
            System.out.println(
                    "table:" + table + 
                    ", keyColumn: " + keyColumnName + 
                    ", updateTimeColumn: " + updateTimeColumnName);
            genericDAO.fetchAll2(snapshotDirectory, new Timestamp(lastRefreshTime), table, keyColumnName, updateTimeColumnName);
        }
        
        //Unlikely failure
        //But Need to factor this into a separate task so that extraction does not have to be repeated. 
        //Extraction is an expensive task. 
        try
        {
            String sourceDirectoryAbsolutePath = snapshotDirectory.getAbsolutePath(); 
            
            File sourceRelativeDirectoryPath  = getRelativeSnapShotDirectory(configurationDAO, sourceDirectoryAbsolutePath);            
            String hdfsRoot = ActionUtils.getConfigurationValue(ConfigurationKeys.GLOBAL_CUSTOMER, ConfigurationKeys.GLOBAL_HDFS_ROOT);
            String targetDirectoryFullPath = hdfsRoot + "/data" + sourceRelativeDirectoryPath;
            
            ActionUtils.copyLocalDirectoryToHdfsDirectory(sourceDirectoryAbsolutePath, targetDirectoryFullPath);
            String dirName = snapshotDirectory.getName();
            ActionUtils.setConfigurationValue(customerName, ConfigurationKeys.LAST_COPY_TO_HDFS_DIRNAME, dirName);
        } catch (Throwable e)
        {
            e.printStackTrace();
            logger.error("Objects Extracted from database. But copy of snapshot directory<" + snapshotDirectory.getAbsolutePath() +  "> to hdfs <" + "" + ">failed. Fix the problem and redo extract.", e);
            System.exit(1);
        }
        
        //Unlikely failure
        //But Need to factor this into a separate task so that extraction does not have to be repeated. 
        //Extraction is an expensive task. 
        String targetZip = null; 
        try
        {
            File targetZipDirectory = new File(snapshotDirectory.getParent(), "archives" );
            if(!targetZipDirectory.exists())
            {
                boolean success = targetZipDirectory.mkdirs();
                if(success == false)
                {
                    logger.error("Objects copied to hdfs. But able to create archive directory <" +  targetZipDirectory.getAbsolutePath() + ">. Fix the problem and redo extract.");
                    System.exit(1);
                }
            }
            targetZip = new File(targetZipDirectory, snapshotDirectory.getName() + ".zip"  ).getAbsolutePath();
            ActionUtils.zipFullDirectory(snapshotDirectory.getAbsolutePath(), targetZip); 
        } catch (Throwable e)
        {
            e.printStackTrace();
            logger.error("Objects copied to hdfs. But zipping of snapshot directory<" + snapshotDirectory.getAbsolutePath() +  "> to  <" + targetZip + ">failed. Fix the problem and redo extract.", e);
            System.exit(1);
        }
                
        //orderDAO.findAll(new Timestamp(lastRefreshTime), orderJsonFileConsumer);
        Configuration updateRefreshTimeConf = new Configuration(customerName, "last-refresh-time",
                String.valueOf(currentTime));
        configurationDAO.update(updateRefreshTimeConf, String.valueOf(lastRefreshTimeConf.getValue()));
        ActionUtils.createMarkerFile("~/.mvdb/status.ExtractDBChanges.complete", true);

    }



    private static File getRelativeSnapShotDirectory(ConfigurationDAO configurationDAO, String absolutePath)
    {
        Configuration dataRootDirConfig = configurationDAO.find(ConfigurationKeys.GLOBAL_CUSTOMER, ConfigurationKeys.GLOBAL_LOCAL_DATA_ROOT);
        String dataRootDir = dataRootDirConfig.getValue();
        dataRootDir = ActionUtils.getAbsoluteFileName(dataRootDir);
        int baseLength = dataRootDir.length(); 
        return new File (absolutePath.substring(baseLength));
    }
    
    private static File getSnapshotDirectory(ConfigurationDAO configurationDAO, String customerName)
    {
        Configuration dataRootDirConfig = configurationDAO.find(ConfigurationKeys.GLOBAL_CUSTOMER, ConfigurationKeys.GLOBAL_LOCAL_DATA_ROOT);
        String dataRootDir = dataRootDirConfig.getValue();
        dataRootDir = ActionUtils.getAbsoluteFileName(dataRootDir);
        File customerDir = new File(dataRootDir, customerName);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        long lastUsedEndTime = ActionUtils.getConfigurationValueLong(customerName, ConfigurationKeys.LAST_USED_END_TIME);
        String snapshotDirectoryName = sdf.format(new Date(lastUsedEndTime));       
        File snapshotDirectory = new File(customerDir, snapshotDirectoryName);
        return snapshotDirectory;
    }

    public static Options constructPosixOptions()
    {
        final Options posixOptions = new Options();
        posixOptions.addOption("customer", true, "Customer Name");

        return posixOptions;
    }
}


