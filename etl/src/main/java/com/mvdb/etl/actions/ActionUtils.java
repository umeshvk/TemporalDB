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

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.mvdb.etl.dao.ConfigurationDAO;
import com.mvdb.etl.model.Configuration;

public class ActionUtils
{
    private static Logger     logger   = LoggerFactory.getLogger(ActionUtils.class);

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    public static Date getDate(String yyyyMMddHHmmss)
    {
        try
        {
            return sdf.parse(yyyyMMddHHmmss);
        } catch (ParseException e)
        {            
            return null;
        }
        
    }
    

    
    public static String getConfigurationValue(String customerName, String propertyName)
    {
        ApplicationContext context = Top.getContext();
        final ConfigurationDAO configurationDAO = (ConfigurationDAO) context.getBean("configurationDAO");        
        Configuration config = configurationDAO.find(customerName, propertyName);
        return config.getValue();
    }
    
    public static long getConfigurationValueLong(String customerName, String propertyName)
    {
        String value = getConfigurationValue(customerName, propertyName);
        return Long.parseLong(value);
    }
    
    public static String setConfigurationValue(String customerName, String propertyName, String propertyValue)
    {
        ApplicationContext context = Top.getContext();
        final ConfigurationDAO configurationDAO = (ConfigurationDAO) context.getBean("configurationDAO");        
        Configuration config = configurationDAO.find(customerName, propertyName);
        config.setValue(propertyValue);
        configurationDAO.update(config, null);
        return config.getValue();
    }
    
    public static Properties getTopProperties()
    {
        Properties topProps = null;

        try
        {
            String propFileName = getAbsoluteFileName("~/.mvdb/etl.init.properties");
            Properties topProp = new Properties();
            topProp.load(new FileInputStream(propFileName));
            topProps = topProp;
            return topProps;
        } catch (FileNotFoundException e)
        {
            e.printStackTrace();
            logger.error("", e);
        } catch (IOException e)
        {
            e.printStackTrace();
            logger.error("", e);
        }

        return null;

    }

    
    public static void writeStringToHdfsFile(String str, String hdfsFile) throws IOException
    {

        String hdfsHome = getConfigurationValue(ConfigurationKeys.GLOBAL_CUSTOMER, ConfigurationKeys.GLOBAL_HADOOP_HOME);
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.addResource(new Path(hdfsHome + "/conf/core-site.xml"));
        FileSystem hdfsFileSystem = FileSystem.get(conf);

        Path hdfsFilePath = new Path(hdfsFile);

        if (hdfsFileSystem.exists(hdfsFilePath))
        {
            boolean deleteSuccess = hdfsFileSystem.delete(hdfsFilePath, true);
            if (deleteSuccess == false)
            {
                throw new RuntimeException("Unable to delete " + hdfsFilePath.toString());
            }
        }

        if (hdfsFileSystem.exists(hdfsFilePath))
        {
            throw new RuntimeException("Output " + hdfsFilePath + "already exists");
        }

        logger.info("Copy " + str + " in to " + hdfsFilePath.toString());

        FSDataOutputStream out = hdfsFileSystem.create(hdfsFilePath);
        byte[] bytes = str.getBytes();
        out.write(bytes, 0, bytes.length);
        out.close();

    }

    public static void copyLocalDirectoryToHdfsDirectory(String localDirectory, String hdfsDirectory) throws Throwable
    {
        String hdfsHome = getConfigurationValue(ConfigurationKeys.GLOBAL_CUSTOMER, ConfigurationKeys.GLOBAL_HADOOP_HOME);
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.addResource(new Path(hdfsHome + "/conf/core-site.xml"));
        FileSystem hdfsFileSystem = FileSystem.get(conf);

        FileSystem localFileSystem = FileSystem.get(new org.apache.hadoop.conf.Configuration());

        Path localDirectoryPath = new Path(localDirectory);
        Path hdfsDirectoryPath = new Path(hdfsDirectory);

        if (hdfsFileSystem.exists(hdfsDirectoryPath))
        {
            boolean deleteSuccess = hdfsFileSystem.delete(hdfsDirectoryPath, true);
            if (deleteSuccess == false)
            {
                throw new RuntimeException("Unable to delete " + hdfsDirectoryPath.toString());
            }
        }
        if (!localFileSystem.exists(localDirectoryPath))
        {
            throw new RuntimeException("Input directory " + localDirectoryPath + " not found");
        }
        FileStatus fileStatus1 = localFileSystem.getFileStatus(localDirectoryPath);
        if (!fileStatus1.isDir())
        {
            throw new RuntimeException("Input " + localDirectoryPath + " should be a directory");
        }
        if (hdfsFileSystem.exists(hdfsDirectoryPath))
        {
            throw new RuntimeException("Output " + hdfsDirectoryPath + "already exists");
        }

        logger.info("Attempting Copy " + localDirectoryPath.toString() + " to " + hdfsDirectoryPath.toString());
        FileUtil.copy(localFileSystem, localDirectoryPath, hdfsFileSystem, hdfsDirectoryPath, false, conf);
        logger.info("-Completed Copy " + localDirectoryPath.toString() + " to " + hdfsDirectoryPath.toString());

    }

    public static boolean isActionChainBroken() throws IOException
    {
        String hdfsHome = getConfigurationValue(ConfigurationKeys.GLOBAL_CUSTOMER, ConfigurationKeys.GLOBAL_HADOOP_HOME);
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.addResource(new Path(hdfsHome + "/conf/core-site.xml"));
        FileSystem hdfsFileSystem = FileSystem.get(conf);

        String actionChainStatusFile = getConfigurationValue(ConfigurationKeys.GLOBAL_CUSTOMER, ConfigurationKeys.GLOBAL_ACTION_CHAIN_STATUS_FILE);
        String actionChainStatusFileName = System.getProperty(File.separator) + actionChainStatusFile;
        Path actionChainStatusFilePath = new Path(actionChainStatusFileName);

        if (hdfsFileSystem.exists(actionChainStatusFilePath))
        {
            return true;
        }

        return false;
    }

    public static String getActionChainBrokenCause() throws IOException
    {
        String hdfsHome = getConfigurationValue(ConfigurationKeys.GLOBAL_CUSTOMER, ConfigurationKeys.GLOBAL_HADOOP_HOME);
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.addResource(new Path(hdfsHome + "/conf/core-site.xml"));
        FileSystem hdfsFileSystem = FileSystem.get(conf);

        String actionChainStatusFile = getConfigurationValue(ConfigurationKeys.GLOBAL_CUSTOMER, ConfigurationKeys.GLOBAL_ACTION_CHAIN_STATUS_FILE);
        String actionChainStatusFileName = /* hdfsHome + */File.separator + actionChainStatusFile;
        Path actionChainStatusFilePath = new Path(actionChainStatusFileName);

        if (hdfsFileSystem.exists(actionChainStatusFilePath))
        {
            FSDataInputStream in = hdfsFileSystem.open(actionChainStatusFilePath);

            int bytesRead = -1;
            byte[] buffer = new byte[1024];
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            while ((bytesRead = in.read(buffer)) > 0)
            {
                baos.write(buffer, 0, bytesRead);
            }
            return new String(baos.toByteArray());
        }

        return null;
    }

    public static boolean markActionChainBroken(String reason)
    {
        try
        {
            String actionChainStatusFile = getConfigurationValue(ConfigurationKeys.GLOBAL_CUSTOMER, ConfigurationKeys.GLOBAL_ACTION_CHAIN_STATUS_FILE);
            String actionChainStatusFileName = File.separator + actionChainStatusFile;
            writeStringToHdfsFile(reason, actionChainStatusFileName);
            return true;
        } catch (IOException e)
        {
            e.printStackTrace();
            logger.error("", e);
        }
        return false;

    }

    public static void setUpInitFileProperty()
    {
        String fileName = System.getenv("ETL_INIT_FILE");
        if (fileName == null)
        {
            logger.error("No initFile found in env.");
            System.exit(1);
        }

        File initFileFile = new File(fileName);
        if (initFileFile.exists() == false)
        {
            logger.error("initFile <" + fileName + "> does not exist");
            System.exit(1);
        }
        System.setProperty("initFile", "file://" + initFileFile.getAbsolutePath());
    }

    public static void createMarkerFile(String touchFile)
    {
        createMarkerFile(touchFile, false);
    }

    public static String getAbsoluteFileName(String fileName)
    {
        if (fileName.startsWith("~" + File.separator))
        {
            fileName = System.getProperty("user.home") + fileName.substring(1);
        }
        File file = new File(fileName);

        return file.getAbsolutePath();
    }

    public static void createMarkerFile(String touchFile, boolean doNotCheckForPriorExistence)
    {
        touchFile = getAbsoluteFileName(touchFile);
        File file = new File(touchFile);
        if (doNotCheckForPriorExistence == false && file.exists() == true)
        {
            logger.error("Warning: <" + touchFile + "> already exists. Check why this happend before proceeding.");
            System.exit(1);
        }
        try
        {
            FileUtils.touch(file);
        } catch (IOException e)
        {
            e.printStackTrace();
            logger.error("", e);
            throw new RuntimeException(e);
        }
    }

    public static void assertActionChainNotBroken()
    {
        try
        {
            String cause = ActionUtils.getActionChainBrokenCause();

            if (cause != null)
            {
                logger.error("Action Chain Broken:" + cause);
                System.exit(1);
            }
        } catch (IOException e1)
        {
            e1.printStackTrace();
            logger.error("", e1);
            System.exit(1);
        }

    }

    public static void assertEnvironmentSetupOk()
    {
        if (System.getenv("BIGDATA_DIR") == null)
        {
            logger.error("BIGDATA_DIR not setup properly. Run call <. 050initenv.sh> before doing anything else.");
            System.exit(1);
        }
        if (System.getenv("ETL_DIR") == null)
        {
            logger.error("ETL_DIR not setup properly. Run call <. 050initenv.sh> before doing anything else.");
            System.exit(1);
        }
        if (System.getenv("MVDB_DIR") == null)
        {
            logger.error("MVDB_DIR not setup properly. Run call <. 050initenv.sh> before doing anything else.");
            System.exit(1);
        }
        if (System.getenv("ETL_INIT_FILE") == null)
        {
            logger.error("ETL_INIT_FILE not setup properly. Run call <. 050initenv.sh> before doing anything else.");
            System.exit(1);
        }
    }

    public static void assertFileExists(String fileName, String failureMessage)
    {
        fileName = getAbsoluteFileName(fileName);
        File file = new File(fileName);
        if (file.exists() == false)
        {
            logger.error(failureMessage);
            System.exit(1);
        }
    }

    public static void assertFileDoesNotExist(String fileName, String failureMessage)
    {
        fileName = getAbsoluteFileName(fileName);
        File file = new File(fileName);
        if (file.exists() == true)
        {
            logger.error(failureMessage);
            System.exit(1);
        }
    }

    public static void zipFullDirectory(String sourceDir, String targetZipFile)
    {
        FileOutputStream fos = null;
        BufferedOutputStream bos = null;
        ZipOutputStream zos = null;

        try
        {
            fos = new FileOutputStream(targetZipFile);
            bos = new BufferedOutputStream(fos);
            zos = new ZipOutputStream(bos);
            zipDir(sourceDir, new File(sourceDir), zos);
            
        } catch (FileNotFoundException e)
        {
            e.printStackTrace();
        } catch (IOException e)
        {
            e.printStackTrace();
        } finally
        {               
            if(zos != null)
            {
                try
                {
                    zos.flush();
                    zos.close();
                } catch (IOException e)
                {                  
                    e.printStackTrace();
                }
                
            }
            if(bos != null)
            {
                try
                {
                    bos.flush();
                    bos.close();
                } catch (IOException e)
                {                  
                    e.printStackTrace();
                }
                
            }
            if(fos != null)
            {
                try
                {
                    fos.flush();
                    fos.close();
                } catch (IOException e)
                {                   
                    e.printStackTrace();
                }
                
            }
        }
    }

    private static void zipDir(String origDir, File dirObj, ZipOutputStream zos) throws IOException
    {
        File[] files = dirObj.listFiles();
        byte[] tmpBuf = new byte[1024];

        for (int i = 0; i < files.length; i++)
        {
            if (files[i].isDirectory())
            {
                zipDir(origDir, files[i], zos);
                continue;
            }
            String wAbsolutePath = files[i].getAbsolutePath().substring(origDir.length()+1,
                    files[i].getAbsolutePath().length());
            FileInputStream in = new FileInputStream(files[i].getAbsolutePath());
            zos.putNextEntry(new ZipEntry(wAbsolutePath));
            int len;
            while ((len = in.read(tmpBuf)) > 0)
            {
                zos.write(tmpBuf, 0, len);
            }
            zos.closeEntry();
            in.close();
        }
    }

    public static void loggerTest(Logger logger)
    {
        logger.error("error");
        logger.warn("warning");
        logger.info("info");
        logger.debug("debug");
        logger.trace("trace");        
    }
}
