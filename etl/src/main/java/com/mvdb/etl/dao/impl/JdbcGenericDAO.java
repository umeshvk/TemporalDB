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
package com.mvdb.etl.dao.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import com.mvdb.etl.consumer.GenericConsumer;
import com.mvdb.etl.consumer.SequenceFileConsumer;
import com.mvdb.etl.dao.GenericDAO;
import com.mvdb.etl.data.ColumnMetadata;
import com.mvdb.etl.data.DataHeader;
import com.mvdb.etl.data.DataRecord;
import com.mvdb.etl.data.GenericDataRecord;
import com.mvdb.etl.data.Metadata;

public class JdbcGenericDAO extends JdbcDaoSupport implements GenericDAO
{
    private static Logger logger = LoggerFactory.getLogger(JdbcGenericDAO.class);
    private GlobalMvdbKeyMaker globalMvdbKeyMaker = new GlobalMvdbKeyMaker();
    private boolean writeDataHeader(DataHeader dataHeader, String objectName, File snapshotDirectory)
    {
        try
        {
            snapshotDirectory.mkdirs();
            String headerFileName = "header-" + objectName + ".dat";
            File headerFile = new File(snapshotDirectory, headerFileName);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(dataHeader);
            FileUtils.writeByteArrayToFile(headerFile, baos.toByteArray());
            return true;
        } catch (Throwable t)
        {
            t.printStackTrace();
            return false;
        }

    }

    @Override
    public void fetchMetadata(String objectName, File snapshotDirectory)
    {
        final Metadata metadata = new Metadata();
        metadata.setTableName(objectName);
        String sql = "SELECT * FROM " + objectName + " limit 1";
        final Map<String, ColumnMetadata> metaDataMap = new HashMap<String, ColumnMetadata>();
        metadata.setColumnMetadataMap(metaDataMap);
        metadata.setTableName(objectName);

        getJdbcTemplate().query(sql, new RowCallbackHandler() {

            @Override
            public void processRow(ResultSet row) throws SQLException
            {
                ResultSetMetaData rsm = row.getMetaData();
                int columnCount = rsm.getColumnCount();
                for (int column = 1; column < (columnCount + 1); column++)
                {
                    ColumnMetadata columnMetadata = new ColumnMetadata();
                    columnMetadata.setColumnLabel(rsm.getColumnLabel(column));
                    columnMetadata.setColumnName(rsm.getColumnName(column));
                    columnMetadata.setColumnType(rsm.getColumnType(column));
                    columnMetadata.setColumnTypeName(rsm.getColumnTypeName(column));

                    metaDataMap.put(rsm.getColumnName(column), columnMetadata);
                }

            }
        });

        writeMetadata(metadata, snapshotDirectory);
    }

    private boolean writeMetadata(Metadata metadata, File snapshotDirectory)
    {
        try
        {
            String structFileName = "schema-" + metadata.getTableName() + ".dat";
            snapshotDirectory.mkdirs();
            File structFile = new File(snapshotDirectory, structFileName);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(metadata);
            oos.flush();
            FileUtils.writeByteArrayToFile(structFile, baos.toByteArray());
            return true;
        } catch (Throwable t)
        {
            t.printStackTrace();
            return false;
        }

    }


    @Override
    public DataHeader fetchAll2(File snapshotDirectory, Timestamp modifiedAfter, String objectName, final String keyName, final String updateTimeColumnName)
    {
        File objectFile = new File(snapshotDirectory, "data-" + objectName + ".dat");
        final GenericConsumer genericConsumer = new SequenceFileConsumer(objectFile);
        final DataHeader dataHeader = new DataHeader();

        String sql = "SELECT * FROM " + objectName + " o where o.update_time >= ?";

        getJdbcTemplate().query(sql, new Object[] { modifiedAfter }, new RowCallbackHandler() {

            @Override
            public void processRow(ResultSet row) throws SQLException
            {
                final Map<String, Object> dataMap = new HashMap<String, Object>();
                ResultSetMetaData rsm = row.getMetaData();
                int columnCount = rsm.getColumnCount();
                for (int column = 1; column < (columnCount + 1); column++)
                {
                    dataMap.put(rsm.getColumnName(column), row.getObject(rsm.getColumnLabel(column)));
                }

                DataRecord dataRecord = new GenericDataRecord(dataMap, keyName , globalMvdbKeyMaker, 
                                updateTimeColumnName, new GlobalMvdbUpdateTimeMaker());
                genericConsumer.consume(dataRecord);
                dataHeader.incrementCount();

            }
        });

        genericConsumer.flushAndClose();
        
        

        writeDataHeader(dataHeader, objectName, snapshotDirectory);
        return dataHeader;
    }


    
    @Override
    public boolean scan2(String objectName, File snapshotDirectory)
    {
        String hadoopLocalFS = "file:///";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hadoopLocalFS);
        String dataFileName = "data-" + objectName + ".dat";
        File dataFile = new File(snapshotDirectory, dataFileName);
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
                GenericDataRecord dr = (GenericDataRecord) ois.readObject();
                System.out.println(dr.toString());
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



    private Metadata readMetadata(String objectName, File snapshotDirectory) throws IOException, ClassNotFoundException
    {
        Metadata metadata = new Metadata();
        FileInputStream fis = null;
        ObjectInputStream ois = null;
        try
        {
            String structFileName = "schema-" + objectName + ".dat";
            File structFile = new File(snapshotDirectory, structFileName);
            fis = new FileInputStream(structFile);
            ois = new ObjectInputStream(fis);
            metadata = (Metadata) ois.readObject();
            return metadata;
        } finally
        {
            if (fis != null)
            {
                fis.close();
            }
            if (ois != null)
            {
                ois.close();
            }
        }

    }

    @Override
    public Metadata getMetadata(String objectName, File snapshotDirectory)
    {

        try
        {
            return readMetadata(objectName, snapshotDirectory);
        } catch (ClassNotFoundException e)
        {
            e.printStackTrace();
        } catch (IOException e)
        {
            e.printStackTrace();
        }
        return null;
    }
    

}


/*
@Override
public boolean scanOld(String objectName, File snapshotDirectory) throws IOException
{
    FileInputStream fis = null;
    ObjectInputStream ois = null;
    try
    {
        String dataFileName = "data-" + objectName + ".dat";
        File dataFile = new File(snapshotDirectory, dataFileName);
        fis = new FileInputStream(dataFile);
        if (fis.available() <= 0)
        {
            return true;
        }
        ois = new ObjectInputStream(fis);
        while (fis.available() > 0)
        {
            DataRecord dataRecord = new GenericDataRecord();
            dataRecord = (GenericDataRecord) ois.readObject();
            System.out.println(dataRecord);
        }

    } catch (Throwable t)
    {
        t.printStackTrace();
        return false;
    } finally
    {
        if (fis != null)
        {
            fis.close();
        }
        if (ois != null)
        {
            ois.close();
        }
    }

    return true;

}

*/

/*
@Override
public DataHeader fetchAllOld(File snapshotDirectory, Timestamp modifiedAfter, String objectName)
{
    final GenericConsumer genericConsumer = new BinaryGenericConsumer(new File(snapshotDirectory, "data-"
            + objectName + ".dat"));
    final DataHeader dataHeader = new DataHeader();

    String sql = "SELECT * FROM " + objectName + " o where o.update_time >= ?";

    getJdbcTemplate().query(sql, new Object[] { modifiedAfter }, new RowCallbackHandler() {

        @Override
        public void processRow(ResultSet row) throws SQLException
        {
            final Map<String, Object> dataMap = new HashMap<String, Object>();
            ResultSetMetaData rsm = row.getMetaData();
            int columnCount = rsm.getColumnCount();
            for (int column = 1; column < (columnCount + 1); column++)
            {
                dataMap.put(rsm.getColumnName(column), row.getObject(rsm.getColumnLabel(column)));
            }

            DataRecord dataRecord = new GenericDataRecord(dataMap);
            genericConsumer.consume(dataRecord);
            dataHeader.incrementCount();

        }
    });

    genericConsumer.flushAndClose();

    writeDataHeader(dataHeader, objectName, snapshotDirectory);
    return dataHeader;

}
*/
