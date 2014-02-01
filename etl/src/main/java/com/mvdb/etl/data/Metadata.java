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
package com.mvdb.etl.data;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Metadata implements Externalizable 
{
    private static final long serialVersionUID = 1L;

    int count;
    String schemaName; 
    String tableName;
    Map<String,ColumnMetadata> columnMetadataMap;


    
    public Metadata()
    {
        count = 0; 
    }
    
    public void incrementCount()
    {
        count++;
    }
    
    public int getCount()
    {
        return count;
    }

    public void setCount(int count)
    {
        this.count = count;
    }
    
    public String getSchemaName()
    {
        return schemaName;
    }

    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }
    
    public String getTableName()
    {
        return tableName;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    public Map<String,ColumnMetadata> getColumnMetadataMap()
    {
        return columnMetadataMap;
    }

    public void setColumnMetadataMap(Map<String,ColumnMetadata> columnMetadataMap)
    {
        this.columnMetadataMap = columnMetadataMap;
    }

    @Override
    public void readExternal(ObjectInput input) throws IOException, ClassNotFoundException
    {
        count = input.readInt();
        schemaName = (String)input.readObject();
        tableName = (String)input.readObject();
        
        
        columnMetadataMap = new HashMap<String, ColumnMetadata>();
        int keyCount = input.readInt();
        
        for(int i=0;i<keyCount;i++)
        {
            String key = (String)input.readObject();
            ColumnMetadata columnMetadata = new ColumnMetadata();
            columnMetadata = (ColumnMetadata) input.readObject();
            //columnMetadata.readExternal(input); 
            columnMetadataMap.put(key, columnMetadata);
        }
        
        
        
    }
    
    @Override
    public void writeExternal(ObjectOutput output) throws IOException
    {
        output.writeInt(count);
        output.writeObject(schemaName);
        output.writeObject(tableName);
        
        output.writeInt(columnMetadataMap.size());
        
        Iterator<String> keysIter = columnMetadataMap.keySet().iterator();
        while(keysIter.hasNext())
        {
            String key = keysIter.next();
            ColumnMetadata value = columnMetadataMap.get(key);
            output.writeObject(key);
            output.writeObject(value);
        }
        
        
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((columnMetadataMap == null) ? 0 : columnMetadataMap.hashCode());
        result = prime * result + count;
        result = prime * result + ((schemaName == null) ? 0 : schemaName.hashCode());
        result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Metadata other = (Metadata) obj;
        if (columnMetadataMap == null)
        {
            if (other.columnMetadataMap != null)
                return false;
        } else if (!columnMetadataMap.equals(other.columnMetadataMap))
            return false;
        if (count != other.count)
            return false;
        if (schemaName == null)
        {
            if (other.schemaName != null)
                return false;
        } else if (!schemaName.equals(other.schemaName))
            return false;
        if (tableName == null)
        {
            if (other.tableName != null)
                return false;
        } else if (!tableName.equals(other.tableName))
            return false;
        return true;
    }




}
