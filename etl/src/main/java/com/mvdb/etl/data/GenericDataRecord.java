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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericDataRecord implements DataRecord
{
    private static final long serialVersionUID = 1L;

    private static Logger logger = LoggerFactory.getLogger(GenericDataRecord.class);
    //Change this to "id". When creating the data feed always add a column called id to contain the key. 
    private static String ID = "mvdb_id"; 
    private static String MVDB_UPDATE_TIME_COLUMN = "mvdb_update_time"; 

    Map<String, Object> dataMap;
    String mvdbKeyValue; 
    Date mvdbUpdateTime;
   
    public GenericDataRecord(Map<String, Object> dataMap, String originalKeyName, MvdbKeyMaker mvdbKeyMaker, String originalUpdateTimeColumn, MvdbUpdateTimeMaker mvdbUpdateTimeMaker)
    {
        if (dataMap == null)
        {
            dataMap = new HashMap<String, Object>();
        }
        this.dataMap = dataMap;
        Object originalKeyValue  = dataMap.get(originalKeyName);
        this.mvdbKeyValue = mvdbKeyMaker.makeKey(originalKeyValue);
        dataMap.put(ID, mvdbKeyValue);
        Object originalUpdateTimeValue  = dataMap.get(originalUpdateTimeColumn);
        this.mvdbUpdateTime = mvdbUpdateTimeMaker.makeMvdbUpdateTime(originalUpdateTimeValue);
        dataMap.put(MVDB_UPDATE_TIME_COLUMN, mvdbUpdateTime);

    }
    

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((dataMap == null) ? 0 : dataMap.hashCode());
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
        GenericDataRecord other = (GenericDataRecord) obj;
        if (dataMap == null)
        {
            if (other.dataMap != null)
                return false;
        } else if (!dataMap.equals(other.dataMap))
            return false;
        return true;
    }

    public GenericDataRecord()
    {
        dataMap = new HashMap<String, Object>();
    }

    public Object getKeyValue()
    {
        return dataMap.get(ID);
    }
    
    public long getTimestampLongValue()
    {
        return ((Date)dataMap.get(MVDB_UPDATE_TIME_COLUMN)).getTime();
    }
    


    public Map<String, Object> getDataMap()
    {
        return dataMap;
    }

    public void setDataMap(Map<String, Object> dataMap)
    {
        this.dataMap = dataMap;
    }

    @Override
    public void readExternal(ObjectInput input) throws IOException, ClassNotFoundException
    {
        this.mvdbKeyValue = (String)input.readObject();
        this.mvdbUpdateTime = (Date)input.readObject();
        int size = input.readInt();
        dataMap = new HashMap<String, Object>();
        for (int i = 0; i < size; i++)
        {
            String key = input.readUTF();
            Object value = input.readObject();
            dataMap.put(key, value);
        }
    }

    @Override
    public void writeExternal(ObjectOutput output) throws IOException
    {
        output.writeObject(mvdbKeyValue);
        output.writeObject(mvdbUpdateTime);
        output.writeInt(dataMap.size());
        Iterator<String> keysIter = dataMap.keySet().iterator();
        while (keysIter.hasNext())
        {
            String key = keysIter.next();
            Object value = dataMap.get(key);
            output.writeUTF(key);
            output.writeObject(value);
        }

    }
    
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append("{");
        sb.append("\"mvdbKeyValue\"");
        sb.append(" : ");
        sb.append("\"" + mvdbKeyValue + "\"");
        sb.append(", ");
        sb.append("\"mvdbUpdateTime\"");
        sb.append(" : ");
        sb.append("\"" + mvdbUpdateTime + "\"");
        sb.append(", ");
        Iterator<String> keysIter = dataMap.keySet().iterator();
        while(keysIter.hasNext())
        {
            String key = keysIter.next();        
            Object value = dataMap.get(key);
            
            sb.append("\"");
            sb.append(key);
            sb.append("\"");
            sb.append(" : \"");
            sb.append(value);
            sb.append("\", ");
        }
        int length = sb.length() -2; 
        if(length> 0)
        {
            sb.setLength(length);
        }
        sb.append("}");
        
        return sb.toString();
    }
    
    public void removeIdenticalColumn(String columnName, Object latestValue)
    {
        Object lastValue = dataMap.get(columnName);
        if(lastValue == null)
        {
            return; 
        }
        if(lastValue.equals(latestValue))
        {
            dataMap.remove(columnName);
        }
    }

    @Override
    public int compareTo(DataRecord dataRecord)
    {
        Long local = new Long(this.getTimestampLongValue());
        Long external = new Long(dataRecord.getTimestampLongValue());        
        return local.compareTo(external);
    }
    
    public String getMvdbKeyValue()
    {
        return mvdbKeyValue;
    }

    public void setMvdbKeyValue(String mvdbKeyValue)
    {
        this.mvdbKeyValue = mvdbKeyValue;
    }


}
