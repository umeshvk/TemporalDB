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
package com.mvdb.platform.data;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.mvdb.etl.data.GenericDataRecord;

public class MultiVersionRecord implements Externalizable
{
    private static final long serialVersionUID = 1L;
    List<GenericDataRecord> versionList;
      
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append("[");
        for(GenericDataRecord gdr : versionList)
        {
            sb.append(gdr);
            sb.append(",");
        }
        sb.setLength(sb.length()-1);
        sb.append("]");
        
        return sb.toString();
        
    }
    
    public MultiVersionRecord()
    {
        versionList = new ArrayList<GenericDataRecord>();               
    }
        
    public void addLatestVersion(GenericDataRecord latestVersion)
    {
        addLatestVersion(versionList, latestVersion);
    }
    
    public int getVersionCount()
    {
        return versionList.size(); 
    }
    
    public GenericDataRecord getVersion(int pos)
    {
        return versionList.get(pos);
    }
    
    private void addLatestVersion(List<GenericDataRecord> currentList, GenericDataRecord latestVersion)
    {
        if(currentList.size() == 0)
        {
            currentList.add(latestVersion);
            return;
        }
        
        GenericDataRecord lastVersion = currentList.get(currentList.size()-1);
        Map<String, Object> map = latestVersion.getDataMap();
        Iterator<String> iter = map.keySet().iterator();
        while(iter.hasNext()) { 
            String columnName = iter.next();
            Object latestValue = map.get(columnName);
            lastVersion.removeIdenticalColumn(columnName, latestValue);
        }
        currentList.add(latestVersion);            
        
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        int size = in.readInt();
        for(int i=0;i<size;i++)
        {
            GenericDataRecord recordVersion = (GenericDataRecord)in.readObject();
            versionList.add(recordVersion);
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        int size = versionList.size();
        out.writeInt(size);
        for(int i=0;i<size;i++)
        {
            GenericDataRecord recordVersion = versionList.get(i);
            out.writeObject(recordVersion);
        }
    }

}
