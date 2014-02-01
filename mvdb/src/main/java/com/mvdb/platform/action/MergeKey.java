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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MergeKey implements WritableComparable<MergeKey>
{
    String company;
    String table; 
    String id;
    
    public MergeKey()
    {
    
    }

    public MergeKey(String company, String table, String id)
    {
        this.company = company; 
        this.table = table; 
        this.id = id;
        if(company == null || table == null || id == null)
        {
            throw new RuntimeException("Bad Merge Key. Not expected to happen.");
        }
    }
    

    public String getCompany()
    {
        return company;
    }

    public void setCompany(String company)
    {
        this.company = company;
    }

    public String getTable()
    {
        return table;
    }

    public void setTable(String table)
    {
        this.table = table;
    }

    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }
    
    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        company = dataInput.readUTF(); 
        table = dataInput.readUTF();
        id = dataInput.readUTF();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeUTF(company); 
        dataOutput.writeUTF(table); 
        dataOutput.writeUTF(id);
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((company == null) ? 0 : company.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((table == null) ? 0 : table.hashCode());
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
        MergeKey other = (MergeKey) obj;
        if (company == null)
        {
            if (other.company != null)
                return false;
        } else if (!company.equals(other.company))
            return false;
        if (id == null)
        {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (table == null)
        {
            if (other.table != null)
                return false;
        } else if (!table.equals(other.table))
            return false;
        return true;
    }

    @Override
    public int compareTo(MergeKey other)
    {
        if(this == other)
        {
            return 0; 
        }
        
        int cv = company.compareTo(other.company);
        if(cv != 0)
        {
            return cv;
        }
        
        cv = table.compareTo(other.table);
        if(cv != 0)
        {
            return cv;
        }
        
        cv = id.compareTo(other.id);
        
        return cv;
    }

    public String toString()
    {
        return String.format("{company: %s, tabl: %s, id: %s}", company, table, id); 
    }



}
