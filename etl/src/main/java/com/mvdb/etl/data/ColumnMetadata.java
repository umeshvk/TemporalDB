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

public class ColumnMetadata implements Externalizable
{

    String  columnName;
    String  columnTypeName;
    String  columnLabel;
    int     columnType;

    @Override
    public void readExternal(ObjectInput input) throws IOException, ClassNotFoundException
    {
        
        columnName = (String)input.readObject();
        columnTypeName = (String)input.readObject();
        columnLabel = (String)input.readObject();
        columnType = input.readInt();
        
    }

    @Override
    public void writeExternal(ObjectOutput output) throws IOException
    {
        
        output.writeObject(columnName);
        output.writeObject(columnTypeName);
        output.writeObject(columnLabel);
        output.writeInt(columnType);
        
    }
    
    public ColumnMetadata()
    {

    }

    public String getColumnName()
    {
        return columnName;
    }

    public void setColumnName(String columnName)
    {
        this.columnName = columnName;
    }

    public String getColumnTypeName()
    {
        return columnTypeName;
    }

    public void setColumnTypeName(String columnTypeName)
    {
        this.columnTypeName = columnTypeName;
    }

    public String getColumnLabel()
    {
        return columnLabel;
    }

    public void setColumnLabel(String columnLabel)
    {
        this.columnLabel = columnLabel;
    }

    public int getColumnType()
    {
        return columnType;
    }

    public void setColumnType(int columnType)
    {
        this.columnType = columnType;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((columnLabel == null) ? 0 : columnLabel.hashCode());
        result = prime * result + ((columnName == null) ? 0 : columnName.hashCode());
        result = prime * result + columnType;
        result = prime * result + ((columnTypeName == null) ? 0 : columnTypeName.hashCode());
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
        ColumnMetadata other = (ColumnMetadata) obj;
        if (columnLabel == null)
        {
            if (other.columnLabel != null)
                return false;
        } else if (!columnLabel.equals(other.columnLabel))
            return false;
        if (columnName == null)
        {
            if (other.columnName != null)
                return false;
        } else if (!columnName.equals(other.columnName))
            return false;
        if (columnType != other.columnType)
            return false;
        if (columnTypeName == null)
        {
            if (other.columnTypeName != null)
                return false;
        } else if (!columnTypeName.equals(other.columnTypeName))
            return false;
        return true;
    }



}
