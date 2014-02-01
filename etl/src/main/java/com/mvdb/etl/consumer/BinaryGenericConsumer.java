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
package com.mvdb.etl.consumer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import com.mvdb.etl.data.DataRecord;

public class BinaryGenericConsumer implements GenericConsumer
{

    File                file;
    FileOutputStream    fos;
    ObjectOutputStream  oos;
    boolean             good;
    boolean             done;

    public BinaryGenericConsumer(File file)
    {
        this.done = false;
        this.file = file;
        try
        {
            file.getParentFile().mkdirs();
            this.fos = new FileOutputStream(file);
            this.oos = new ObjectOutputStream(fos);
            good = true;
        } catch (FileNotFoundException e)
        {
            good = false;
            e.printStackTrace();
            
        } catch (IOException e)
        {
            good = false;
            e.printStackTrace();
        }
    }

    @Override
    public boolean consume(DataRecord dataRecord)
    {
        if (done == true)
        {
            throw new ConsumerException("Consumer closed for output file:" + file.getAbsolutePath());
        }
        if (good == false)
        {
            throw new ConsumerException("Check log for prior error. Consumer unusable for output file:"
                    + file.getAbsolutePath());
        }

        try
        {            
            oos.writeObject(dataRecord);
            return true;
        } catch (IOException e)
        {
            e.printStackTrace();
            throw new ConsumerException("Consumer failed to consume for output file:" + file.getAbsolutePath()
                    + ", and DataRecord:" + dataRecord.toString());
        }

    }

    @Override
    public boolean flushAndClose()
    {
        try
        {
            if (oos != null)
            {
                oos.flush();
                oos.close();               
            }
            
            if (fos != null)
            {
                fos.flush();
                fos.close();               
            }
            
            return true;
        } catch (IOException ioe)
        {
            throw new ConsumerException("Unable to flush for output file:" + file.getAbsolutePath(), ioe);
        }
    }

}
