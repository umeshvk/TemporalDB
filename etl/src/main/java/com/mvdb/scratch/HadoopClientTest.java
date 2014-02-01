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
package com.mvdb.scratch;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;

public class HadoopClientTest
{

    /**
     * @param args
     */
    public static void main(String[] args)
    {
        String dataFileName = "/tmp/df.txt";
        String sequenceFileName = "/tmp/seq.dat";
        String hadoopLocalFS = "file:///";
        createDataFile(dataFileName);
        testWriteSequenceFile(dataFileName, sequenceFileName, hadoopLocalFS);
        testReadSequenceFile(sequenceFileName, hadoopLocalFS);

    }

    public static void testReadSequenceFile(String sequenceFileName, String hadoopLocalFS)
    {
        try
        {
            HadoopClient.readSequenceFile(sequenceFileName, hadoopLocalFS) ;
        } catch (IOException e)
        {
            e.printStackTrace();
        }

    }

    private static void createDataFile(String dataFileName)
    {
        int numOfLines = 20;
        String baseStr = "....Test...";
        List<String> lines = new ArrayList<String>();
        for (int i = 0; i < numOfLines; i++)
            lines.add(i + baseStr + UUID.randomUUID());

        File dataFile = new File(dataFileName);
        try
        {
            FileUtils.writeLines(dataFile, lines, true);
            Thread.sleep(2000);
        } catch (IOException e)
        {
            e.printStackTrace();
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    public static void testWriteSequenceFile(String dataFileName, String sequenceFileName, String hadoopLocalFS)
    {
        try
        {
            File dataFile = new File(dataFileName);
            HadoopClient.writeToSequenceFile(dataFile, sequenceFileName, hadoopLocalFS);
        } catch (IOException e)
        {
            e.printStackTrace();
        }

    }

}
