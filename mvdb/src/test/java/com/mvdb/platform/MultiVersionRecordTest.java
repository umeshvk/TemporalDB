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
package com.mvdb.platform;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.mvdb.etl.data.GenericDataRecord;
import com.mvdb.platform.data.MultiVersionRecord;

/**
 * Unit test for simple App.
 */
public class MultiVersionRecordTest extends TestCase
{
    /**
     * Create the test case
     * 
     * @param testName
     *            name of the test case
     */
    public MultiVersionRecordTest(String testName)
    {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite(MultiVersionRecordTest.class);
    }


    public void testGenericDataRecord()
    {
        GenericDataRecord genericDataRecord1 = new GenericDataRecord();
        Map<String, Object> map1 = new HashMap<String, Object>();
        map1.put("f1", 10);
        map1.put("f2", 20);
        map1.put("f3", 30);
        map1.put("f4", 40);
        genericDataRecord1.setDataMap(map1);


        GenericDataRecord genericDataRecord2 = new GenericDataRecord();
        Map<String, Object> map2 = new HashMap<String, Object>();
        map2.put("f1", 11);
        map2.put("f2", 20);
        map2.put("f3", 31);
        map2.put("f4", 40);
        genericDataRecord2.setDataMap(map2);

        MultiVersionRecord multiVersionRecord = new MultiVersionRecord();
        // Begin Test for adding first record
        multiVersionRecord.addLatestVersion(genericDataRecord1);
        GenericDataRecord ultimateVersion = multiVersionRecord.getVersion(0);
        Map<String, Object> ultimateVersionMap = ultimateVersion.getDataMap();
        assertTrue(ultimateVersionMap.get("f1").equals(10));
        assertTrue(ultimateVersionMap.get("f2").equals(20));
        assertTrue(ultimateVersionMap.get("f3").equals(30));
        assertTrue(ultimateVersionMap.get("f4").equals(40));
        // End Test for adding first record

        // Begin Test for adding second record
        multiVersionRecord.addLatestVersion(genericDataRecord2);
        GenericDataRecord penultimateVersion = multiVersionRecord.getVersion(0);
        Map<String, Object> penultimateVersionMap = penultimateVersion.getDataMap();
        assertTrue(penultimateVersionMap.get("f1").equals(10));
        assertTrue(penultimateVersionMap.get("f2") == null);
        assertTrue(penultimateVersionMap.get("f3").equals(30));
        assertTrue(penultimateVersionMap.get("f4") == null);

        ultimateVersion = multiVersionRecord.getVersion(1);
        ultimateVersionMap = ultimateVersion.getDataMap();
        assertTrue(ultimateVersionMap.get("f1").equals(11));
        assertTrue(ultimateVersionMap.get("f2").equals(20));
        assertTrue(ultimateVersionMap.get("f3").equals(31));
        assertTrue(ultimateVersionMap.get("f4").equals(40));
        // End Test for adding second record

        assertTrue(true);
    }
}
