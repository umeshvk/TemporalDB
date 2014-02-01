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

public class Trash
{

    /**

    String startDirectory = otherArgs[0]; 
    String objectName = otherArgs[1]; 
    String passiveDB = otherArgs[2]; 
    
    if(startDirectory.startsWith("/") == false)
    {
        System.err.println(String.format("The startDirectory %s must be a absolute path", startDirectory));
        System.exit(1);
    }
    
//    if(endDirectory.startsWith("/") == false)
//    {
//        System.err.println(String.format("The endDirectory %s must be a absolute path", endDirectory));
//        System.exit(1);
//    }
    
    if(startDirectory.endsWith("/"))
    {
        startDirectory = startDirectory.substring(0, startDirectory.length()-1);
    }
//    if(endDirectory.endsWith("/"))
//    {
//        endDirectory = endDirectory.substring(0, endDirectory.length()-1);
//    }
    
    String parentStartDirectory = startDirectory.substring(0, startDirectory.lastIndexOf('/')); 
    //String parentEndDirectory = endDirectory.substring(0, endDirectory.lastIndexOf('/'));
    
//    if(parentStartDirectory.equals(parentEndDirectory) == false)
//    {
//        System.err.println(String.format("The startDirectory %s and endDirectory %s must have the same parent", startDirectory, endDirectory));
//        System.exit(1);
//    }
        
    String[] files = FileUtil.list(new File(parentStartDirectory));
    StringBuffer inputFileNames = new StringBuffer();
//    for(String fileName: files)
//    {
//        if(fileName.compareTo(startDirectory) >= 0 && fileName.compareTo(endDirectory) <= 0)
//        {
//            inputFileNames.append(fileName + "/" + objectName + ", ");
//        }
//    }
      for(String fileName: files)
      {
          if(fileName.compareTo(startDirectory) >= 0 && fileName.matches("^[0-9]{14}$"))
          {
              inputFileNames.append(parentStartDirectory + "/" + fileName + "/" + objectName + ", ");
          }
      }
    
    inputFileNames.append(parentStartDirectory + "/dbdata/" + passiveDB + "/" + objectName );
    
    String target = parentStartDirectory + "/dbdata/tmp-mvdb"; 
    
    System.out.println(String.format("InputPaths:%s", inputFileNames.toString()));
    System.out.println(String.format("Target:%s", target));
    
    
    //FileInputFormat.addInputPaths(job, "/home/umesh/.mvdb/etl/data/alpha/20130719114223/data-orders.dat /home/umesh/.mvdb/etl/data/alpha/20130719134839/data-orders.dat " );

**/

/*
Configuration conf = new Configuration();
conf.set("fs.default.name","hdfs://localhost:54310");
FileSystem fs = FileSystem.get(conf);
    //if(true) System.exit(1);
*/
    
    /**
    String globalDataRoot = topProps.getProperty(Constants.DataRootKey);
    String hdfsHome = topProps.getProperty(Constants.HdfsHomeKey);
    String[] commands = {
            "DROP TABLE IF EXISTS configuration;",
            "CREATE TABLE  configuration (" 
                    + " customer varchar(128)  NOT NULL, " 
                    + " name varchar(128)  NOT NULL,"
                    + " value varchar(128)  NOT NULL, " 
                    + " category varchar(32)  NOT NULL, " 
                    + " note varchar(512)  NOT NULL, " 
                    + "UNIQUE (customer, name, value, category)); ", 
            "INSERT INTO configuration (customer, name, value, category, note) VALUES  ('global', '" + Constants.DataRootKey + "', '" + globalDataRoot + "', '', '');", 
            "INSERT INTO configuration (customer, name, value, category, note) VALUES  ('global', '" + Constants.HdfsHomeKey + "', '" + hdfsHome + "', '', '');",
            "COMMIT;" };
    **/
    
 // Order order1 = new Order(orderDAO.getNextSequenceValue(),
 // RandomUtil.getRandomString(5),RandomUtil.getRandomInt(), new
 // Date(tm-10000000000L), new Date(tm-5000000000L));
 // Order order3 = new Order(orderDAO.getNextSequenceValue(),
 // RandomUtil.getRandomString(5),RandomUtil.getRandomInt(), new
 // Date(tm-20000000000L), new Date(tm-4000000000L));
 // Order order2 = new Order(orderDAO.getNextSequenceValue(),
 // RandomUtil.getRandomString(5),RandomUtil.getRandomInt(), new
 // Date(tm-30000000000L), new Date(tm-6000000000L));
 // orders.add(order1);
 // orders.add(order2);
 // orders.add(order3);

 /**
  * CREATE TABLE orders ( ORDER_ID bigint NOT NULL, NOTE varchar(100) NOT NULL,
  * SALE_CODE int NOT NULL, CREATE_TIME timestamp NOT NULL, UPDATE_TIME timestamp
  * NOT NULL ); COMMIT;
  * 
  * CREATE SEQUENCE com_etl_good_bad_Order START 101; commit; SELECT
  * nextval('com_etl_good_bad_Order');
  */

 /**
  * Order orderA = orderDAO.findByOrderId(1); System.out.println("Order A : " +
  * orderA);
  * 
  * 
  * 
  * List<Order> orderAs = orderDAO.findAll(); for(Order order: orderAs){
  * System.out.println("Order As : " + order); }
  **/
    
 // Order order1 = new Order(orderDAO.getNextSequenceValue(),
 // RandomUtil.getRandomString(5),RandomUtil.getRandomInt(), new
 // Date(tm-10000000000L), new Date(tm-5000000000L));
 // Order order3 = new Order(orderDAO.getNextSequenceValue(),
 // RandomUtil.getRandomString(5),RandomUtil.getRandomInt(), new
 // Date(tm-20000000000L), new Date(tm-4000000000L));
 // Order order2 = new Order(orderDAO.getNextSequenceValue(),
 // RandomUtil.getRandomString(5),RandomUtil.getRandomInt(), new
 // Date(tm-30000000000L), new Date(tm-6000000000L));
 // orders.add(order1);
 // orders.add(order2);
 // orders.add(order3);

 /**
  * CREATE TABLE orders ( ORDER_ID bigint NOT NULL, NOTE varchar(100) NOT NULL,
  * SALE_CODE int NOT NULL, CREATE_TIME timestamp NOT NULL, UPDATE_TIME timestamp
  * NOT NULL ); COMMIT;
  * 
  * CREATE SEQUENCE com_etl_good_bad_Order START 101; commit; SELECT
  * nextval('com_etl_good_bad_Order');
  */

 /**
  * Order orderA = orderDAO.findByOrderId(1); System.out.println("Order A : " +
  * orderA);
  * 
  * 
  * 
  * List<Order> orderAs = orderDAO.findAll(); for(Order order: orderAs){
  * System.out.println("Order As : " + order); }
  **/
    
    /*
     * 
     * FSDataInputStream in = fs1.open(inFile); FSDataOutputStream out =
     * fs2.create(outFile); System.out.println("Copy " + inFile.toString() +
     * " to " + outFile.toString());
     * 
     * int bytesRead = -1; byte[] buffer = new byte[1024]; while ((bytesRead
     * = in.read(buffer)) > 0) { out.write(buffer, 0, bytesRead); }
     * 
     * in.close(); out.close();
   */
    
    
    /*
     
    private static void testHdfs(String infileName, String outFileName) throws IOException
   {
       org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
       conf.addResource(new Path("/home/umesh/ops/hadoop-1.2.0/conf/core-site.xml")); 
       FileSystem fs2 = FileSystem.get(conf);
       FileSystem fs1 = FileSystem.get(new org.apache.hadoop.conf.Configuration());

       Path inFile = new Path(infileName);
       Path outFile = new Path(outFileName);
       
       if (fs2.exists(outFile))
       {
           boolean deleteSuccess = fs2.delete(outFile, true);
           if(deleteSuccess == false)
           {
               printAndExit("Unable to delete " + outFile.toString()); 
           }
       }
       if (!fs1.exists(inFile))
         printAndExit("Input file not found");
       FileStatus fileStatus1 = fs1.getFileStatus(inFile);        
       if (!fileStatus1.isDir())
         printAndExit("Input should be a directory");
       if (fs2.exists(outFile))
         printAndExit("Output already exists");

       System.out.println("Copy " + inFile.toString() + " to " + outFile.toString());
       FileUtil.copy(fs1, inFile, fs2, outFile, false, conf);
       
      
       
//       FSDataInputStream in = fs1.open(inFile);
//       FSDataOutputStream out = fs2.create(outFile);
//       System.out.println("Copy " + inFile.toString() + " to " + outFile.toString());
//       
//       int bytesRead = -1;
//       byte[] buffer = new byte[1024];
//       while ((bytesRead = in.read(buffer)) > 0) {
//         out.write(buffer, 0, bytesRead);
//       }
//
//       in.close();
//       out.close();
       
       System.exit(1);
   }
   
   
   private static void printAndExit(String string)
   {
       System.out.println(string);
       System.exit(1);        
   }
   
*/

}
