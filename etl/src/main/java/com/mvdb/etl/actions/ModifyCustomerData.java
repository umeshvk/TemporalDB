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
package com.mvdb.etl.actions;

import java.util.Date;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.mvdb.etl.dao.OrderDAO;
import com.mvdb.etl.model.Order;
import com.mvdb.etl.util.RandomUtil;

public class ModifyCustomerData  implements IAction
{
    private static Logger logger = LoggerFactory.getLogger(ModifyCustomerData.class);

    public static void main(String[] args)
    {
        
        ActionUtils.assertEnvironmentSetupOk();
        ActionUtils.assertFileExists("~/.mvdb", "~/.mvdb missing. Existing.");
        ActionUtils.assertFileExists("~/.mvdb/status.InitCustomerData.complete", "300init-customer-data.sh not executed yet. Exiting");
        //This check is not required as data can be modified any number of times
        //ActionUtils.assertFileDoesNotExist("~/.mvdb/status.ModifyCustomerData.complete", "ModifyCustomerData already done. Start with 100init.sh if required. Exiting");
        ActionUtils.setUpInitFileProperty();
        ActionUtils.createMarkerFile("~/.mvdb/status.ModifyCustomerData.start", true);
        

        
        String customerName = null; 
        //Date startDate  = null;
        //Date endDate  = null;
        final CommandLineParser cmdLinePosixParser = new PosixParser();
        final Options posixOptions = constructPosixOptions();
        CommandLine commandLine;
        try
        {
            commandLine = cmdLinePosixParser.parse(posixOptions, args);
//            if (commandLine.hasOption("startDate"))
//            {
//                String startDateStr = commandLine.getOptionValue("startDate");
//                startDate = ActionUtils.getDate(startDateStr);
//            }
//            if (commandLine.hasOption("endDate"))
//            {
//                String endDateStr = commandLine.getOptionValue("endDate");
//                endDate = ActionUtils.getDate(endDateStr);
//            }
            if (commandLine.hasOption("customerName"))
            {
                customerName = commandLine.getOptionValue("customerName");
            }
        } catch (ParseException parseException) // checked exception
        {
            System.err
                    .println("Encountered exception while parsing using PosixParser:\n" + parseException.getMessage());
        }

        if (customerName == null)
        {
            System.err.println("customerName has not been specified.  Aborting...");
            System.exit(1);
        }
        
//        if (startDate == null)
//        {
//            System.err.println("startDate has not been specified with the correct format YYYYMMddHHmmss.  Aborting...");
//            System.exit(1);
//        }
//        
//        if (endDate == null)
//        {
//            System.err.println("endDate has not been specified with the correct format YYYYMMddHHmmss.  Aborting...");
//            System.exit(1);
//        }
//        
//        if (endDate.after(startDate) == false)
//        {
//            System.err.println("endDate must be after startDate.  Aborting...");
//            System.exit(1);
//        }
        
        
        
        ApplicationContext context = Top.getContext();

        final OrderDAO orderDAO = (OrderDAO) context.getBean("orderDAO");

        
        long maxId = orderDAO.findMaxId();
        long totalOrders = orderDAO.findTotalOrders();
        
        long modifyCount = (long)(totalOrders * 0.1);
       

        String lastUsedEndTimeStr = ActionUtils.getConfigurationValue(customerName, ConfigurationKeys.LAST_USED_END_TIME);
        long lastUsedEndTime = Long.parseLong(lastUsedEndTimeStr);
        Date startDate1 = new Date();
        startDate1.setTime(lastUsedEndTime + 1000 * 60 * 60 * 24 * 1);
        Date endDate1 = new Date(startDate1.getTime() + 1000 * 60 * 60 * 24 * 1);
        
        for(int i=0;i<modifyCount;i++)
        {
             Date updateDate = RandomUtil.getRandomDateInRange(startDate1, endDate1);
             long orderId = (long)Math.floor((Math.random() * maxId)) + 1L;
             logger.info("Modify Id " + orderId + " in orders");
             Order theOrder = orderDAO.findByOrderId(orderId);
//             System.out.println("theOrder : " + theOrder);
             theOrder.setNote(RandomUtil.getRandomString(4));
             theOrder.setUpdateTime(updateDate);
             theOrder.setSaleCode(RandomUtil.getRandomInt());
             orderDAO.update(theOrder);
//             System.out.println("theOrder Modified: " + theOrder);

        }
        ActionUtils.setConfigurationValue(customerName, ConfigurationKeys.LAST_USED_END_TIME, String.valueOf(endDate1.getTime()));
        logger.info("Modified " + modifyCount + " orders");
        ActionUtils.createMarkerFile("~/.mvdb/status.ModifyCustomerData.complete", true);
    }
    
    public static Options constructPosixOptions()
    {
        final Options posixOptions = new Options();
        posixOptions.addOption("customerName", true, "Customer Name");
//        posixOptions.addOption("startDate", true, "Start Date");
//        posixOptions.addOption("endDate", true, "End Date");

        return posixOptions;
    }
}
