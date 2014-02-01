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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.mvdb.etl.dao.ConfigurationDAO;
import com.mvdb.etl.dao.OrderDAO;
import com.mvdb.etl.util.db.SequenceNames;

public class InitDB implements IAction
{
    
    private static Logger logger = LoggerFactory.getLogger(InitDB.class);
            
    public static void main(String[] args)
    {        
        ActionUtils.assertEnvironmentSetupOk();
        ActionUtils.assertFileExists("~/.mvdb", "~/.mvdb missing. Existing.");
        ActionUtils.assertFileExists("~/.mvdb/status.init.sh.complete", "init.sh not executed yet. Exiting");
        ActionUtils.assertFileDoesNotExist("~/.mvdb/status.InitDB.complete", "initDB already done. Start with init.sh if required. Exiting");
       
        ActionUtils.setUpInitFileProperty();
                
        ActionUtils.createMarkerFile("~/.mvdb/status.InitDB.start");
        
        ActionUtils.loggerTest(logger);
        
        ApplicationContext context = Top.getContext();

        createConfiguration(context);
        createOrder(context);

        ActionUtils.createMarkerFile("~/.mvdb/status.InitDB.complete");
    }

    private static void createOrder(ApplicationContext context)
    {
        final OrderDAO orderDAO = (OrderDAO) context.getBean("orderDAO");

        String[] commands = {
                "DROP SEQUENCE IF EXISTS " + SequenceNames.ORDER_SEQUENCE_NAME + ";",
                "CREATE SEQUENCE com_mvdb_etl_dao_OrderDAO START 1;",
                "COMMIT;",
                "DROP TABLE IF EXISTS orders;",
                "CREATE TABLE  orders (" + " ORDER_ID bigint  NOT NULL, " + " NOTE varchar(200) NOT NULL,"
                        + " SALE_CODE int NOT NULL," + " CREATE_TIME timestamp NOT NULL,"
                        + " UPDATE_TIME timestamp NOT NULL, " + "constraint order_pk PRIMARY KEY (ORDER_ID)" + " ); ", "COMMIT;" };

        
        orderDAO.executeSQl(commands);
        
    }
    
    private static void createConfiguration(ApplicationContext context)
    {
        final ConfigurationDAO configurationDAO = (ConfigurationDAO) context.getBean("configurationDAO");
        Properties topProps = ActionUtils.getTopProperties();
        if(topProps == null)
        {
            throw new RuntimeException("Unable to find top properties.");
        }
        List<String> commandList = new ArrayList<String>();
        commandList.add("DROP TABLE IF EXISTS configuration;");
        commandList.add("CREATE TABLE  configuration (" 
                        + " customer varchar(128)  NOT NULL, " 
                        + " name varchar(128)  NOT NULL,"
                        + " value varchar(128)  NOT NULL, " 
                        + " category varchar(32)  NOT NULL, " 
                        + " note varchar(512)  NOT NULL, " 
                        + "UNIQUE (customer, name, value, category)); ");
        
        Iterator<?> keysIter = topProps.keySet().iterator();
        while(keysIter.hasNext())
        {
            String key = (String)keysIter.next();
            String value = topProps.getProperty(key);
            commandList.add("INSERT INTO configuration (customer, name, value, category, note) VALUES  ('global', '" + key + "', '" + value + "', '', '');");
        }
        commandList.add("COMMIT;");
        
        String[] commands =commandList.toArray(new String[0]);
                
        configurationDAO.executeSQl(commands);
        
    }

}

