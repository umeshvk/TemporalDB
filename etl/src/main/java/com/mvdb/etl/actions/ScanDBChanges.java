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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.mvdb.etl.dao.ConfigurationDAO;
import com.mvdb.etl.dao.GenericDAO;
import com.mvdb.etl.data.Metadata;
import com.mvdb.etl.model.Configuration;

public class ScanDBChanges  implements IAction
{
    private static Logger logger = LoggerFactory.getLogger(ScanDBChanges.class);

    public static void main(String[] args) throws IOException
    {

        
        String customerName = null;
        String snapshotDir = null;
        final CommandLineParser cmdLinePosixParser = new PosixParser();
        final Options posixOptions = constructPosixOptions();
        CommandLine commandLine;
        try
        {
            commandLine = cmdLinePosixParser.parse(posixOptions, args);
            if (commandLine.hasOption("customer"))
            {
                customerName = commandLine.getOptionValue("customer");
            }
            if (commandLine.hasOption("snapshotDir"))
            {
                snapshotDir = commandLine.getOptionValue("snapshotDir");
            }
        } catch (ParseException parseException) // checked exception
        {
            System.err
                    .println("Encountered exception while parsing using PosixParser:\n" + parseException.getMessage());
        }

        if (customerName == null)
        {
            System.err.println("Could not find customerName. Aborting...");
            System.exit(1);
        }
        if (snapshotDir == null)
        {
            System.err.println("Could not find snapshotDir. Aborting...");
            System.exit(1);
        }

        ApplicationContext context = Top.getContext();

        final ConfigurationDAO configurationDAO = (ConfigurationDAO) context.getBean("configurationDAO");
        final GenericDAO genericDAO = (GenericDAO)context.getBean("genericDAO");
        File snapshotDirectory = getSnapshotDirectory(configurationDAO, customerName, snapshotDir);
        //write file schema-orders.dat in snapshotDirectory
        Metadata metadata= genericDAO.getMetadata("orders", snapshotDirectory);
        //writes files: header-orders.dat, data-orders.dat in snapshotDirectory
        genericDAO.scan2("orders", snapshotDirectory);
        
        

    }



    private static File getSnapshotDirectory(ConfigurationDAO configurationDAO, String customerName, String snapshotDir)
    {
        Configuration dataRootDirConfig = configurationDAO.find(ConfigurationKeys.GLOBAL_CUSTOMER, ConfigurationKeys.GLOBAL_LOCAL_DATA_ROOT);
        String dataRootDir = dataRootDirConfig.getValue();
        File customerDir = new File(dataRootDir, customerName);
        File snapshotDirectory = new File(customerDir, snapshotDir);
        return snapshotDirectory;
    }

    public static Options constructPosixOptions()
    {
        final Options posixOptions = new Options();
        posixOptions.addOption("customer", true, "Customer Name");
        posixOptions.addOption("snapshotDir", true, "Snapshot directory(YYMMDDHHMMSS format)");
        

        return posixOptions;
    }
}
