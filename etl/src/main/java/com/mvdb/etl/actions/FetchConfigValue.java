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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.FileUtils;

public class FetchConfigValue
{

    /**
     * @param args
     * @throws IOException 
     */
    public static void main(String[] args) throws IOException
    {
        String customerName = null;
        String valueName = null;
        String outputFileName = null;
        
        ActionUtils.setUpInitFileProperty();
        
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
            if (commandLine.hasOption("name"))
            {
                valueName = commandLine.getOptionValue("name");
            }
            if (commandLine.hasOption("outputFile"))
            {
                outputFileName = commandLine.getOptionValue("outputFile");
            }
            
            System.out.println("customerName:" + customerName);
            System.out.println("valueName:" + valueName);
            System.out.println("outputFileName:" + outputFileName);
            
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
        
        if (valueName == null)
        {
            System.err.println("Could not find valueName. Aborting...");
            System.exit(1);
        }
        
        if (outputFileName == null)
        {
            System.err.println("Could not find outputFileName. Aborting...");
            System.exit(1);
        }


        String value =  ActionUtils.getConfigurationValue(customerName, valueName);
        FileUtils.writeStringToFile(new File(outputFileName), value);
        
    }
    
    public static Options constructPosixOptions()
    {
        final Options posixOptions = new Options();
        posixOptions.addOption("customer", true, "Customer Name");
        posixOptions.addOption("name", true, "Value Name");
        posixOptions.addOption("outputFile", true, "Output File Name");

        return posixOptions;
    }

}
