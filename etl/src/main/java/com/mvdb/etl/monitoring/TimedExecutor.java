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
package com.mvdb.etl.monitoring;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public abstract class TimedExecutor
{

    protected String      name             = "NamelessOperation";
    ByteArrayOutputStream baos;
    PrintStream           ps;
    long                  timeConsumedinMS = -1;

    public TimedExecutor()
    {
        baos = new ByteArrayOutputStream();
        ps = new PrintStream(baos);
    }

    public abstract void execute();

    public PrintStream getPrintStream()
    {
        return ps;
    }

    public String getOperationName()
    {
        return name;
    }

    public final long timedExecute()
    {

        long t1 = System.currentTimeMillis();
        long t2 = t1;
        try
        {
            execute();
        } catch (Throwable t)
        {
            t.printStackTrace();
        } finally
        {
            t2 = System.currentTimeMillis();
            timeConsumedinMS = t2 - t1;
            getPrintStream().println("Operation " + getOperationName() + " executed in time(ms):" + timeConsumedinMS);
            getPrintStream().flush();
            return timeConsumedinMS;
        }

    }

    public String getMessage()
    {
        return baos.toString();
    }

}
