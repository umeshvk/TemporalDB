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
package com.mvdb.etl.dao.impl;

import java.sql.Timestamp;
import java.util.Date;

import com.mvdb.etl.data.MvdbUpdateTimeMaker;

public class GlobalMvdbUpdateTimeMaker implements MvdbUpdateTimeMaker
{

    @Override
    public Date makeMvdbUpdateTime(Object originalUpdateTimeValue)
    {
        if(originalUpdateTimeValue instanceof Timestamp)
        {            
            return (Date)originalUpdateTimeValue;
        }
        //Must map to something meaningful. Otherwise deal with the failure
        return null;
    }

}
