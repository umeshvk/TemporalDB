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

import java.text.DecimalFormat;

import com.mvdb.etl.data.MvdbKeyMaker;

public class GlobalMvdbKeyMaker implements MvdbKeyMaker
{
    DecimalFormat df = new DecimalFormat("0000000000000000");
    @Override
    public String makeKey(Object originalKeyValue)
    {
        if(originalKeyValue instanceof Long)
        {            
            return df.format(originalKeyValue);
        }
        //Default conversion
        return originalKeyValue.toString();
    }

}
