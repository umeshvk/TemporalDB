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
package com.mvdb.etl.dao;

import java.util.List;

import com.mvdb.etl.model.Configuration;

public interface ConfigurationDAO
{
    public void insert(Configuration configuration);


    public Configuration find(String customer, String name);

    public List<Configuration> findAll();
    
    public int getCount(String customer);
    
    public int getCustomerCount();

    public int update(Configuration configuration, String requiredOldValue);
    
    public void executeSQl(String[] sqlList);
    
    public boolean acquireLock(String customer, String name);
    public boolean releaseLock(String customer, String name);

}
