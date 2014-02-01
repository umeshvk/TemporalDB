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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.support.JdbcDaoSupport;

import com.mvdb.etl.dao.ConfigurationDAO;
import com.mvdb.etl.model.Configuration;
import com.mvdb.etl.model.ConfigurationRowMapper;

public class JdbcConfigurationDAO extends JdbcDaoSupport implements ConfigurationDAO
{

    @Override
    public void insert(Configuration configuration)
    {

        String sql = "INSERT INTO configuration "
                + "(customer, name, value) VALUES (?, ?, ?)";

        getJdbcTemplate().update(
                sql,
                new Object[] { configuration.getCustomer(), configuration.getName(), configuration.getValue() });

    }

   
    @Override
    public Configuration find(String customer, String name)
    {
        String sql = "SELECT * FROM configuration WHERE customer = ? AND name = ?";

        Configuration configuration = (Configuration) getJdbcTemplate().queryForObject(sql, new Object[] { customer, name }, new ConfigurationRowMapper());

        return configuration;
    }



    @Override
    public int getCount(String customer)
    {
        String sql = "SELECT COUNT(*) FROM configuration where customer = ?";
        int total = getJdbcTemplate().queryForInt(sql, new Object[] { customer});
        return total;
    }



    @Override
    public int getCustomerCount()
    {
        String sql = "select count(*) from (select count(*) from configuration group by customer) x";
        int total = getJdbcTemplate().queryForInt(sql);
        return total;
    }


    @Override
    public boolean acquireLock(String customer, String name)
    {
        Configuration configuration = new Configuration();
        configuration.setCustomer(customer);
        configuration.setName(name);
        configuration.setValue("1");
        int updateCount = update(configuration, "0");
        if(updateCount == 0)
        {
            return false;
        }
        return true;
    }


    @Override
    public boolean releaseLock(String customer, String name)
    {
        Configuration configuration = new Configuration();
        configuration.setCustomer(customer);
        configuration.setName(name);
        configuration.setValue("0");
        int updateCount = update(configuration, "1");
        if(updateCount == 0)
        {
            return false;
        }
        return true;
    }

    @Override
    public int update(Configuration configuration, String requiredOldValue)
    {
        int updateCount = -1;
        if(requiredOldValue == null)
        {
           updateCount = 
                getJdbcTemplate().update(
                        "update configuration set value = ? where customer = ? AND name = ?", new Object[] { 
                        configuration.getValue(), 
                        configuration.getCustomer(), 
                        configuration.getName()});
        } else 
        {
            updateCount =
                getJdbcTemplate().update(
                          "update configuration set value = ? where customer = ? AND name = ? AND value = ? ", new Object[] { 
                           configuration.getValue(), 
                           configuration.getCustomer(), 
                           configuration.getName(), 
                           requiredOldValue});
        }
        
        return updateCount;
    }


    

    @Override
    public List<Configuration> findAll()
    {
      String sql = "SELECT * FROM configuration";        
      List<Configuration> configurations = findAll(sql);
      return configurations;
    }


    private List<Configuration> findAll(String sql)
    {
        List<Configuration> configurations = new ArrayList<Configuration>();

        List<Map> rows = getJdbcTemplate().queryForList(sql);
        for (Map row : rows)
        {
            Configuration configuration = new Configuration();
            configuration.setCustomer((String) (row.get("customer")));
            configuration.setName((String) row.get("name"));
            configuration.setValue((String) row.get("value"));
            configurations.add(configuration);
        }

        return configurations;
        
       
    }


    @Override
    public void executeSQl(String[] sqlList)
    {

        for (String sql : sqlList)
        {
            getJdbcTemplate().update(sql);
        }
    }

}
