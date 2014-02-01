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

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import com.mvdb.etl.consumer.Consumer;
import com.mvdb.etl.data.ColumnMetadata;
import com.mvdb.etl.model.Order;

public interface OrderDAO
{
    public Map<String, ColumnMetadata> findMetadata(); 
    
    public void insert(Order order);

    public void insertBatch(List<Order> customer);

    public Order findByOrderId(long orderId);

    public List<Order> findAll();
    
    //public List<Order> findAll(Timestamp modifiedAfter);

    public void findAll(Timestamp modifiedAfter, Consumer consumer);
    
    public int findTotalOrders();
    
    //public int findTotalOrders(String customer);

    public long findMaxId();

    public long getNextSequenceValue();

    public void executeSQl(String[] sqlList);

    public void update(Order order);

}
