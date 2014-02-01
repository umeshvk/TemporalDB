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
package com.mvdb.etl.consumer;

import java.util.ArrayList;
import java.util.List;

import com.mvdb.etl.model.Order;

public class OrderConsumer implements Consumer
{

   List<Order> orders = new ArrayList<Order>();

    @Override
    public void consume(Object object)
    {
        Order order = (Order)object;
        orders.add(order);        
    }

    public List<Order> getOrders()
    {
        return orders;
    }
    
    

}
