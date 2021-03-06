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
package com.mvdb.etl.model;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

public class OrderRowMapper implements RowMapper
{
    public Object mapRow(ResultSet rs, int rowNum) throws SQLException
    {
        Order order = new Order();
        order.setOrderId(rs.getLong("ORDER_ID"));
        order.setNote(rs.getString("NOTE"));
        order.setSaleCode(rs.getInt("SALE_CODE"));
        order.setCreateTime(new java.util.Date(rs.getDate("CREATE_TIME").getTime()));
        order.setUpdateTime(new java.util.Date(rs.getDate("UPDATE_TIME").getTime()));
        return order;
    }

}
