/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.avatica.remote.looker;


import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Test for Looker specific functionality in {@link LookerRemoteMeta} implementations.
 */
public class LookerRemoteMetaTest {

  @Ignore
  @Test
  public void testIt() throws SQLException {
    Connection connection = DriverManager.getConnection(LookerTestCommon.getUrl(),
        LookerTestCommon.getBaseProps());
    ResultSet models = connection.getMetaData().getSchemas();
    while (models.next()) {
      System.out.println(models.getObject(1));
    }
    ResultSet test = connection.createStatement().executeQuery("SELECT\n"
        + "    (FORMAT_TIMESTAMP('%F %T', order_items.created_at )) AS order_items_created_time, "
        + "'AHHHH' as testy, 1000000 as num\n"
        + "FROM `bigquery-public-data.thelook_ecommerce.order_items`\n" + "     AS order_items\n"
        + "GROUP BY\n" + "    1\n" + "ORDER BY\n" + "    1 DESC LIMIT 101");
    int i = 0;
    while (test.next()) {
      i++;
      System.out.println(
          i + ": " + test.getObject(1) + "||" + test.getObject(2) + "||" + test.getObject(3));
    }
    System.out.println("END !!!!!");
  }
}
