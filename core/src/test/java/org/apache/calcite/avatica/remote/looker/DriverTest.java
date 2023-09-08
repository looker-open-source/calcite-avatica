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

import org.apache.calcite.avatica.AvaticaConnection;

import org.junit.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class DriverTest {

  @Test
  public void lookerDriverIsRegistered() throws SQLException {
    Driver driver = DriverManager.getDriver("jdbc:looker:url=foobar.com");

    assertThat(driver, is(instanceOf(org.apache.calcite.avatica.remote.looker.Driver.class)));
  }

  @Test
  public void driverThrowsAuthExceptionForBlankProperties() throws SQLException {
    Properties props = new Properties();
    try {
      Driver driver = DriverManager.getDriver("jdbc:looker:url=foobar.com");
      driver.connect("jdbc:looker:url=foobar.com", props);

      fail("Should have thrown an auth exception!");
    } catch (SQLInvalidAuthorizationSpecException e) {
      assertThat(e.getMessage(), is("Invalid connection params.\nMissing either API3 credentials"
          + " or access token"));
    }
  }

  @Test
  public void createsAvaticaConnections() throws SQLException {
    Properties props = new Properties();
    props.put("token", "foobar");

    Driver driver = DriverManager.getDriver("jdbc:looker:url=foobar.com");
    Connection connection = driver.connect("jdbc:looker:url=foobar.com", props);

    assertThat(connection, is(instanceOf(AvaticaConnection.class)));
  }
}
