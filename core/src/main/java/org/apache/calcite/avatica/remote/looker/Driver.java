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
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.UnregisteredDriver;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.remote.looker.utils.LookerSdkFactory;

import com.looker.sdk.LookerSDK;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class Driver extends UnregisteredDriver {

  static {
    new Driver().register();
  }

  public Driver() {
    super();
  }

  public static final String CONNECT_STRING_PREFIX = "jdbc:looker:";

  @Override
  protected DriverVersion createDriverVersion() {
    return DriverVersion.load(
        org.apache.calcite.avatica.remote.Driver.class,
        "org-apache-calcite-jdbc.properties",
        "Looker JDBC Driver",
        "unknown version",
        "Looker",
        "unknown version");
  }

  @Override
  protected String getConnectStringPrefix() {
    return CONNECT_STRING_PREFIX;
  }

  @Override
  public Meta createMeta(AvaticaConnection connection) {
    final Service service = new LookerRemoteService();
    connection.setService(service);
    return new LookerRemoteMeta(connection, service);
  }

  @Override public Connection connect(String url, Properties info)
      throws SQLException {
    AvaticaConnection conn = (AvaticaConnection) super.connect(url, info);

    if (conn == null) {
      // It's not an url for our driver
      return null;
    }
    Service service = conn.getService();
    // the `looker` driver should always have a matching Service
    assert service instanceof LookerRemoteService;
    // create and set LookerSDK for the connection
    LookerSDK sdk = LookerSdkFactory.createSdk(conn.config().url(), info);
    ((LookerRemoteService) service).setSdk(sdk);
    return conn;
  }

}
