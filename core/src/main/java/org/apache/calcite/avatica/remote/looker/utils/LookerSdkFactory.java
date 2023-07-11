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

package org.apache.calcite.avatica.remote.looker.utils;

import com.looker.rtl.AuthSession;
import com.looker.rtl.ConfigurationProvider;
import com.looker.rtl.Transport;
import com.looker.sdk.ApiSettings;

import com.looker.sdk.LookerSDK;

import java.sql.SQLException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.avatica.BuiltInConnectionProperty;

public class LookerSdkFactory {
  static private boolean hasApiCreds(Map<String, String> props) {
    return props.containsKey("user") && props.containsKey("password");
  }

  static private boolean hasAuthToken(Map<String, String> props) {
    return props.containsKey("token");
  }

  static public AuthSession createAuthSession(String url, Map<String, String> props) throws SQLException {
    Map<String, String> apiConfig = new HashMap<>();
    apiConfig.put("base_url", url);
    apiConfig.put("timeout", props.get(props.getOrDefault("timeout", "120")));
    apiConfig.put("verify_ssl", props.get("verifySSL"));

    boolean apiLogin = hasApiCreds(props);
    boolean authToken = hasAuthToken(props);

    if (apiLogin && authToken) {
      throw new SQLInvalidAuthorizationSpecException("Invalid connection params.\n" +
          "Cannot provide both API3 credentials and an access token");
    } else if (apiLogin) {
      apiConfig.put("client_id", props.get("user"));
      apiConfig.put("client_secret", props.get("password"));
    } else if (authToken) {
      Map<String, String> headers = new HashMap<>();
      headers.put("Authorization", "token " + props.get("token"));
      apiConfig.put("headers", headers.toString());
    } else {
      throw new SQLInvalidAuthorizationSpecException("Invalid connection params.\n" +
          "Missing either API3 credentials or access token");
    }

    ConfigurationProvider finalizedConfig = ApiSettings.fromMap(apiConfig);
    AuthSession session = new AuthSession(finalizedConfig, new Transport(finalizedConfig));

    if (apiLogin) {
      // empty string means no sudo - we won't support this
      session.login("");
    }

    return session;
  }

  static public LookerSDK createSdk(String url, Map<String, String> props) throws SQLException {
    AuthSession session = createAuthSession(url, props);
    return new LookerSDK(session);
  }
}
