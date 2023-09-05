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
import com.looker.rtl.SDKErrorInfo;
import com.looker.rtl.SDKResponse;
import com.looker.rtl.Transport;
import com.looker.rtl.TransportKt;
import com.looker.sdk.ApiSettings;
import com.looker.sdk.LookerSDK;

import java.sql.SQLException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import static com.looker.rtl.TransportKt.ok;
import static com.looker.rtl.TransportKt.parseSDKError;

import static org.apache.calcite.avatica.remote.Service.OpenConnectionRequest.serializeProperties;

/**
 * Utility class for generating, authenticating, and calling {@link LookerSDK}s.
 */
public class LookerSdkFactory {

  private LookerSdkFactory() {
  }

  private static final String RESULT_FORMAT = "json_bi";
  private static final String QUERY_ENDPOINT = "/api/4.0/sql_interface_queries/%s/run/%s";
  /**
   * Default buffer size. Could probably be more or less. 1024 chosen for now.
   */
  public static final int DEFAULT_STREAM_BUFFER_SIZE = 1024;


  /**
   * Simple functional interface to wrap SDK calls
   */
  public interface LookerSDKCall {
    SDKResponse call();
  }

  /**
   * Wraps {@link SQLException}s as {@link RuntimeException}s. Almost all exceptions in Avatica are
   * thrown as RuntimeExceptions. There are 'TODO's to change this behavior but until those are
   * resolved we should do the same. RuntimeExceptions do not have to be part of the method
   * signature so it does make things nicer to work with.
   */
  public static RuntimeException handle(String errorMessage) {
    return new RuntimeException(errorMessage);
  }

  /**
   * Makes the API endpoint to run a previously made query.
   */
  public static String queryEndpoint(Long id) {
    return String.format(Locale.ROOT, QUERY_ENDPOINT, TransportKt.encodeParam(id), RESULT_FORMAT);
  }

  /**
   * Makes the SDK call and throws any errors as runtime {@link SQLException}s
   */
  public static <T> T safeSdkCall(LookerSDKCall sdkCall) {
    try {
      return ok(sdkCall.call());
    } catch (Error e) {
      SDKErrorInfo error = parseSDKError(e.toString());
      // TODO: Get full errors from error.errors array
      throw handle(error.getMessage());
    }
  }

  private static boolean hasApiCreds(Map<String, String> props) {
    return props.containsKey("user") && props.containsKey("password");
  }

  private static boolean hasAuthToken(Map<String, String> props) {
    return props.containsKey("token");
  }

  /**
   * Creates a {@link AuthSession} to a Looker instance.
   * <p>If {@code client_id} and {@code client_secret} are provided in {@code props} then
   * {@link AuthSession#login} is called on the session. Otherwise, if {@code token} is provided
   * then its value is set as the auth token in the HTTP header for all requests for the session.
   *
   * @param url the URL of the Looker instance.
   * @param props map of properties for the session.
   */
  private static AuthSession createAuthSession(String url, Map<String, String> props)
      throws SQLException {
    Map<String, String> apiConfig = new HashMap<>();
    apiConfig.put("base_url", url);
    apiConfig.put("timeout", props.get(props.getOrDefault("timeout", "120")));
    apiConfig.put("verify_ssl", props.get("verifySSL"));

    boolean apiLogin = hasApiCreds(props);
    boolean authToken = hasAuthToken(props);

    if (apiLogin && authToken) {
      throw new SQLInvalidAuthorizationSpecException("Invalid connection params.\n"
          + "Cannot provide both API3 credentials and an access token");
    } else if (apiLogin) {
      apiConfig.put("client_id", props.get("user"));
      apiConfig.put("client_secret", props.get("password"));
    } else if (authToken) {
      // TODO b/295025684: Set the token for the session using `session.setAuthToken(AuthToken);`.
      //  Doing so will allow us to rely on the same auth session for the stream query call.
      Map<String, String> headers = new HashMap<>();
      headers.put("Authorization", "token " + props.get("token"));
      apiConfig.put("headers", headers.toString());
    } else {
      throw new SQLInvalidAuthorizationSpecException(
          "Invalid connection params.\n" + "Missing either API3 credentials or access token");
    }

    ConfigurationProvider finalizedConfig = ApiSettings.fromMap(apiConfig);
    AuthSession session = new AuthSession(finalizedConfig, new Transport(finalizedConfig));
    // need to log in if client_id and client_secret are used
    if (apiLogin) {
      // empty string means no sudo - we won't support this
      session.login("");
    }

    return session;
  }

  /**
   * Creates an authenticated {@link LookerSDK}.
   *
   * @param url the URL of the Looker instance.
   * @param props map of properties for the session.
   */
  public static LookerSDK createSdk(String url, Properties props) throws SQLException {
    Map<String, String> stringProps = serializeProperties(props);
    AuthSession session = createAuthSession(url, stringProps);
    return new LookerSDK(session);
  }
}
