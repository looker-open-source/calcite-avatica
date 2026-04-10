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

import com.looker.rtl.SDKResponse;
import com.looker.sdk.LookerSDK;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LookerSdkFactoryTest {

  @Test
  public void testCreateSdkWithIapProperties() throws Exception {
    Properties props = new Properties();
    props.setProperty("token", "dummy_auth_token");
    props.setProperty("iap_client_id", "iap_client_id.apps.googleusercontent.com");
    props.setProperty("iap_service_account_email",
        "iap-service-account@sample.iam.gserviceaccount.com");

    String dummyUrl = "https://dummy.looker.com:19999";
    LookerSDK sdk = LookerSdkFactory.createSdk(dummyUrl, props);

    assertNotNull(
        "LookerSDK should instantiate successfully when IAP properties are provided", sdk);
  }

  @Test
  public void testQueryEndpoint_FormatsUrlCorrectly() {
    Long queryId = 12345L;
    String expectedEndpoint = "/api/4.0/sql_interface_queries/12345/run/json_bi";

    String actualEndpoint = LookerSdkFactory.queryEndpoint(queryId);

    assertEquals(
        "The query endpoint URL should be correctly formatted with the ID and json_bi format.",
        expectedEndpoint,
        actualEndpoint
    );
  }

//  @Test
//  public void testSafeSdkCall_OnSuccess_ReturnsResult() {
//    com.looker.rtl.SDKSuccess mockResponse = mock(com.looker.rtl.SDKSuccess.class);
//
//    when(mockResponse.getOk()).thenReturn(true);
//    when(mockResponse.getValue()).thenReturn((Object) "success_data");
//
//    LookerSdkFactory.LookerSDKCall successfulCall = () -> mockResponse;
//
//    Object result = LookerSdkFactory.safeSdkCall(successfulCall);
//
//    assertEquals("safeSdkCall should return the payload on a successful SDK call.",
//        "success_data", result);
//  }

  @Test
  public void testSafeSdkCall_OnError_WrapsInRuntimeException() {
    LookerSdkFactory.LookerSDKCall failingCall = () -> {
      throw new Error("Simulated Looker SDK Error");
    };

    RuntimeException exception = assertThrows(
        "safeSdkCall should catch java.lang.Error and wrap it in a RuntimeException.",
        RuntimeException.class,
        () -> LookerSdkFactory.safeSdkCall(failingCall)
    );

    assertNotNull("The original Error should be preserved as the cause.", exception.getCause());
    assertTrue("The cause should be an instance of Error.", exception.getCause() instanceof Error);
  }

  @Test
  public void testCreateSdk_AppliesDefaultUserAgent() throws SQLException {
    Properties props = new Properties();
    props.setProperty("token", "mock-token");

    LookerSDK sdk = LookerSdkFactory.createSdk("https://looker.example.com", props);

    Map<String, String> headers = sdk.getAuthSession().getApiSettings().getHeaders();
    assertEquals("Should default to DRIVER_USER_AGENT if none is provided.",
        "looker-jdbc-driver-1.24.1", headers.get("User-Agent"));
  }

  @Test
  public void testCreateSdk_AppliesCustomUserAgent() throws SQLException {
    String customAgent = "my-custom-calcite-client-v2";
    Properties props = new Properties();
    props.setProperty("token", "mock-token");
    props.setProperty("userAgent", customAgent);

    LookerSDK sdk = LookerSdkFactory.createSdk("https://looker.example.com", props);

    Map<String, String> headers = sdk.getAuthSession().getApiSettings().getHeaders();
    assertEquals("Should use the custom userAgent provided in the properties.",
        customAgent, headers.get("User-Agent"));
  }
}
