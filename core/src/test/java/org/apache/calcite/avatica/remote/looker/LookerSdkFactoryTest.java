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

import com.looker.rtl.AuthSession;
import com.looker.rtl.AuthToken;
import com.looker.sdk.LookerSDK;

import org.junit.Test;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LookerSdkFactoryTest {
  private static final String USER_AGENT = "User-Agent";

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
        "LookerSDK should instantiate successfully when IAP properties are provided.", sdk);
  }

  @Test
  public void testIapTokenIncludesEmail() {
    String dummyIapToken = "dummy-token";
    LookerRemoteService mockService = mock(LookerRemoteService.class);
    LookerSDK mockSdk = mock(LookerSDK.class);
    AuthSession mockSession = mock(AuthSession.class);
    com.looker.rtl.Transport mockTransport = mock(com.looker.rtl.Transport.class);
    com.looker.rtl.ConfigurationProvider mockOptions = mock(
        com.looker.rtl.ConfigurationProvider.class);

    mockService.sdk = mockSdk;
    when(mockSdk.getAuthSession()).thenReturn(mockSession);
    when(mockSession.getTransport()).thenReturn(mockTransport);
    when(mockSession.getAuthToken()).thenReturn(new AuthToken(
        "access", "Bearer", 3600L, null));
    when(mockSession.getApiSettings()).thenReturn(
        com.looker.sdk.ApiSettings.fromMap(new HashMap<>()));

    when(mockTransport.getOptions()).thenReturn(mockOptions);
    when(mockOptions.getVerifySSL()).thenReturn(true);
    when(mockOptions.getTimeout()).thenReturn(120);
    when(mockTransport.makeUrl(anyString(), anyMap(), any())).thenReturn("https://localhost/api");

    when(mockSession.fetchIapToken()).thenReturn(dummyIapToken);

    LookerRemoteMeta meta = new LookerRemoteMeta(null, mockService);

    try {
      meta.makeRunQueryRequest("/some/path");
    } catch (Exception ignored) {
    }

    verify(mockSession).fetchIapToken();
  }

  @Test
  public void testSafeSdkCallOnErrorWrapsInRuntimeException() {
    String expectedMessage = "Simulated Looker SDK Error";
    LookerSdkFactory.LookerSDKCall failingCall = () -> {
      throw new Error(expectedMessage);
    };

    RuntimeException exception = assertThrows(
        "safeSdkCall should catch java.lang.Error and wrap it in a RuntimeException.",
        RuntimeException.class,
        () -> LookerSdkFactory.safeSdkCall(failingCall)
    );

    assertNotNull("The original Error should be preserved as the cause.",
        exception.getCause()
    );
    assertTrue("The cause should be an instance of Error.",
        exception.getCause() instanceof Error
    );
    assertEquals("The cause's message should also match.",
        expectedMessage,
        exception.getCause().getMessage()
    );
  }

  @Test
  public void testCreateSdkAppliesCustomUserAgent() throws SQLException {
    String customAgent = "my-custom-calcite-client-v2";
    Properties props = new Properties();
    props.setProperty("token", "mock-token");
    props.setProperty("userAgent", customAgent);

    LookerSDK sdk = LookerSdkFactory.createSdk("https://looker.example.com", props);

    Map<String, String> headers = sdk.getAuthSession().getApiSettings().getHeaders();
    assertEquals("Should use the custom userAgent provided in the properties.",
        customAgent, headers.get(USER_AGENT));
  }
}
