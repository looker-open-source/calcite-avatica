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

import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertNotNull;

public class LookerSdkFactoryTest {

  @Test
  public void testCreateSdkWithIapProperties() throws Exception {
    Properties props = new Properties();
    props.setProperty("token", "dummy_auth_token");
    props.setProperty("iap_client_id", "iap_client_id.apps.googleusercontent.com");
    props.setProperty("iap_service_account_email",
        "iap-service-account@sample.iam.gserviceaccount.com");

    String dummyUrl = "https://dummy.looker.com:19999";
    com.looker.sdk.LookerSDK sdk = LookerSdkFactory.createSdk(dummyUrl, props);

    assertNotNull(
        "LookerSDK should instantiate successfully when IAP properties are provided", sdk);
  }
}
