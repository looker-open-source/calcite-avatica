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

import java.util.Properties;

public class LookerTestCommon {
  private LookerTestCommon() {
  }

  private static final String BASE_URL = "https://localhost:19999";
  private static final String CLIENT_ID = "f6qG2zPw464yStBrJwrT";
  private static final String CLIENT_SECRET = "KQTpGMPp5mWRQy2Mgrs4SdQT";
  private static final String URL = "jdbc:avatica:remote:url=" + BASE_URL + ";serialization=LOOKER";

  private static final Properties BASE_PROPS = new Properties();

  static {
    BASE_PROPS.put("user", CLIENT_ID);
    BASE_PROPS.put("password", CLIENT_SECRET);
    BASE_PROPS.put("verifySSL", "false");
  }

  public static String getUrl() {
    return URL;
  }

  public static Properties getBaseProps() {
    return BASE_PROPS;
  }
}
