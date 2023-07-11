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

import static com.looker.rtl.TransportKt.ok;
import static com.looker.rtl.TransportKt.parseSDKError;

import com.looker.rtl.SDKErrorInfo;
import com.looker.rtl.SDKResponse;

import java.sql.SQLException;

public class LookerUtils {
  public interface LookerSDKCall {
    SDKResponse call();
  }

  public static RuntimeException handle(SQLException e) {
    return new RuntimeException(e);
  }

  public static <T> T safeSdkCall(LookerSDKCall sdkCall) {
    try {
      return ok(sdkCall.call());
    } catch (Error e) {
      SDKErrorInfo error = parseSDKError(e.toString());
      // TODO: Get full errors from error.errors array
      throw handle(new SQLException(error.getMessage()));
    }
  }
}
