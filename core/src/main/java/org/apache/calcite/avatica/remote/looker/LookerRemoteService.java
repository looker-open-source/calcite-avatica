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

import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.remote.JsonService;
import org.apache.calcite.avatica.remote.looker.LookerRemoteMeta.LookerFrame;

import com.looker.sdk.JdbcInterface;
import com.looker.sdk.LookerSDK;
import com.looker.sdk.SqlInterfaceQuery;
import com.looker.sdk.WriteSqlInterfaceQueryCreate;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.calcite.avatica.remote.looker.LookerSdkFactory.safeSdkCall;

/**
 * Implementation of {@link org.apache.calcite.avatica.remote.Service} that uses the Looker SDK to
 * send Avatica request/responses to a Looker instance via JSON.
 */
public class LookerRemoteService extends JsonService {

  public LookerSDK sdk;

  void setSdk(LookerSDK sdk) {
    this.sdk = sdk;
  }

  /**
   * Helper method to create a {@link ExecuteResponse} for this request. Since we are using the
   * Looker SDK we need to create this response client side.
   */
  ExecuteResponse lookerExecuteResponse(PrepareAndExecuteRequest request, Signature signature,
      LookerFrame lookerFrame) {
    ResultSetResponse rs = new ResultSetResponse(request.connectionId, request.statementId, false,
        signature, lookerFrame, -1, null);
    return new ExecuteResponse(Arrays.asList(new ResultSetResponse[]{rs}), false, null);
  }

  /**
   * Handles all non-overridden {@code apply} methods.
   *
   * Calls the {@code jdbc_interface} endpoint of the instance which behaves similarly to a standard
   * Avatica server.
   */
  @Override
  public String apply(String request) {
    assert null != sdk : "No authenticated SDK";
    JdbcInterface response = safeSdkCall(() -> sdk.jdbc_interface(request));
    return response.getResults();
  }

  /**
   * Handles PrepareAndExecuteRequests by preparing a query via {@link LookerSDK#create_sql_query}
   * whose response contains a query id. This id is used to execute the query via
   * {@link LookerSDK#run_sql_query} with the 'json_bi' format.
   *
   * @param request the base Avatica request to convert into a Looker SDK call.
   * @return a {@link ExecuteResponse} containing a prepared {@link LookerFrame}.
   */
  @Override
  public ExecuteResponse apply(PrepareAndExecuteRequest request) {
    assert null != sdk;

    WriteSqlInterfaceQueryCreate queryRequest = new WriteSqlInterfaceQueryCreate(
        request.sql, /*jdbcClient=*/true);
    SqlInterfaceQuery preparedQuery = safeSdkCall(
        () -> sdk.create_sql_interface_query(queryRequest));

    Signature signature;
    try {
      signature = JsonService.MAPPER.readValue(preparedQuery.getSignature(), Signature.class);
    } catch (IOException e) {
      throw handle(e);
    }
    return lookerExecuteResponse(request, signature, LookerFrame.create(preparedQuery.getId()));
  }
}
