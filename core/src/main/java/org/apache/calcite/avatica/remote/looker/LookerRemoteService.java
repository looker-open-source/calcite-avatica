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
import org.apache.calcite.avatica.remote.looker.utils.LookerSdkFactory;

import com.looker.sdk.JdbcInterface;
import com.looker.sdk.LookerSDK;
import com.looker.sdk.SqlQuery;
import com.looker.sdk.SqlQueryCreate;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;

import static org.apache.calcite.avatica.remote.looker.utils.LookerSdkFactory.safeSdkCall;

/**
 * Implementation of {@link org.apache.calcite.avatica.remote.Service} that uses the Looker SDK to
 * send Avatica request/responses to a Looker instance via JSON.
 */
public class LookerRemoteService extends JsonService {

  private String url;
  public LookerSDK sdk;

  public LookerRemoteService(String url) {
    super();
    this.url = url;
  }

  /**
   * Non-overridden {@code apply} methods hit the {@code jdbc_interface} endpoint of the instance.
   * This endpoint behaves similarly to a standard Avatica server.
   */
  @Override
  public String apply(String request) {
    JdbcInterface response = safeSdkCall(() -> sdk.jdbc_interface(request));
    return response.getResults();
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
   * Handles PrepareAndExecuteRequests by preparing a query via {@link LookerSDK#create_sql_query}
   * whose response contains a slug. This slug is used to execute the query via
   * {@link LookerSDK#run_sql_query} with the 'json_bi' format.
   *
   * @param request the base Avatica request to convert into a Looker SDK call.
   * @return a {@link ExecuteResponse} containing a prepared {@link LookerFrame}.
   */
  @Override
  public ExecuteResponse apply(PrepareAndExecuteRequest request) {
    // TODO: b/288031194 - Remove this stubbed query once the Looker SQL endpoints exist.
    //  For dev we first prepare the query to get a signature and then create a query to run.
    String prepSql = "SELECT\n" + "    (FORMAT_TIMESTAMP('%F %T', `order_items.created_time` )) AS "
        + "order_items_created_time, 'AHHHH' as testy, 10000 as num\n"
        + "FROM `thelook`.`order_items`\n" + "     AS order_items\n" + "GROUP BY\n" + "    1\n"
        + "ORDER BY\n" + "    1 DESC";
    PrepareRequest prepareRequest = new PrepareRequest("looker-adapter", prepSql, -1);
    PrepareResponse prepare = super.apply(prepareRequest);
    SqlQuery query = safeSdkCall(() -> {
      SqlQueryCreate sqlQuery = new SqlQueryCreate(
          /* connection_name=*/ null,
          /* connection_id=*/ null,
          /* model_name=*/ "thelook",
          /* sql=*/ request.sql,
          /* vis_config=*/ null);
      return sdk.create_sql_query(sqlQuery);
    });
    return lookerExecuteResponse(request, prepare.statement.signature,
        LookerFrame.create(query.getSlug()));
  }

  /**
   * Opening a connection initializes a {@link LookerSDK} to communicate with the Looker API.
   */
  @Override
  public OpenConnectionResponse apply(OpenConnectionRequest request) {
    try {
      sdk = LookerSdkFactory.createSdk(url, request.info);
      return decode(apply(encode(request)), OpenConnectionResponse.class);
    } catch (IOException e) {
      throw handle(e);
    } catch (SQLException e) {
      throw LookerSdkFactory.handle(e);
    }
  }
}
