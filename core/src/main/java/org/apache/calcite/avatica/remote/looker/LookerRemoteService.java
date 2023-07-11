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

import static com.looker.rtl.TransportKt.ok;

import static org.apache.calcite.avatica.remote.looker.utils.LookerUtils.safeSdkCall;

import com.looker.sdk.JdbcInterface;
import com.looker.sdk.LookerSDK;

import com.looker.sdk.SqlQuery;
import com.looker.sdk.SqlQueryCreate;

import java.io.IOException;

import java.sql.SQLException;

import org.apache.calcite.avatica.remote.JsonService;
import org.apache.calcite.avatica.remote.looker.utils.LookerSdkFactory;
import org.apache.calcite.avatica.remote.looker.utils.LookerUtils;

public class LookerRemoteService extends JsonService {
  private String url;
  public LookerSDK sdk;

  public LookerRemoteService(String url) {
    super();
    this.url = url;
  }

  /** Fallback to default Avatica behavior when we don't need Looker specific handling */
  @Override
  public String apply(String request) {
    JdbcInterface response = safeSdkCall(() -> sdk.jdbc_interface(request));
    return response.getResults();
  }

  @Override
  public ExecuteResponse apply(PrepareAndExecuteRequest request) {
    String results = safeSdkCall(() -> {
      SqlQueryCreate sqlQuery = new SqlQueryCreate(
          /* connection_name=*/ null,
          /* connection_id=*/ null,
          /* model_name=*/ "thelook",
          /* sql=*/ request.sql,
          /* vis_config=*/ null
      );
      SqlQuery query = ok(sdk.create_sql_query(sqlQuery));
      return sdk.run_sql_query(query.getSlug(), "json");
    });
    System.out.println(results);
    return super.apply(request);
  }

  @Override
  public OpenConnectionResponse apply(OpenConnectionRequest request) {
    try {
      sdk = LookerSdkFactory.createSdk(url, request.info);
      return decode(apply(encode(request)), OpenConnectionResponse.class);
    } catch (IOException e) {
      throw handle(e);
    } catch (SQLException e) {
      throw LookerUtils.handle(e);
    }
  }
}
