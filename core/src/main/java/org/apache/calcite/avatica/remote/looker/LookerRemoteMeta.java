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

import static org.apache.calcite.avatica.remote.looker.utils.LookerUtils.safeSdkCall;

import com.looker.rtl.AuthSession;

import com.looker.sdk.LookerSDK;

import com.looker.sdk.SqlQuery;
import com.looker.sdk.SqlQueryCreate;

import java.util.List;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.RemoteMeta;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.remote.TypedValue;

public class LookerRemoteMeta extends RemoteMeta implements Meta {
  private AuthSession session;
  private LookerSDK sdk;

  public LookerRemoteMeta(AvaticaConnection connection, Service service) {
    super(connection, service);
    assert service.getClass() == LookerRemoteService.class;
  }

  public LookerSDK getSdk() {
    if (null == sdk) sdk = ((LookerRemoteService) service).sdk;
    return sdk;
  }


  @Override
  public Iterable<Object> createIterable(StatementHandle stmt, QueryState state, Signature signature,
      List<TypedValue> parameters, Frame firstFrame) {
    return super.createIterable(stmt, state, signature, parameters, firstFrame);
  }
}
