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

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.RemoteMeta;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.remote.Service.PrepareAndExecuteRequest;
import org.apache.calcite.avatica.remote.Service.PrepareRequest;
import org.apache.calcite.avatica.remote.TypedValue;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.looker.rtl.AuthSession;
import com.looker.rtl.Transport;
import com.looker.sdk.LookerSDK;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * Implementation of Meta that works in tandem with {@link LookerRemoteService} to stream results
 * from the Looker SDK.
 */
public class LookerRemoteMeta extends RemoteMeta implements Meta {
  private final LookerRemoteService lookerService;

  public LookerRemoteMeta(AvaticaConnection connection, Service service) {
    super(connection, service);
    // this class _must_ be backed by a LookerRemoteService
    assert service.getClass() == LookerRemoteService.class;
    lookerService = (LookerRemoteService) service;
  }

  /**
   * Constants used in JSON parsing
   */
  private static final String ROWS_KEY = "rows";
  private static final String VALUE_KEY = "value";

  /**
   * Default queue size. Could probably be more or less. 10 chosen for now.
   */
  private static final int DEFAULT_FRAME_QUEUE_SIZE = 10;

  /**
   * Returns authenticated LookerSDK from LookerRemoteService.
   */
  private LookerSDK getSdk() {
    return lookerService.sdk;
  }

  /**
   * A single meta can have multiple running statements. This map keeps track of
   * {@code FrameEnvelopes}s that belong to a running statement. See {@link #prepareStreamingThread}
   * for more details.
   */
  private final ConcurrentMap<StatementHandle, BlockingQueue<FrameEnvelope>> stmtQueueMap =
      new ConcurrentHashMap();

  /**
   * Wrapper for either a {@link Frame} or {@link Exception}. Allows for {@link #stmtQueueMap} to
   * hold complete Frames and present exceptions when consumers are ready to encounter them.
   */
  private class FrameEnvelope {

    final Frame frame;
    final Exception exception;

    FrameEnvelope(/*@Nullable*/ Frame frame, /*@Nullable*/ Exception exception) {
      this.frame = frame;
      this.exception = exception;
    }

    boolean hasException() {
      return this.exception != null;
    }
  }

  private FrameEnvelope makeFrame(long offset, boolean done, Iterable<Object> rows) {
    Frame frame = new Frame(offset, done, rows);
    return new FrameEnvelope(frame, null);
  }

  private FrameEnvelope makeException(Exception e) {
    return new FrameEnvelope(null, e);
  }

  /**
   * Calls the correct method to read the current value on the stream. The {@code get} methods do
   * not advance the current token, so they can be called multiple times without changing the state
   * of the parser.
   *
   * @param columnMetaData the {@link ColumnMetaData} for this value. It is important to use the
   *     {@link Rep} rather than the type name since Avatica represents most datetime values as
   *     milliseconds since epoch via {@code long}s or {@code int}s.
   * @param parser a JsonParser whose current token is a value from the JSON response. Callers
   *     must ensure that the parser is ready to consume a value token. This method does not change
   *     the state of the parser.
   * @return the parsed value.
   */
  static Object deserializeValue(JsonParser parser, ColumnMetaData columnMetaData)
      throws IOException {
    switch (columnMetaData.type.rep) {
    case PRIMITIVE_BOOLEAN:
    case BOOLEAN:
      return parser.getBooleanValue();
    case PRIMITIVE_BYTE:
    case BYTE:
      return parser.getByteValue();
    case STRING:
      return parser.getValueAsString();
    case PRIMITIVE_SHORT:
    case SHORT:
      return parser.getShortValue();
    case PRIMITIVE_INT:
    case INTEGER:
      return parser.getIntValue();
    case PRIMITIVE_LONG:
    case LONG:
      return parser.getLongValue();
    case PRIMITIVE_FLOAT:
    case FLOAT:
      return parser.getFloatValue();
    case PRIMITIVE_DOUBLE:
    case DOUBLE:
      return parser.getDoubleValue();
    case NUMBER:
      // NUMBER is represented as BigDecimal
      return parser.getDecimalValue();
    // TODO: MEASURE types are appearing as Objects. This should have been solved by CALCITE-5549.
    //  Be sure that the signature of a prepared query matches the metadata we see from JDBC.
    case OBJECT:
      switch (columnMetaData.type.id) {
      case Types.INTEGER:
        return parser.getIntValue();
      case Types.BIGINT:
        return parser.getBigIntegerValue();
      case Types.DOUBLE:
        return parser.getDoubleValue();
      case Types.DECIMAL:
      case Types.NUMERIC:
        return parser.getDecimalValue();
      }
    default:
      throw new IOException("Unable to parse " + columnMetaData.type.rep + " from stream!");
    }
  }

  @Override
  public Frame fetch(final StatementHandle h, final long offset, final int fetchMaxRowCount)
      throws NoSuchStatementException, MissingResultsException {
    // If this statement was initiated as a LookerFrame then it will have an entry in the queue map
    if (stmtQueueMap.containsKey(h)) {
      try {
        BlockingQueue queue = stmtQueueMap.get(h);

        // `take` blocks until there is an entry in the queue
        FrameEnvelope nextEnvelope = (FrameEnvelope) queue.take();

        // remove the statement from the map if it has an exception, or it is the last frame
        if (nextEnvelope.hasException()) {
          stmtQueueMap.remove(h);
          throw new RuntimeException(nextEnvelope.exception);
        } else if (nextEnvelope.frame.done) {
          stmtQueueMap.remove(h);
        }
        return nextEnvelope.frame;
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    // not a streaming query - default to RemoteMeta behavior
    return super.fetch(h, offset, fetchMaxRowCount);
  }

  /**
   * An initially empty frame specific to Looker result sets. {@link #sqlInterfaceQueryId} is used
   * to begin a streaming query.
   */
  static class LookerFrame extends Frame {

    /**
     * A unique ID for the current SQL statement to run. Prepared and set during
     * {@link LookerRemoteService#apply(PrepareAndExecuteRequest)} or
     * {@link LookerRemoteService#apply(PrepareRequest)}. This is distinct from a statement ID.
     * Multiple statements may execute the same query ID.
     */
    public final Long sqlInterfaceQueryId;

    private LookerFrame(long offset, boolean done, Iterable<Object> rows, Long statementId) {
      super(offset, done, rows);
      this.sqlInterfaceQueryId = statementId;
    }

    /**
     * Creates a {@code LookerFrame} for the statement slug
     *
     * @param sqlInterfaceQueryId id for the prepared statement generated by a Looker instance.
     * @return the {@code firstFrame} for the result set.
     */
    public static final LookerFrame create(Long sqlInterfaceQueryId) {
      return new LookerFrame(0, false, Collections.emptyList(), sqlInterfaceQueryId);
    }
  }

  /**
   * Helper to disable any SSL and hostname verification when `verifySSL` is false.
   */
  private void trustAllHosts(HttpsURLConnection connection) {
    // Create a trust manager that does not validate certificate chains
    TrustManager[] trustAllCerts = new TrustManager[]{
        new X509TrustManager() {
          public java.security.cert.X509Certificate[] getAcceptedIssuers() {
            return null;
          }

          public void checkClientTrusted(java.security.cert.X509Certificate[] certs,
              String authType) {
          }

          public void checkServerTrusted(java.security.cert.X509Certificate[] certs,
              String authType) {
          }
        }
    };
    // Create all-trusting host name verifier
    HostnameVerifier trustAllHostNames = (hostname, session) -> true;
    try {
      SSLContext sc = SSLContext.getInstance("SSL");
      sc.init(null, trustAllCerts, new java.security.SecureRandom());
      connection.setSSLSocketFactory(sc.getSocketFactory());
      connection.setHostnameVerifier(trustAllHostNames);
    } catch (NoSuchAlgorithmException | KeyManagementException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * A regrettable necessity. The Kotlin SDK relies on an outdated Ktor HTTP client based on Kotlin
   * Coroutines which are difficult to work with in Java. Here we make a HTTP client to handle the
   * request and input stream ourselves. This adds complexity that would normally be handled by the
   * SDK. We should revisit this once the SDK has built-in streams.
   *
   * TODO https://github.com/looker-open-source/sdk-codegen/issues/1341:
   *  Add streaming support to the Kotlin SDK.
   */
  private InputStream makeRunQueryRequest(String url) throws IOException {
    AuthSession authSession = getSdk().getAuthSession();
    Transport sdkTransport = authSession.getTransport();

    // makes a proper URL from the API endpoint path as the SDK would.
    String endpoint = sdkTransport.makeUrl(url, Collections.emptyMap(), null);
    URL httpsUrl = new URL(endpoint);
    HttpsURLConnection connection = (HttpsURLConnection) httpsUrl.openConnection();

    // WARNING: You should only set `verifySSL=false` for local/dev instances!!
    if (!sdkTransport.getOptions().getVerifySSL()) {
      trustAllHosts(connection);
    }

    // timeout is given as seconds
    int timeout = sdkTransport.getOptions().getTimeout() * 1000;
    connection.setReadTimeout(timeout);
    connection.setRequestMethod("GET");
    connection.setRequestProperty("Accept", "application/json");
    connection.setDoOutput(true);

    // Set the auth header as the SDK would
    connection.setRequestProperty("Authorization",
        "token " + authSession.getAuthToken().getAccessToken());

    int responseCode = connection.getResponseCode();
    if (responseCode == 200) {
      // return the input stream to parse from.
      return connection.getInputStream();
    } else {
      throw new IOException("HTTP request failed with status code: " + responseCode);
    }
  }

  private void seekToRows(JsonParser parser) throws IOException {
    while (parser.nextToken() != null && !ROWS_KEY.equals(parser.currentName())) {
      // move position to start of `rows`
    }
  }

  private void seekToValue(JsonParser parser) throws IOException {
    while (parser.nextToken() != null && !VALUE_KEY.equals(parser.currentName())) {
      // seeking to `value` key for the field e.g. `"rows": [{"field_1": {"value": 123 }}]
    }
    // now move to the actual value
    parser.nextToken();
  }

  private void putExceptionOrFail(BlockingQueue queue, Exception e) {
    try {
      // `put` blocks until there is room on the queue but needs a catch
      queue.put(makeException(e));
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Prepares a thread to stream a query response into a series of {@link FrameEnvelope}s.
   */
  private Thread prepareStreamingThread(String baseUrl, Signature signature, int fetchSize,
      BlockingQueue frameQueue) {
    Thread stream = new Thread(() -> {
      try {
        InputStream in = makeRunQueryRequest(baseUrl);
        JsonParser parser = new JsonFactory().createParser(in);

        int currentOffset = 0;

        while (parser.nextToken() != null) {
          if (currentOffset == 0) {
            // TODO: Handle `metadata`. We are currently ignoring it and seeking to `rows` array
            seekToRows(parser);
          }

          int rowsRead = 0;
          List<Object> rows = new ArrayList<>();

          while (rowsRead < fetchSize) {
            List<Object> columnValues = new ArrayList<>();
            // the signature should _always_ have the correct number of columns.
            // if not, something went wrong during query preparation on the Looker instance.
            for (int i = 0; i < signature.columns.size(); i++) {
              seekToValue(parser);

              if (parser.isClosed()) {
                // the stream is closed - all rows should be accounted for
                currentOffset += rowsRead;
                frameQueue.put(makeFrame(currentOffset, /*done=*/true, rows));
                return;
              }

              // add the value to the column list
              columnValues.add(deserializeValue(parser, signature.columns.get(i)));
            }

            // Meta.CursorFactory#deduce will select an OBJECT cursor if there is only a single
            // column in the signature. This is intended behavior. Since the rows of a frame are
            // simply `Object` - we could get illegal casts from an ArrayList to the underlying Rep
            // type if we don't discard the wrapping ArrayList for the single value here.
            Object row = signature.columns.size() == 1 ? columnValues.get(0) : columnValues;

            rows.add(row);
            rowsRead++;
          }
          // we fetched the allowed number of rows so add the complete frame to the queue
          currentOffset += rowsRead;
          frameQueue.put(makeFrame(currentOffset, /*done=*/false, rows));
        }
      } catch (Exception e) {
        // enqueue all exceptions for the main thread to report
        putExceptionOrFail(frameQueue, e);
      }
    });
    return stream;
  }

  /**
   * Creates a streaming iterable that parses a JSON {@link InputStream} into a series of
   * {@link FrameEnvelope}s.
   */
  @Override
  public Iterable<Object> createIterable(StatementHandle h, QueryState state, Signature signature,
      List<TypedValue> parameters, Frame firstFrame) {
    // If this a LookerFrame, then we must be targeting the sql_interface APIs
    if (LookerFrame.class.isAssignableFrom(firstFrame.getClass())) {
      try {
        // generate the endpoint URL to begin the request
        LookerFrame lookerFrame = (LookerFrame) firstFrame;
        String url = LookerSdkFactory.queryEndpoint(lookerFrame.sqlInterfaceQueryId);

        // grab the statement
        AvaticaStatement stmt = connection.statementMap.get(h.id);
        if (null == stmt) {
          throw new NoSuchStatementException(h);
        }

        // setup queue to place complete frames
        BlockingQueue frameQueue = new ArrayBlockingQueue<FrameEnvelope>(DEFAULT_FRAME_QUEUE_SIZE);

        // update map so this statement is associated with a queue
        stmtQueueMap.put(stmt.handle, frameQueue);

        // init and start a new thread to stream from a Looker instance and populate the frameQueue
        prepareStreamingThread(url, signature, stmt.getFetchSize(), frameQueue).start();
      } catch (SQLException | NoSuchStatementException e) {
        throw new RuntimeException(e);
      }
    }
    // always return a FetchIterable - we'll check in LookerRemoteMeta#fetch for any enqueued Frames
    return super.createIterable(h, state, signature, parameters, firstFrame);
  }
}
