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
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.RemoteMeta;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.remote.Service.PrepareAndExecuteRequest;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.avatica.remote.looker.utils.LookerSdkFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.looker.rtl.AuthSession;
import com.looker.rtl.Transport;
import com.looker.sdk.LookerSDK;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import static org.apache.calcite.avatica.remote.looker.utils.LookerSdkFactory.DEFAULT_STREAM_BUFFER_SIZE;

/**
 * Implementation of Meta that works in tandem with {@link LookerRemoteService} to stream results
 * from the Looker SDK.
 */
public class LookerRemoteMeta extends RemoteMeta implements Meta {

  /** Authenticated LookerSDK lazily set from LookerRemoteService. */
  LookerSDK sdk;

  public LookerRemoteMeta(AvaticaConnection connection, Service service) {
    super(connection, service);
    // this class _must_ be backed by a LookerRemoteService
    assert service.getClass() == LookerRemoteService.class;
  }

  /**
   * Calls the correct method to read the current value on the stream. The {@code get} methods do
   * not advance the current token, so they can be called multiple times without changing the state
   * of the parser.
   *
   * @param columnTypeRep the internal Avatica representation for this value. It is important to
   *     use the {@link Rep} rather than the type name since Avatica represents most datetime values
   *     as milliseconds since epoch via {@code long}s or {@code int}s.
   * @param parser a JsonParser whose current token is a value from the JSON response. Callers
   *     must ensure that the parser is ready to consume a value token. This method does not change
   *     the state of the parser.
   * @return the parsed value.
   */
  static Object deserializeValue(Rep columnTypeRep, JsonParser parser) throws IOException {
    switch (columnTypeRep) {
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
    default:
      throw new RuntimeException("Unable to parse " + columnTypeRep + " from stream!");
    }
  }

  /**
   * An initially empty frame specific to Looker result sets. The {@code statementSlug} is used to
   * begin a streaming query.
   */
  static class LookerFrame extends Frame {

    /**
     * A unique ID for the current SQL statement to run. Prepared and set during
     * {@link LookerRemoteService#apply(PrepareAndExecuteRequest)}.
     */
    public final Long statementId;

    private LookerFrame(long offset, boolean done, Iterable<Object> rows, Long statementId) {
      super(offset, done, rows);
      this.statementId = statementId;
    }

    /**
     * Creates a {@code LookerFrame} for the statement slug
     *
     * @param statementId id for the prepared statement generated by a Looker instance.
     * @return the {@code firstFrame} for the result set.
     */
    public static final LookerFrame create(Long statementId) {
      return new LookerFrame(0, false, Collections.emptyList(), statementId);
    }
  }

  LookerSDK getSdk() {
    if (null == sdk) {
      sdk = ((LookerRemoteService) service).sdk;
    }
    return sdk;
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
   * Coroutines which are a tad difficult to work with in Java. Here we make a HTTP client to handle
   * the response stream ourselves which is "good enough" for now. This method should be revisited
   * when the Kotlin SDK has built-in streams.
   *
   * TODO https://github.com/looker-open-source/sdk-codegen/issues/1341:
   *  Add streaming support to the Kotlin SDK.
   */
  private void streamResponse(String url, OutputStream outputStream) throws IOException {
    // use some SDK client helpers to tighten up this call
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
      // grab the input stream and write to the output for the main thread to consume.
      InputStream inputStream = connection.getInputStream();
      int bytesRead;
      byte[] buffer = new byte[DEFAULT_STREAM_BUFFER_SIZE];
      while ((bytesRead = inputStream.read(buffer)) != -1) {
        outputStream.write(buffer, 0, bytesRead);
      }
      outputStream.close();
      inputStream.close();
    } else {
      throw new IOException("HTTP request failed with status code: " + responseCode);
    }
  }

  /**
   * Prepares a thread to stream query response into an OutputStream. Sets an exception handler for
   * the thread for reporting.
   */
  private Thread streamingThread(String baseUrl, InputStream in, OutputStream out) {
    Thread stream = new Thread(() -> {
      try {
        streamResponse(baseUrl, out);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    Thread.UncaughtExceptionHandler exceptionHandler = (th, ex) -> {
      try {
        in.close();
        out.close();
        throw new RuntimeException(ex);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };
    stream.setUncaughtExceptionHandler(exceptionHandler);
    return stream;
  }

  /**
   * Creates a streaming iterable that parses a JSON {@link InputStream} into a series of
   * {@link Frame}s.
   */
  @Override
  public Iterable<Object> createIterable(StatementHandle h, QueryState state, Signature signature,
      List<TypedValue> parameters, Frame firstFrame) {
    if (LookerFrame.class.isAssignableFrom(firstFrame.getClass())) {
      try {
        LookerFrame lookerFrame = (LookerFrame) firstFrame;
        String endpoint = LookerSdkFactory.queryEndpoint(lookerFrame.statementId);
        // set up in/out streams
        PipedOutputStream out = new PipedOutputStream();
        PipedInputStream in = new PipedInputStream(out);
        // grab the statement
        AvaticaStatement stmt = connection.lookupStatement(h);
        // init a new thread to stream from a Looker instance
        Thread stream = streamingThread(endpoint, in, out);
        stream.start();
        // TODO bug in Avatica - the statement handle never has the signature updated
        //  workaround by handing the signature to the iterable directly.
        return new LookerIterable(stmt, state, firstFrame, in, signature);
      } catch (IOException | SQLException e) {
        throw new RuntimeException(e);
      }
    }
    // if this is a normal Frame from Avatica no special treatment is needed.
    return super.createIterable(h, state, signature, parameters, firstFrame);
  }

  /**
   * Iterable that yields an iterator that parses an {@link InputStream} of JSON into a sequence of
   * {@link Meta.Frame}s.
   */
  public class LookerIterable extends FetchIterable implements Iterable<Object> {

    static final String ROWS_KEY = "rows";
    static final String VALUE_KEY = "value";

    final AvaticaStatement statement;
    final QueryState state;
    final Frame firstFrame;
    final JsonParser parser;
    final Signature signature;

    LookerIterable(AvaticaStatement statement, QueryState state, Frame firstFrame, InputStream in,
        Signature signature) {
      super(statement, state, firstFrame);
      assert null != in;
      this.statement = statement;
      this.state = state;
      this.firstFrame = firstFrame;
      this.signature = signature;
      try {
        // Create a streaming parser based on the provided InputStream
        this.parser = new JsonFactory().createParser(in);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Iterator<Object> iterator() {
      return new LookerIterator(statement, state, firstFrame);
    }

    /**
     * Iterator that parses an {@link InputStream} of JSON into a sequence of {@link Meta.Frame}s.
     */
    public class LookerIterator extends FetchIterator implements Iterator<Object> {

      LookerIterator(AvaticaStatement stmt, QueryState state, Frame firstFrame) {
        super(stmt, state, firstFrame);
      }

      private void seekToRows() throws IOException {
        while (parser.nextToken() != null && !ROWS_KEY.equals(parser.currentName())) {
          // move position to start of `rows`
        }
      }

      private void seekToValue() throws IOException {
        while (parser.nextToken() != null && !VALUE_KEY.equals(parser.currentName())) {
          // seeking to `value` key for the field e.g. `"rows": [{"field_1": {"value": 123 }}]
        }
        // now move to the actual value
        parser.nextToken();
      }

      @Override
      public Frame doFetch(StatementHandle h, long currentOffset, int fetchSize) {
        try {
          if (currentOffset == 0) {
            // TODO: Handle `metadata`. We are currently ignoring it and seeking to `rows` array
            seekToRows();
          }
          int rowsRead = 0;
          List<Object> rows = new ArrayList<>();
          // only read the number of rows requested by the connection config `fetchSize`
          while (rowsRead < fetchSize) {
            // TODO: could probably be optimized
            List<Object> columns = new ArrayList<>();
            // the signature should _always_ have the correct number of columns.
            // if not, something went wrong during query preparation on the Looker instance.
            for (int i = 0; i < signature.columns.size(); i++) {
              seekToValue();
              if (parser.isClosed()) {
                // the stream is closed - all rows should be accounted for
                return new Frame(currentOffset + rowsRead, true, rows);
              }
              // add the value to the column list
              Object value = LookerRemoteMeta.deserializeValue(signature.columns.get(i).type.rep,
                  parser);
              columns.add(value);
            }
            rows.add(columns);
            rowsRead++;
          }
          // we fetched the allowed number of rows so return the frame with the current batch
          return new Frame(currentOffset + rowsRead, false, rows);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
