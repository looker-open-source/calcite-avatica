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
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.AvaticaType;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.avatica.ConnectStringParser;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.remote.JsonService;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.remote.looker.LookerRemoteMeta.LookerFrame;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.StructImpl;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Test for Looker specific functionality in {@link LookerRemoteMeta} implementations.
 */
public class LookerRemoteMetaTest {

  private static Map<Rep, Object> supportedRepValues;
  private static Map<Rep, Object> unsupportedRepValues;

  private static ObjectMapper mapper = new ObjectMapper();

  /**
   * A testable Looker driver without requiring access to a Looker instance.
   *
   * {@link #withStubbedResponse} must be called before creating a connection.
   */
  public class StubbedLookerDriver extends Driver {

    String stubbedSignature;
    String stubbedResponse;

    /**
     * Sets stubbed responses for the test. The signature must match the stubbed response.
     *
     * @param signature {@link Signature} as a JSON string.
     * @param response a JSON response identical to one returned by a Looker API call to
     *     {@code GET /sql_interface_queries/:id/run/json_bi}.
     * @return the driver with a stubbed {@link Service} and {@link Meta}.
     */
    public Driver withStubbedResponse(String signature, String response) {
      this.stubbedSignature = signature;
      this.stubbedResponse = response;

      return this;
    }

    @Override
    public Meta createMeta(AvaticaConnection connection) {
      assertNotNull(stubbedSignature);
      assertNotNull(stubbedResponse);

      final Service service = new StubbedLookerRemoteService(stubbedSignature);
      connection.setService(service);

      return new StubbedLookerRemoteMeta(connection, service, stubbedResponse);
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
      if (!acceptsURL(url)) {
        return null;
      }

      final String prefix = getConnectStringPrefix();
      assert url.startsWith(prefix);
      final String urlSuffix = url.substring(prefix.length());
      final Properties info2 = ConnectStringParser.parse(urlSuffix, info);
      final AvaticaConnection connection = factory.newConnection(this, factory, url, info2);
      handler.onConnectionInit(connection);

      return connection;
    }
  }

  private class StubbedLookerRemoteMeta extends LookerRemoteMeta {

    String stubbedResponse;

    StubbedLookerRemoteMeta(AvaticaConnection connection, Service service,
        String testResponse) {
      super(connection, service);
      this.stubbedResponse = testResponse;
    }

    @Override
    protected InputStream makeRunQueryRequest(String url) {
      return new ByteArrayInputStream(stubbedResponse.getBytes(StandardCharsets.UTF_8));
    }
  }

  private class StubbedLookerRemoteService extends LookerRemoteService {

    private String stubbedSignature;

    StubbedLookerRemoteService(String signature) {
      super();
      this.stubbedSignature = signature;
    }

    @Override
    public ConnectionSyncResponse apply(ConnectionSyncRequest request) {
      try {
        // value does not matter for this stub class but needed by the connection
        return decode("{\"response\": \"connectionSync\"}", ConnectionSyncResponse.class);
      } catch (IOException e) {
        throw handle(e);
      }
    }

    @Override
    public CreateStatementResponse apply(CreateStatementRequest request) {
      try {
        // value does not matter for this stub class but needed by the connection
        return decode("{\"response\": \"createStatement\"}", CreateStatementResponse.class);
      } catch (IOException e) {
        throw handle(e);
      }
    }

    @Override
    public ExecuteResponse apply(PrepareAndExecuteRequest request) {
      try {
        Signature signature = JsonService.MAPPER.readValue(stubbedSignature, Signature.class);
        return lookerExecuteResponse(request, signature, LookerFrame.create(1L));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static {
    Map buildingMap = new HashMap<ColumnMetaData.Rep, Object>();
    // Primitive types
    buildingMap.put(Rep.PRIMITIVE_BOOLEAN, true);
    buildingMap.put(Rep.PRIMITIVE_BYTE, (byte) 10);
    buildingMap.put(Rep.PRIMITIVE_SHORT, (short) 5);
    buildingMap.put(Rep.PRIMITIVE_INT, 100);
    buildingMap.put(Rep.PRIMITIVE_LONG, (long) 10000);
    buildingMap.put(Rep.PRIMITIVE_FLOAT, (float) 1.99);
    buildingMap.put(Rep.PRIMITIVE_DOUBLE, 1.99);
    // Non-Primitive types
    buildingMap.put(Rep.BOOLEAN, true);
    buildingMap.put(Rep.BYTE, new Byte((byte) 10));
    buildingMap.put(Rep.SHORT, new Short((short) 10));
    buildingMap.put(Rep.INTEGER, new Integer(100));
    buildingMap.put(Rep.LONG, new Long(10000));
    buildingMap.put(Rep.FLOAT, new Float(1.99));
    buildingMap.put(Rep.DOUBLE, new Double(1.99));
    buildingMap.put(Rep.STRING, "hello");
    buildingMap.put(Rep.NUMBER, new BigDecimal(1000000));
    // TODO: We shouldn't need to support OBJECT but MEASUREs are appearing as generic objects in
    //  the signature
    buildingMap.put(Rep.OBJECT, 1000);
    supportedRepValues = new HashMap(buildingMap);
    buildingMap.clear();

    // Unsupported datetime types
    buildingMap.put(Rep.JAVA_SQL_TIME, new Time(1000000));
    buildingMap.put(Rep.JAVA_SQL_DATE, new Date(1000000));
    buildingMap.put(Rep.JAVA_SQL_TIMESTAMP, new Timestamp(100000));
    buildingMap.put(Rep.JAVA_UTIL_DATE, new java.util.Date(1000000));
    // Unsupported object types
    buildingMap.put(Rep.ARRAY, new Array[]{});
    buildingMap.put(Rep.BYTE_STRING, new ByteString(new byte[]{'h', 'e', 'l', 'l', 'o'}));
    buildingMap.put(Rep.PRIMITIVE_CHAR, 'c');
    buildingMap.put(Rep.CHARACTER, new Character('c'));
    buildingMap.put(Rep.MULTISET, new ArrayList());
    buildingMap.put(Rep.STRUCT, new StructImpl(new ArrayList()));
    unsupportedRepValues = new HashMap(buildingMap);
    buildingMap.clear();
  }

  private JsonParser makeTestParserFromValue(Object value) throws IOException {
    String template = "{ \"value\": %s }";
    try {
      String valAsJson = mapper.writeValueAsString(value);
      String testInput = String.format(Locale.ROOT, template, valAsJson);

      // start input stream and move to `value`
      InputStream in = new ByteArrayInputStream(testInput.getBytes(Charset.defaultCharset()));
      JsonParser jp = new JsonFactory().createParser(in);
      jp.nextFieldName(); // move to "value:" key
      jp.nextValue(); // move to value itself

      return jp;
    } catch (IOException e) {
      throw e;
    }
  }

  private ColumnMetaData makeDummyMetadata(Rep rep) {
    // MEASUREs appear as Objects but typeId is the underlying data type (usually int or double)
    // See relevant TODO in LookerRemoteMeta#deserializeValue
    int typeId = rep == Rep.OBJECT ? 4 : rep.typeId;
    AvaticaType type = new AvaticaType(typeId, rep.name(), rep);
    return ColumnMetaData.dummy(type, false);
  }

  @Test
  public void deserializeValueTestingIsExhaustive() {
    HashMap allMap = new HashMap();
    allMap.putAll(supportedRepValues);
    allMap.putAll(unsupportedRepValues);

    Arrays.stream(Rep.values()).forEach(val -> assertNotNull(allMap.get(val)));
  }

  @Test
  public void deserializeValueThrowsErrorOnUnsupportedType() {
    unsupportedRepValues.forEach((rep, value) -> {
      try {
        JsonParser parser = makeTestParserFromValue(value);

        // should throw an IOException
        LookerRemoteMeta.deserializeValue(parser, makeDummyMetadata(rep));
        fail("Should have thrown an IOException!");

      } catch (IOException e) {
        assertThat(e.getMessage(), is("Unable to parse " + rep.name() + " from stream!"));
      }
    });
  }

  @Test
  public void deserializeValueWorksForSupportedTypes() {
    supportedRepValues.forEach((rep, value) -> {
      try {
        JsonParser parser = makeTestParserFromValue(value);
        Object deserializedValue = LookerRemoteMeta.deserializeValue(parser,
            makeDummyMetadata(rep));

        assertThat(value, is(equalTo(deserializedValue)));
      } catch (IOException e) {
        fail(e.getMessage());
      }
    });
  }

  @Test
  public void resultsCanBeParsedIntoResultSet() throws SQLException {
    // Set up the driver with some pre-recorded responses from Looker. These are large JSON strings
    // so have been placed in the LookerTestCommon class to make this test easier to read.
    Driver driver = new StubbedLookerDriver().withStubbedResponse(
        LookerTestCommon.stubbedSignature, LookerTestCommon.stubbedJsonResults);
    Connection connection = driver.connect(LookerTestCommon.getUrl(),
        LookerTestCommon.getBaseProps());

    // stubbed JSON response is based off of this query
    String sql = "SELECT "
        + "`users.created_date`, `users.created_year`, `users.created_time`, `users.name`\n"
        + "`users.age`, `users.is45or30`, SUM(`users.age`), AGGREGATE(`users.average_age`)\n"
        + "FROM example.users\n" + "GROUP BY 1, 2, 3, 4, 5, 6";
    ResultSet test = connection.createStatement().executeQuery(sql);
    ResultSetMetaData rsMetaData = test.getMetaData();

    // verify column types
    assertThat(rsMetaData.getColumnCount(), is(8));

    // DATE
    assertThat(rsMetaData.getColumnType(1), is(Types.DATE));
    assertThat(rsMetaData.getColumnName(1), is("users.created_date"));

    // YEAR
    assertThat(rsMetaData.getColumnType(2), is(Types.INTEGER));
    assertThat(rsMetaData.getColumnName(2), is("users.created_year"));

    // TIMESTAMP
    assertThat(rsMetaData.getColumnType(3), is(Types.TIMESTAMP));
    assertThat(rsMetaData.getColumnName(3), is("users.created_time"));

    // STRING
    assertThat(rsMetaData.getColumnType(4), is(Types.VARCHAR));
    assertThat(rsMetaData.getColumnName(4), is("users.name"));

    // DOUBLE dimension
    assertThat(rsMetaData.getColumnType(5), is(Types.DOUBLE));
    assertThat(rsMetaData.getColumnName(5), is("users.age"));

    // BOOLEAN
    assertThat(rsMetaData.getColumnType(6), is(Types.BOOLEAN));
    assertThat(rsMetaData.getColumnName(6), is("users.is45or30"));

    // DOUBLE custom measure
    assertThat(rsMetaData.getColumnType(7), is(Types.DOUBLE));
    assertThat(rsMetaData.getColumnName(7), is("EXPR$6"));

    // DOUBLE LookML measure
    assertThat(rsMetaData.getColumnType(8), is(Types.DOUBLE));
    // TODO: investigate why measures are not being aliased as their LookML field name
    assertThat(rsMetaData.getColumnName(8), is("EXPR$7"));

    // verify every row can be fetched with the appropriate getter method
    while (test.next()) {
      assertNotNull(test.getDate(1));
      assertNotNull(test.getInt(2));
      assertNotNull(test.getTimestamp(3));
      assertNotNull(test.getString(4));
      assertNotNull(test.getDouble(5));
      assertNotNull(test.getBoolean(6));
      assertNotNull(test.getDouble(7));
      assertNotNull(test.getDouble(8));
    }
  }
}
