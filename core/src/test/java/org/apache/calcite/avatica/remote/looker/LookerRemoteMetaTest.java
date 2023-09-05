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


import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.StructImpl;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Test for Looker specific functionality in {@link LookerRemoteMeta} implementations.
 */
public class LookerRemoteMetaTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static Map<Rep, Object> supportedRepValues;
  private static Map<Rep, Object> unsupportedRepValues;

  private static ObjectMapper mapper = new ObjectMapper();

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
    supportedRepValues = new HashMap(buildingMap);
    buildingMap.clear();

    // Unsupported datetime types
    buildingMap.put(Rep.JAVA_SQL_TIME, new Time(1000000));
    buildingMap.put(Rep.JAVA_SQL_DATE, new Date(1000000));
    buildingMap.put(Rep.JAVA_SQL_TIMESTAMP, new Timestamp(100000));
    buildingMap.put(Rep.JAVA_UTIL_DATE, new java.util.Date(1000000));
    // Unsupported object types
    buildingMap.put(Rep.ARRAY, new Array[]{});
    buildingMap.put(Rep.BYTE_STRING, new ByteString(new byte[] {'h', 'e', 'l', 'l', 'o'}));
    buildingMap.put(Rep.PRIMITIVE_CHAR, 'c');
    buildingMap.put(Rep.CHARACTER, new Character('c'));
    buildingMap.put(Rep.MULTISET, new ArrayList());
    buildingMap.put(Rep.STRUCT, new StructImpl(new ArrayList()));
    buildingMap.put(Rep.OBJECT, new Object());
    unsupportedRepValues = new HashMap(buildingMap);
    buildingMap.clear();
  }

  private JsonParser makeTestParserFromRep(Rep rep, Object value) {
    String template = "{ \"value\": %s }";
    try {
      String valAsJson = mapper.writeValueAsString(value);
      String testInput = String.format(Locale.ROOT, template, valAsJson);
      InputStream in = new ByteArrayInputStream(testInput.getBytes(Charset.defaultCharset()));
      JsonParser jp = new JsonFactory().createParser(in);
      jp.nextFieldName(); // move to "value:" key
      jp.nextValue(); // move to value itself
      return jp;
    } catch (IOException e) {
      fail(e.getMessage());
    }
    return null;
  }

  @Test
  public void deserializeValueTestingIsExhaustive() {
    HashMap allMap = new HashMap();
    allMap.putAll(supportedRepValues);
    allMap.putAll(unsupportedRepValues);
    Arrays.stream(Rep.values()).forEach(val -> {
      assertNotNull(allMap.get(val));
    });
  }

  @Test
  public void deserializeValueThrowsErrorOnUnsupportedType() {
    unsupportedRepValues.forEach((rep, value) -> {
      JsonParser parser = makeTestParserFromRep(rep, value);
      thrown.expect(RuntimeException.class);
      thrown.expectMessage("Unable to parse " + rep.name() + " from stream!");
      try {
        LookerRemoteMeta.deserializeValue(rep, parser);
      } catch (IOException e) {
        fail();
      }
    });
  }

  @Test
  public void deserializeValueWorksForSupportedTypes() {
    supportedRepValues.forEach((rep, value) -> {
      JsonParser parser = makeTestParserFromRep(rep, value);
      try {
        assertThat(value, is(equalTo(LookerRemoteMeta.deserializeValue(rep, parser))));
      } catch (IOException e) {
        fail(e.getMessage());
      }
    });
  }

  @Ignore
  @Test
  public void testIt() throws SQLException, InterruptedException, IOException {
    Connection connection = DriverManager.getConnection(LookerTestCommon.getUrl(),
        LookerTestCommon.getBaseProps());
    ResultSet models = connection.getMetaData().getSchemas();
    while (models.next()) {
      System.out.println(models.getObject(1));
    }

    String sql =
        "SELECT `orders.status`"
            + "FROM `thelook`.`order_items`";
    ResultSet test = connection.createStatement().executeQuery(sql);
    int i = 0;
    PrintWriter writer = new PrintWriter("the-file-name.txt", "UTF-8");
    while (test.next()) {
      i++;
      writer.println(i + ": " + test.getObject(1));
    }
    writer.close();
    System.out.println("END !!!!!");
  }
}
