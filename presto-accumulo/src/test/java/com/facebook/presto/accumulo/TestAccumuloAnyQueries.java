/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.accumulo;

import com.facebook.presto.Session;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.QueryAssertions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.annotations.Test;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.presto.accumulo.AccumuloQueryRunner.createAccumuloQueryRunner;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.util.DateTimeZoneIndex.getDateTimeZone;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestAccumuloAnyQueries
{
    private static final DateTimeFormatter DATE_TIME_FORMATTER = ISODateTimeFormat.date();
    private static final Date DATE1 = new Date(new DateTime(2016, 5, 25, 0, 0, DateTimeZone.UTC).getMillis());
    private static final Date DATE2 = new Date(new DateTime(2016, 5, 26, 0, 0, DateTimeZone.UTC).getMillis());
    private static final Date DATE3 = new Date(new DateTime(2016, 5, 27, 0, 0, DateTimeZone.UTC).getMillis());

    private static final TimeZoneKey TIME_ZONE_KEY = getTimeZoneKey("Europe/Berlin");
    private static final DateTimeZone DATE_TIME_ZONE = getDateTimeZone(TIME_ZONE_KEY);
    private static final Timestamp TIMESTAMP1 = new Timestamp(new DateTime(2016, 5, 25, 3, 2, 3, 123, DATE_TIME_ZONE).getMillis());
    private static final Timestamp TIMESTAMP2 = new Timestamp(new DateTime(2016, 5, 26, 6, 5, 6, 456, DATE_TIME_ZONE).getMillis());
    private static final Timestamp TIMESTAMP3 = new Timestamp(new DateTime(2016, 5, 27, 9, 8, 9, 789, DATE_TIME_ZONE).getMillis());

    private static final Time TIME1 = new Time(new DateTime(1970, 1, 1, 2, 2, 3, 123, DATE_TIME_ZONE).getMillis());
    private static final Time TIME2 = new Time(new DateTime(1970, 1, 1, 5, 5, 6, 456, DATE_TIME_ZONE).getMillis());
    private static final Time TIME3 = new Time(new DateTime(1970, 1, 1, 8, 8, 9, 789, DATE_TIME_ZONE).getMillis());
    private QueryRunner runner;
    private Session session;

    public TestAccumuloAnyQueries()
            throws Exception
    {
        runner = createAccumuloQueryRunner(ImmutableMap.of());
        session = runner.getDefaultSession();
    }

    protected void assertCreate(String sql)
    {
        MaterializedResult results = runner.execute(sql);
        Optional<String> type = results.getUpdateType();
        assertEquals(1, results.getMaterializedRows().size(), "CREATE TABLE contains more than one result");
        assertTrue((Boolean) results.getMaterializedRows().get(0).getField(0), "Failed to create table");
    }

    protected void assertInsert(String sql, long count)
    {
        QueryAssertions.assertUpdate(runner, session, sql, OptionalLong.of(count));
    }

    protected void assertSelect(String sql, List<Object[]> row)
    {
        MaterializedResult actual = runner.execute(sql);

        MaterializedResult.Builder bldr = MaterializedResult.resultBuilder(session);
        for (Object[] values : row) {
            bldr.row(values);
        }
        MaterializedResult expected = bldr.build();

        assertEquals(actual, expected);
    }

    @Test
    public void testBigint()
            throws Exception
    {
        try {
            assertCreate("CREATE TABLE test_bigint (a integer, b bigint) WITH (index_columns='b')");
            assertInsert("INSERT INTO test_bigint VALUES (1, 123), (2, 456)", 2);
            assertSelect("SELECT * FROM test_bigint WHERE b ANY ARRAY[BIGINT '123', BIGINT '456']",
                    ImmutableList.of(new Object[] {1, 123L}, new Object[] {2, 456L}));
        }
        finally {
            dropTable("test_bigint");
        }
    }

    @Test
    public void testBigintArray()
            throws Exception
    {
        try {
            assertCreate("CREATE TABLE test_bigint_array (a integer, b array(bigint)) WITH (index_columns='b')");
            assertInsert("INSERT INTO test_bigint_array VALUES (1, ARRAY[BIGINT '123', BIGINT '456']), (2, ARRAY[BIGINT '456', BIGINT '789'])", 2);
            assertSelect("SELECT * FROM test_bigint_array WHERE BIGINT '456' ANY b",
                    ImmutableList.of(new Object[] {1, ImmutableList.of(123L, 456L)}, new Object[] {2, ImmutableList.of(456L, 789L)}));
        }
        finally {
            dropTable("test_bigint_array");
        }
    }

    @Test
    public void testBoolean()
            throws Exception
    {
        try {
            assertCreate("CREATE TABLE test_boolean (a integer, b boolean) WITH (index_columns='b')");
            assertInsert("INSERT INTO test_boolean VALUES (1, true), (2, false)", 2);
            assertSelect("SELECT * FROM test_boolean WHERE b ANY ARRAY[true, false]",
                    ImmutableList.of(new Object[] {1, true}, new Object[] {2, false}));
        }
        finally {
            dropTable("test_boolean");
        }
    }

    @Test
    public void testBooleanArray()
            throws Exception
    {
        try {
            assertCreate("CREATE TABLE test_boolean_array (a integer, b array(boolean)) WITH (index_columns='b')");
            assertInsert("INSERT INTO test_boolean_array VALUES (1, ARRAY[true, false]), (2, ARRAY[true, false])", 2);
            assertSelect("SELECT * FROM test_boolean_array WHERE true ANY b",
                    ImmutableList.of(new Object[] {1, ImmutableList.of(true, false)}, new Object[] {2, ImmutableList.of(true, false)}));
        }
        finally {
            dropTable("test_boolean_array");
        }
    }

    @Test
    public void testInteger()
            throws Exception
    {
        try {
            assertCreate("CREATE TABLE test_integer (a integer, b integer) WITH (index_columns='b')");
            assertInsert("INSERT INTO test_integer VALUES (1, 123), (2, 456)", 2);
            assertSelect("SELECT * FROM test_integer WHERE b ANY ARRAY[123, 456]",
                    ImmutableList.of(new Object[] {1, 123}, new Object[] {2, 456}));
        }
        finally {
            dropTable("test_integer");
        }
    }

    @Test
    public void testIntegerArray()
            throws Exception
    {
        try {
            assertCreate("CREATE TABLE test_integer_array (a integer, b array(integer)) WITH (index_columns='b')");
            assertInsert("INSERT INTO test_integer_array VALUES (1, ARRAY[123, 456]), (2, ARRAY[456, 789])", 2);
            assertSelect("SELECT * FROM test_integer_array WHERE 456 ANY b",
                    ImmutableList.of(new Object[] {1, ImmutableList.of(123, 456)}, new Object[] {2, ImmutableList.of(456, 789)}));
        }
        finally {
            dropTable("test");
        }
    }

    @Test
    public void testDate()
            throws Exception
    {
        try {
            assertCreate("CREATE TABLE test_date (a integer, b date) WITH (index_columns='b')");
            assertInsert("INSERT INTO test_date VALUES (1, DATE '2016-05-25'), (2, DATE '2016-05-26')", 2);
            assertSelect("SELECT * FROM test_date WHERE b ANY ARRAY[DATE '2016-05-25', DATE '2016-05-26']",
                    ImmutableList.of(new Object[] {1, DATE1}, new Object[] {2, DATE2}));
        }
        finally {
            dropTable("test_date");
        }
    }

    @Test
    public void testDateArray()
            throws Exception
    {
        try {
            assertCreate("CREATE TABLE test_date_array (a integer, b array(date)) WITH (index_columns='b')");
            assertInsert("INSERT INTO test_date_array VALUES (1, ARRAY[DATE '2016-05-25', DATE '2016-05-26']), (2, ARRAY[DATE '2016-05-26', DATE '2016-05-27'])", 2);
            assertSelect("SELECT * FROM test_date_array WHERE DATE '2016-05-26' ANY b",
                    ImmutableList.of(new Object[] {1, ImmutableList.of(DATE1, DATE2)}, new Object[] {2, ImmutableList.of(DATE2, DATE3)}));
        }
        finally {
            dropTable("test_date_array");
        }
    }

    @Test
    public void testDouble()
            throws Exception
    {
        try {
            assertCreate("CREATE TABLE test_double (a integer, b double) WITH (index_columns='b')");
            assertInsert("INSERT INTO test_double VALUES (1, 123.0), (2, 456.0)", 2);
            assertSelect("SELECT * FROM test_double WHERE b ANY ARRAY[123.0, 456.0]",
                    ImmutableList.of(new Object[] {1, 123.0}, new Object[] {2, 456.0}));
        }
        finally {
            dropTable("test_double");
        }
    }

    @Test
    public void testDoubleArray()
            throws Exception
    {
        try {
            assertCreate("CREATE TABLE test_double_array (a integer, b array(double)) WITH (index_columns='b')");
            assertInsert("INSERT INTO test_double_array VALUES (1, ARRAY[123.0, 456.0]), (2, ARRAY[456.0, 789.0])", 2);
            assertSelect("SELECT * FROM test_double_array WHERE 456.0 ANY b",
                    ImmutableList.of(new Object[] {1, ImmutableList.of(123.0, 456.0)}, new Object[] {2, ImmutableList.of(456.0, 789.0)}));
        }
        finally {
            dropTable("test_double_array");
        }
    }

    @Test
    public void testTime()
            throws Exception
    {
        try {
            assertCreate("CREATE TABLE test_time (a integer, b time) WITH (index_columns='b')");
            assertInsert("INSERT INTO test_time VALUES (1, TIME '01:02:03.123'), (2, TIME '04:05:06.456')", 2);
            assertSelect("SELECT * FROM test_time WHERE b ANY ARRAY[TIME '01:02:03.123', TIME '04:05:06.456']",
                    ImmutableList.of(new Object[] {1, TIME1}, new Object[] {2, TIME2}));
        }
        finally {
            dropTable("test");
        }
    }

    @Test
    public void testTimeArray()
            throws Exception
    {
        try {
            assertCreate("CREATE TABLE test_time_array (a integer, b array(time)) WITH (index_columns='b')");
            assertInsert("INSERT INTO test_time_array VALUES (1, ARRAY[TIME '01:02:03.123', TIME '04:05:06.456']), (2, ARRAY[TIME '04:05:06.456', TIME '07:08:09.789'])", 2);
            assertSelect("SELECT * FROM test_time_array WHERE TIME '04:05:06.456' ANY b",
                    ImmutableList.of(new Object[] {1, ImmutableList.of(TIME1, TIME2)}, new Object[] {2, ImmutableList.of(TIME2, TIME3)}));
        }
        finally {
            dropTable("test_time_array");
        }
    }

    @Test
    public void testTimestamp()
            throws Exception
    {
        try {
            assertCreate("CREATE TABLE test_timestamp (a integer, b timestamp) WITH (index_columns='b')");
            assertInsert("INSERT INTO test_timestamp VALUES (1, TIMESTAMP '2016-05-25 01:02:03.123'), (2, TIMESTAMP '2016-05-26 04:05:06.456')", 2);
            assertSelect("SELECT * FROM test_timestamp WHERE b ANY ARRAY[TIMESTAMP '2016-05-25 01:02:03.123', TIMESTAMP '2016-05-26 04:05:06.456']",
                    ImmutableList.of(new Object[] {1, TIMESTAMP1}, new Object[] {2, TIMESTAMP2}));
        }
        finally {
            dropTable("test");
        }
    }

    @Test
    public void testTimestampArray()
            throws Exception
    {
        try {
            assertCreate("CREATE TABLE test_timestamp_array (a integer, b array(timestamp)) WITH (index_columns='b')");
            assertInsert("INSERT INTO test_timestamp_array VALUES (1, ARRAY[TIMESTAMP '2016-05-25 01:02:03.123', TIMESTAMP '2016-05-26 04:05:06.456']), (2, ARRAY[TIMESTAMP '2016-05-26 04:05:06.456', TIMESTAMP '2016-05-27 07:08:09.789'])", 2);
            assertSelect("SELECT * FROM test_timestamp_array WHERE TIMESTAMP '2016-05-26 04:05:06.456' ANY b",
                    ImmutableList.of(new Object[] {1, ImmutableList.of(TIMESTAMP1, TIMESTAMP2)}, new Object[] {2, ImmutableList.of(TIMESTAMP2, TIMESTAMP3)}));
        }
        finally {
            dropTable("test");
        }
    }

    @Test
    public void testVarchar()
            throws Exception
    {
        try {
            assertCreate("CREATE TABLE test_varchar (a integer, b varchar) WITH (index_columns='b')");
            assertInsert("INSERT INTO test_varchar VALUES (1, '123'), (2, '456')", 2);
            assertSelect("SELECT * FROM test_varchar WHERE b ANY ARRAY['123', '456']",
                    ImmutableList.of(new Object[] {1, "123"}, new Object[] {2, "456"}));
        }
        finally {
            dropTable("test_varchar");
        }
    }

    @Test
    public void testVarcharArray()
            throws Exception
    {
        try {
            assertCreate("CREATE TABLE test_varchar_array (a integer, b array(varchar)) WITH (index_columns='b')");
            assertInsert("INSERT INTO test_varchar_array VALUES (1, ARRAY['123', '456']), (2, ARRAY['456', '789'])", 2);
            assertSelect("SELECT * FROM test_varchar_array WHERE '456' ANY b",
                    ImmutableList.of(new Object[] {1, ImmutableList.of("123", "456")}, new Object[] {2, ImmutableList.of("456", "789")}));
        }
        finally {
            dropTable("test_varchar_array");
        }
    }

    private void dropTable(String table)
    {
        try {
            runner.execute("DROP TABLE " + table);
        }
        catch (Exception e) {
            Logger.get(getClass()).warn(e, "Failed to drop table " + table);
        }
    }
}
