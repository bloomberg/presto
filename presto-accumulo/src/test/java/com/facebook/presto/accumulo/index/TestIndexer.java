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
package com.facebook.presto.accumulo.index;

import com.facebook.presto.accumulo.AccumuloQueryRunner;
import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.index.metrics.MetricsStorage;
import com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision;
import com.facebook.presto.accumulo.index.metrics.MetricsWriter;
import com.facebook.presto.accumulo.index.storage.ShardedIndexStorage;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.model.IndexColumn;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeParameter;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.accumulo.index.Indexer.getTruncatedTimestamps;
import static com.facebook.presto.accumulo.index.Indexer.splitTimestampRange;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.DAY;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.HOUR;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.MILLISECOND;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.MINUTE;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.SECOND;
import static com.facebook.presto.accumulo.metadata.AccumuloTable.DEFAULT_NUM_SHARDS;
import static com.facebook.presto.accumulo.serializers.AccumuloRowSerializer.getBlockFromArray;
import static com.facebook.presto.accumulo.serializers.AccumuloRowSerializer.getBlockFromRow;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.MapParametricType.MAP;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestIndexer
{
    private static final LexicoderRowSerializer SERIALIZER = new LexicoderRowSerializer();

    private static final AccumuloConfig CONFIG = new AccumuloConfig();

    private static final DateTimeFormatter PARSER = ISODateTimeFormat.dateTimeParser();
    private static final Long TIMESTAMP = PARSER.parseDateTime("2001-08-22T03:04:05.321+0000").getMillis();
    private static final byte[] TIMESTAMP_VALUE = encode(TimestampType.TIMESTAMP, PARSER.parseDateTime("2001-08-22T03:04:05.321+0000").getMillis());
    private static final Long SECOND_TIMESTAMP = PARSER.parseDateTime("2001-08-22T03:04:05.000+0000").getMillis();
    private static final byte[] SECOND_TIMESTAMP_VALUE = encode(TimestampType.TIMESTAMP, PARSER.parseDateTime("2001-08-22T03:04:05.000+0000").getMillis());
    private static final Long MINUTE_TIMESTAMP = PARSER.parseDateTime("2001-08-22T03:04:00.000+0000").getMillis();
    private static final byte[] MINUTE_TIMESTAMP_VALUE = encode(TimestampType.TIMESTAMP, PARSER.parseDateTime("2001-08-22T03:04:00.000+0000").getMillis());
    private static final Long HOUR_TIMESTAMP = PARSER.parseDateTime("2001-08-22T03:00:00.000+0000").getMillis();
    private static final byte[] HOUR_TIMESTAMP_VALUE = encode(TimestampType.TIMESTAMP, PARSER.parseDateTime("2001-08-22T03:00:00.000+0000").getMillis());
    private static final Long DAY_TIMESTAMP = PARSER.parseDateTime("2001-08-22T00:00:00.000+0000").getMillis();
    private static final byte[] DAY_TIMESTAMP_VALUE = encode(TimestampType.TIMESTAMP, PARSER.parseDateTime("2001-08-22T00:00:00.000+0000").getMillis());

    private static final byte[] EMPTY_BYTES = new byte[] {};
    private static final String ROW = "row1";
    private static final byte[] CF = bytes("cf");
    private static final byte[] CQ = bytes("a");
    private Connector connector;
    private MetricsStorage metricsStorage;

    @BeforeClass
    public void setupClass()
            throws Exception
    {
        CONFIG.setUsername("root");
        CONFIG.setPassword("secret");

        connector = AccumuloQueryRunner.getAccumuloConnector();
        connector.securityOperations().changeUserAuthorizations("root", new Authorizations("private", "moreprivate", "foo", "bar", "xyzzy"));
        metricsStorage = MetricsStorage.getDefault(connector);
    }

    public AccumuloTable createAndGetTable(Type type)
            throws TableExistsException, AccumuloSecurityException, AccumuloException
    {
        AccumuloTable table = new AccumuloTable("default",
                "index_test_table",
                ImmutableList.of(
                        new AccumuloColumnHandle("id", Optional.empty(), Optional.empty(), VARCHAR, 0, ""),
                        new AccumuloColumnHandle("a", Optional.of("cf"), Optional.of("a"), type, 1, "")
                ), "id",
                false,
                LexicoderRowSerializer.class.getCanonicalName(),
                null,
                Optional.empty(),
                true,
                Optional.of("a"));

        connector.tableOperations().create(table.getFullTableName());
        for (IndexColumn column : table.getParsedIndexColumns()) {
            connector.tableOperations().create(column.getIndexTable());
        }
        metricsStorage.create(table);

        return table;
    }

    @AfterMethod
    public void cleanup()
            throws Exception
    {
        if (connector.tableOperations().exists("index_test_table")) {
            connector.tableOperations().delete("index_test_table");
        }

        if (connector.tableOperations().exists("index_test_table__a")) {
            connector.tableOperations().delete("index_test_table__a");
        }
        if (connector.tableOperations().exists("index_test_table_metrics")) {
            connector.tableOperations().delete("index_test_table_metrics");
        }
    }

    @Test
    public void testIndexBoolean()
            throws Exception
    {
        assertIndexType(BOOLEAN, true, false, true);
    }

    @Test
    public void testIndexTinyInt()
            throws Exception
    {
        assertIndexType(TINYINT, 10L);
    }

    @Test
    public void testIndexSmallInt()
            throws Exception
    {
        assertIndexType(SMALLINT, 10L);
    }

    @Test
    public void testIndexInteger()
            throws Exception
    {
        assertIndexType(INTEGER, 10L);
    }

    @Test
    public void testIndexBigInt()
            throws Exception
    {
        assertIndexType(BIGINT, 10L);
    }

    @Test
    public void testIndexReal()
            throws Exception
    {
        assertIndexType(REAL, 10.0);
    }

    @Test
    public void testIndexDouble()
            throws Exception
    {
        assertIndexType(DOUBLE, 10.0);
    }

    @Test(enabled = false)
    public void testIndexDecimal()
            throws Exception
    {
        // TODO Support for DECIMAL types
        assertIndexType(DecimalType.createDecimalType(), 10.0);
    }

    @Test
    public void testIndexVarchar()
            throws Exception
    {
        assertIndexType(VARCHAR, "abc");
    }

    @Test(enabled = false)
    public void testIndexChar()
            throws Exception
    {
        // TODO Support for CHAR types
        assertIndexType(CharType.createCharType(3), "abc");
    }

    @Test
    public void testIndexVarbinary()
            throws Exception
    {
        assertIndexType(VARBINARY, "abc".getBytes(UTF_8));
    }

    @Test
    public void testIndexDate()
            throws Exception
    {
        assertIndexType(DATE, new Date(System.currentTimeMillis()), true, true);
    }

    @Test
    public void testIndexTime()
            throws Exception
    {
        assertIndexType(TIME, System.currentTimeMillis(), true, true);
    }

    @Test(enabled = false)
    public void testIndexTimeWithTimeZone()
            throws Exception
    {
        // TODO Support for TIME_WITH_TIME_ZONE
        assertIndexType(TIME_WITH_TIME_ZONE, System.currentTimeMillis());
    }

    @Test
    public void testIndexTimestamp()
            throws Exception
    {
        AccumuloTable table = createAndGetTable(TimestampType.TIMESTAMP);
        Mutation mutation = new Mutation(ROW);
        mutation.put(CF, CQ, encode(TimestampType.TIMESTAMP, TIMESTAMP));

        writeMutation(table, mutation);

        List<Entry<Key, Value>> expectedResults = ImmutableList.of(
                kvp(shard(DAY_TIMESTAMP_VALUE), "cf_a_tsd_car", "car", "1"),
                kvp(shard(HOUR_TIMESTAMP_VALUE), "cf_a_tsh_car", "car", "1"),
                kvp(shard(MINUTE_TIMESTAMP_VALUE), "cf_a_tsm_car", "car", "1"),
                kvp(shard(SECOND_TIMESTAMP_VALUE), "cf_a_tss_car", "car", "1"),
                kvp(shard(TIMESTAMP_VALUE), "cf_a_car", "car", "1"),
                kvp(shard(TIMESTAMP_VALUE), "cf_a", ROW, ""))
                .stream()
                .sorted(Comparator.comparing(Entry::getKey))
                .collect(Collectors.toList());

        Scanner scanner = createIndexScanner(table);
        Iterator<Entry<Key, Value>> iterator = scanner.iterator();
        assertTrue(iterator.hasNext());
        for (Entry<Key, Value> expected : expectedResults) {
            assertKeyValuePair(iterator.next(), expected);
        }
        assertFalse(iterator.hasNext());
        scanner.close();
    }

    @Test(enabled = false)
    public void testIndexTimestampWithTimeZone()
            throws Exception
    {
        // TODO Support for TIMESTAMP_WITH_TIME_ZONE
        assertIndexType(TIMESTAMP_WITH_TIME_ZONE, System.currentTimeMillis());
    }

    @Test
    public void testIndexArray()
            throws Exception
    {
        Type type = new ArrayType(VARCHAR);
        AccumuloTable table = createAndGetTable(type);
        Mutation mutation = new Mutation(ROW);
        mutation.put(CF, CQ, encode(type, getBlockFromArray(VARCHAR, ImmutableList.of("abc", "def", "ghi"))));

        writeMutation(table, mutation);

        Scanner scanner = createIndexScanner(table);
        Iterator<Entry<Key, Value>> iterator = scanner.iterator();
        assertTrue(iterator.hasNext());
        assertKeyValuePair(iterator.next(), kvp("abc", "cf_a", ROW, ""));
        assertKeyValuePair(iterator.next(), kvp("abc", "cf_a_car", "car", "1"));
        assertKeyValuePair(iterator.next(), kvp("def", "cf_a", ROW, ""));
        assertKeyValuePair(iterator.next(), kvp("def", "cf_a_car", "car", "1"));
        assertKeyValuePair(iterator.next(), kvp("ghi", "cf_a", ROW, ""));
        assertKeyValuePair(iterator.next(), kvp("ghi", "cf_a_car", "car", "1"));
        assertFalse(iterator.hasNext());
        scanner.close();
    }

    @Test(enabled = false)
    public void testIndexMap()
            throws Exception
    {
        // TODO functionRegistry is null
        Type type = MAP.createType(new TypeRegistry(), ImmutableList.of(TypeParameter.of(VARCHAR), TypeParameter.of(BIGINT)));
        assertIndexType(type, System.currentTimeMillis());
    }

    @Test
    public void testIndexRow()
            throws Exception
    {
        Type type = new RowType(ImmutableList.of(VARCHAR, BIGINT), Optional.empty());
        assertIndexType(type, getBlockFromRow(type, ImmutableList.of("abc", 10L)));
    }

    @Test
    public void testTruncateTimestamp()
    {
        Map<TimestampPrecision, Long> expected = ImmutableMap.of(
                SECOND, SECOND_TIMESTAMP,
                MINUTE, MINUTE_TIMESTAMP,
                HOUR, HOUR_TIMESTAMP,
                DAY, DAY_TIMESTAMP);
        assertEquals(getTruncatedTimestamps(TIMESTAMP), expected);
    }

    @Test
    public void testSplitTimestampRangeNone()
    {
        String startTime = "2001-08-02T00:30:05.321+0000";
        String endTime = "2001-08-02T00:30:05.456+0000";
        Range splitRange = new Range(text(startTime), text(endTime));

        ImmutableMultimap<TimestampPrecision, Range> expected = ImmutableMultimap.of(MILLISECOND, splitRange);
        Multimap<TimestampPrecision, Range> splitRanges = splitTimestampRange(splitRange);
        assertEquals(splitRanges, expected);
    }

    @Test
    public void testSplitTimestampRangeNoneExclusive()
    {
        String startTime = "2001-08-02T00:30:05.321+0000";
        String endTime = "2001-08-02T00:30:05.456+0000";
        Range splitRange = new Range(text(startTime), false, text(endTime), false);

        ImmutableMultimap<TimestampPrecision, Range> expected = ImmutableMultimap.of(MILLISECOND, splitRange);
        Multimap<TimestampPrecision, Range> splitRanges = splitTimestampRange(splitRange);
        assertEquals(splitRanges, expected);
    }

    @Test
    public void testSplitTimestampRangeSecond()
    {
        String startTime = "2001-08-02T00:30:05.321+0000";
        String endTime = "2001-08-02T00:30:12.456+0000";
        Range splitRange = new Range(text(startTime), text(endTime));

        ImmutableMultimap.Builder<TimestampPrecision, Range> builder = ImmutableMultimap.builder();
        builder.put(MILLISECOND, new Range(text(startTime), text("2001-08-02T00:30:05.999+0000")))
                .put(SECOND, new Range(text("2001-08-02T00:30:06.000+0000"), text("2001-08-02T00:30:11.000+0000")))
                .put(MILLISECOND, new Range(text("2001-08-02T00:30:12.000+0000"), text(endTime)));
        Multimap<TimestampPrecision, Range> expected = builder.build();

        Multimap<TimestampPrecision, Range> splitRanges = splitTimestampRange(splitRange);
        assertEquals(splitRanges, expected);
    }

    @Test
    public void testSplitTimestampRangeSecondExclusive()
    {
        String startTime = "2001-08-02T00:30:05.321+0000";
        String endTime = "2001-08-02T00:30:12.456+0000";
        Range splitRange = new Range(text(startTime), false, text(endTime), false);

        ImmutableMultimap.Builder<TimestampPrecision, Range> builder = ImmutableMultimap.builder();
        builder.put(MILLISECOND, new Range(text(startTime), false, text("2001-08-02T00:30:05.999+0000"), true))
                .put(SECOND, new Range(text("2001-08-02T00:30:06.000+0000"), text("2001-08-02T00:30:11.000+0000")))
                .put(MILLISECOND, new Range(text("2001-08-02T00:30:12.000+0000"), true, text(endTime), false));
        Multimap<TimestampPrecision, Range> expected = builder.build();

        Multimap<TimestampPrecision, Range> splitRanges = splitTimestampRange(splitRange);
        assertEquals(splitRanges, expected);
    }

    @Test
    public void testSplitTimestampRangeMinute()
    {
        String startTime = "2001-08-02T00:30:58.321+0000";
        String endTime = "2001-08-02T00:33:03.456+0000";
        Range splitRange = new Range(text(startTime), text(endTime));

        ImmutableMultimap.Builder<TimestampPrecision, Range> builder = ImmutableMultimap.builder();
        builder.put(MILLISECOND, new Range(text(startTime), text("2001-08-02T00:30:58.999+0000")))
                .put(SECOND, new Range(text("2001-08-02T00:30:59.000+0000")))
                .put(MINUTE, new Range(text("2001-08-02T00:31:00.000+0000"), text("2001-08-02T00:32:00.000+0000")))
                .put(SECOND, new Range(text("2001-08-02T00:33:00.000+0000"), text("2001-08-02T00:33:02.000+0000")))
                .put(MILLISECOND, new Range(text("2001-08-02T00:33:03.000+0000"), text(endTime)));
        Multimap<TimestampPrecision, Range> expected = builder.build();

        Multimap<TimestampPrecision, Range> splitRanges = splitTimestampRange(splitRange);
        assertEquals(splitRanges, expected);
    }

    @Test
    public void testSplitTimestampRangeMinuteExclusive()
    {
        String startTime = "2001-08-02T00:30:58.321+0000";
        String endTime = "2001-08-02T00:33:03.456+0000";
        Range splitRange = new Range(text(startTime), false, text(endTime), false);

        ImmutableMultimap.Builder<TimestampPrecision, Range> builder = ImmutableMultimap.builder();
        builder.put(MILLISECOND, new Range(text(startTime), false, text("2001-08-02T00:30:58.999+0000"), true))
                .put(SECOND, new Range(text("2001-08-02T00:30:59.000+0000")))
                .put(MINUTE, new Range(text("2001-08-02T00:31:00.000+0000"), text("2001-08-02T00:32:00.000+0000")))
                .put(SECOND, new Range(text("2001-08-02T00:33:00.000+0000"), text("2001-08-02T00:33:02.000+0000")))
                .put(MILLISECOND, new Range(text("2001-08-02T00:33:03.000+0000"), true, text(endTime), false));
        Multimap<TimestampPrecision, Range> expected = builder.build();

        Multimap<TimestampPrecision, Range> splitRanges = splitTimestampRange(splitRange);
        assertEquals(splitRanges, expected);
    }

    @Test
    public void testSplitTimestampRangeHour()
    {
        String startTime = "2001-08-02T00:58:58.321+0000";
        String endTime = "2001-08-02T03:03:03.456+0000";
        Range splitRange = new Range(text(startTime), text(endTime));

        ImmutableMultimap.Builder<TimestampPrecision, Range> builder = ImmutableMultimap.builder();
        builder.put(MILLISECOND, new Range(text(startTime), text("2001-08-02T00:58:58.999+0000")))
                .put(SECOND, new Range(text("2001-08-02T00:58:59.000+0000")))
                .put(MINUTE, new Range(text("2001-08-02T00:59:00.000+0000")))
                .put(HOUR, new Range(text("2001-08-02T01:00:00.000+0000"), text("2001-08-02T02:00:00.000+0000")))
                .put(MINUTE, new Range(text("2001-08-02T03:00:00.000+0000"), text("2001-08-02T03:02:00.000+0000")))
                .put(SECOND, new Range(text("2001-08-02T03:03:00.000+0000"), text("2001-08-02T03:03:02.000+0000")))
                .put(MILLISECOND, new Range(text("2001-08-02T03:03:03.000+0000"), text(endTime)));
        Multimap<TimestampPrecision, Range> expected = builder.build();

        Multimap<TimestampPrecision, Range> splitRanges = splitTimestampRange(splitRange);
        assertEquals(splitRanges, expected);
    }

    @Test
    public void testSplitTimestampRangeHourExclusive()
    {
        String startTime = "2001-08-02T00:58:58.321+0000";
        String endTime = "2001-08-02T03:03:03.456+0000";
        Range splitRange = new Range(text(startTime), false, text(endTime), false);

        ImmutableMultimap.Builder<TimestampPrecision, Range> builder = ImmutableMultimap.builder();
        builder.put(MILLISECOND, new Range(text(startTime), false, text("2001-08-02T00:58:58.999+0000"), true))
                .put(SECOND, new Range(text("2001-08-02T00:58:59.000+0000")))
                .put(MINUTE, new Range(text("2001-08-02T00:59:00.000+0000")))
                .put(HOUR, new Range(text("2001-08-02T01:00:00.000+0000"), text("2001-08-02T02:00:00.000+0000")))
                .put(MINUTE, new Range(text("2001-08-02T03:00:00.000+0000"), text("2001-08-02T03:02:00.000+0000")))
                .put(SECOND, new Range(text("2001-08-02T03:03:00.000+0000"), text("2001-08-02T03:03:02.000+0000")))
                .put(MILLISECOND, new Range(text("2001-08-02T03:03:03.000+0000"), true, text(endTime), false));
        Multimap<TimestampPrecision, Range> expected = builder.build();

        Multimap<TimestampPrecision, Range> splitRanges = splitTimestampRange(splitRange);
        assertEquals(splitRanges, expected);
    }

    @Test
    public void testSplitTimestampRangeDay()
    {
        String startTime = "2001-08-02T22:58:58.321+0000";
        String endTime = "2001-08-05T01:03:03.456+0000";
        Range splitRange = new Range(text(startTime), text(endTime));

        ImmutableMultimap.Builder<TimestampPrecision, Range> builder = ImmutableMultimap.builder();
        builder.put(MILLISECOND, new Range(text(startTime), text("2001-08-02T22:58:58.999+0000")))
                .put(SECOND, new Range(text("2001-08-02T22:58:59.000+0000")))
                .put(MINUTE, new Range(text("2001-08-02T22:59:00.000+0000")))
                .put(HOUR, new Range(text("2001-08-02T23:00:00.000+0000")))
                .put(DAY, new Range(text("2001-08-03T00:00:00.000+0000"), text("2001-08-04T00:00:00.000+0000")))
                .put(HOUR, new Range(text("2001-08-05T00:00:00.000+0000")))
                .put(MINUTE, new Range(text("2001-08-05T01:00:00.000+0000"), text("2001-08-05T01:02:00.000+0000")))
                .put(SECOND, new Range(text("2001-08-05T01:03:00.000+0000"), text("2001-08-05T01:03:02.000+0000")))
                .put(MILLISECOND, new Range(text("2001-08-05T01:03:03.000+0000"), text(endTime)));
        Multimap<TimestampPrecision, Range> expected = builder.build();

        Multimap<TimestampPrecision, Range> splitRanges = splitTimestampRange(splitRange);
        assertEquals(splitRanges, expected);
    }

    @Test
    public void testSplitTimestampRangeDayExclusive()
    {
        String startTime = "2001-08-02T22:58:58.321+0000";
        String endTime = "2001-08-05T01:03:03.456+0000";
        Range splitRange = new Range(text(startTime), false, text(endTime), false);

        ImmutableMultimap.Builder<TimestampPrecision, Range> builder = ImmutableMultimap.builder();
        builder.put(MILLISECOND, new Range(text(startTime), false, text("2001-08-02T22:58:58.999+0000"), true))
                .put(SECOND, new Range(text("2001-08-02T22:58:59.000+0000")))
                .put(MINUTE, new Range(text("2001-08-02T22:59:00.000+0000")))
                .put(HOUR, new Range(text("2001-08-02T23:00:00.000+0000")))
                .put(DAY, new Range(text("2001-08-03T00:00:00.000+0000"), text("2001-08-04T00:00:00.000+0000")))
                .put(HOUR, new Range(text("2001-08-05T00:00:00.000+0000")))
                .put(MINUTE, new Range(text("2001-08-05T01:00:00.000+0000"), text("2001-08-05T01:02:00.000+0000")))
                .put(SECOND, new Range(text("2001-08-05T01:03:00.000+0000"), text("2001-08-05T01:03:02.000+0000")))
                .put(MILLISECOND, new Range(text("2001-08-05T01:03:03.000+0000"), true, text(endTime), false));
        Multimap<TimestampPrecision, Range> expected = builder.build();

        Multimap<TimestampPrecision, Range> splitRanges = splitTimestampRange(splitRange);
        assertEquals(splitRanges, expected);
    }

    @Test
    public void testSplitTimestampRangeHourRange()
    {
        String startTime = "2001-08-02T22:58:00.000+0000";
        String endTime = "2001-08-05T02:02:00.000+0000";
        Range splitRange = new Range(text(startTime), text(endTime));

        ImmutableMultimap.Builder<TimestampPrecision, Range> builder = ImmutableMultimap.builder();
        builder.put(MINUTE, new Range(text(startTime), text("2001-08-02T22:59:00.000+0000")))
                .put(HOUR, new Range(text("2001-08-02T23:00:00.000+0000")))
                .put(DAY, new Range(text("2001-08-03T00:00:00.000+0000"), text("2001-08-04T00:00:00.000+0000")))
                .put(HOUR, new Range(text("2001-08-05T00:00:00.000+0000"), text("2001-08-05T01:00:00.000+0000")))
                .put(MINUTE, new Range(text("2001-08-05T02:00:00.000+0000")))
                .put(SECOND, new Range(text("2001-08-05T02:01:00.000+0000"), text("2001-08-05T02:01:58.000+0000")))
                .put(MILLISECOND, new Range(text("2001-08-05T02:01:59.000+0000"), text("2001-08-05T02:01:59.999+0000")));
        Multimap<TimestampPrecision, Range> expected = builder.build();

        Multimap<TimestampPrecision, Range> splitRanges = splitTimestampRange(splitRange);
        assertEquals(splitRanges, expected);
    }

    @Test
    public void testSplitTimestampRangeInfiniteEnd()
    {
        String startTime = "2001-08-02T00:30:05.321+0000";
        Range splitRange = new Range(text(startTime), null);
        Multimap<TimestampPrecision, Range> expected = ImmutableMultimap.of(MILLISECOND, splitRange);
        Multimap<TimestampPrecision, Range> splitRanges = splitTimestampRange(splitRange);
        assertEquals(splitRanges, expected);
    }

    @Test
    public void testSplitTimestampRangeInfiniteStart()
    {
        String endTime = "2001-08-07T00:32:07.456+0000";
        Range splitRange = new Range(null, text(endTime));
        Multimap<TimestampPrecision, Range> expected = ImmutableMultimap.of(MILLISECOND, splitRange);
        Multimap<TimestampPrecision, Range> splitRanges = splitTimestampRange(splitRange);
        assertEquals(splitRanges, expected);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testSplitTimestampRangeRangeNull()
    {
        splitTimestampRange(null);
    }

    private static Entry<Key, Value> kvp(String rowId, String cf, String cq, String value)
    {
        return kvp(rowId.getBytes(UTF_8), cf, cq, value);
    }

    private static Entry<Key, Value> kvp(byte[] rowId, String cf, String cq, String value)
    {
        return Pair.of(new Key(rowId, cf.getBytes(UTF_8), cq.getBytes(UTF_8), EMPTY_BYTES, 0), new Value(value.getBytes(UTF_8)));
    }

    private static Text text(String v)
    {
        return new Text(encode(TimestampType.TIMESTAMP, ts(v).getTime()));
    }

    private static Timestamp ts(String date)
    {
        return new Timestamp(PARSER.parseDateTime(date).getMillis());
    }

    private static byte[] encode(Type type, Object v)
    {
        return SERIALIZER.encode(type, v);
    }

    private static void assertKeyValuePair(Entry<Key, Value> actual, Entry<Key, Value> expected)
    {
        assertKeyValuePair(actual, expected, false);
    }

    private static void assertKeyValuePair(Entry<Key, Value> actual, Entry<Key, Value> expected, boolean postfixed)
    {
        if (postfixed) {
            byte[] rowId = actual.getKey().getRow().copyBytes();
            assertEquals(Arrays.copyOfRange(rowId, 0, rowId.length - 8), expected.getKey().getRow().copyBytes(), format("Expected %s, but got %s", expected.getKey().getRow().toString(), actual.getKey().getRow().toString()));
        }
        else {
            assertEquals(actual.getKey().getRow().copyBytes(), expected.getKey().getRow().copyBytes(), format("Expected %s, but got %s", expected.getKey().getRow().toString(), actual.getKey().getRow().toString()));
        }

        assertEquals(actual.getKey().getColumnFamily().copyBytes(), expected.getKey().getColumnFamily().copyBytes(), format("Expected %s, but got %s", expected.getKey().getColumnFamily().toString(), actual.getKey().getColumnFamily().toString()));
        assertEquals(actual.getKey().getColumnQualifier().copyBytes(), expected.getKey().getColumnQualifier().copyBytes(), format("Expected %s, but got %s", expected.getKey().getColumnQualifier().toString(), actual.getKey().getColumnQualifier().toString()));
        assertTrue(actual.getKey().getTimestamp() > 0, "Timestamp is zero");
        assertEquals(actual.getValue().toString(), expected.getValue().toString());
    }

    private static byte[] bytes(String s)
    {
        return s.getBytes(UTF_8);
    }

    private static byte[] shard(byte[] bytes)
    {
        return new ShardedIndexStorage(DEFAULT_NUM_SHARDS).encode(bytes);
    }

    private Scanner createIndexScanner(AccumuloTable table)
            throws TableNotFoundException
    {
        return connector.createScanner(table.getParsedIndexColumns().get(0).getIndexTable(), new Authorizations("private", "moreprivate"));
    }

    private void writeMutation(AccumuloTable table, Mutation mutation)
            throws Exception
    {
        MultiTableBatchWriter multiTableBatchWriter = connector.createMultiTableBatchWriter(new BatchWriterConfig());
        BatchWriter dataWriter = multiTableBatchWriter.getBatchWriter(table.getFullTableName());
        MetricsWriter metricsWriter = table.getMetricsStorageInstance(connector).newWriter(table);
        Indexer indexer = new Indexer(connector, table, multiTableBatchWriter, metricsWriter);

        dataWriter.addMutation(mutation);
        indexer.index(mutation);
        metricsWriter.close();
        multiTableBatchWriter.close();
    }

    private void assertIndexType(Type type, Object value)
            throws Exception
    {
        assertIndexType(type, value, false, false);
    }

    private void assertIndexType(Type type, Object value, boolean sharded, boolean postfixed)
            throws Exception
    {
        AccumuloTable table = createAndGetTable(type);
        Mutation mutation = new Mutation(ROW);
        mutation.put(CF, CQ, encode(type, value));

        writeMutation(table, mutation);

        Scanner scanner = createIndexScanner(table);
        Iterator<Entry<Key, Value>> iterator = scanner.iterator();
        assertTrue(iterator.hasNext());
        if (sharded) {
            if (postfixed) {
                assertKeyValuePair(iterator.next(), kvp(shard(encode(type, value)), "cf_a_car", "car", "1"));
                assertKeyValuePair(iterator.next(), kvp(shard(encode(type, value)), "cf_a", ROW, ""), true);
            }
            else {
                assertKeyValuePair(iterator.next(), kvp(shard(encode(type, value)), "cf_a_car", "car", "1"));
                assertKeyValuePair(iterator.next(), kvp(shard(encode(type, value)), "cf_a", ROW, ""));
            }
        }
        else {
            if (postfixed) {
                assertKeyValuePair(iterator.next(), kvp(encode(type, value), "cf_a_car", "car", "1"));
                assertKeyValuePair(iterator.next(), kvp(encode(type, value), "cf_a", ROW, ""), true);
            }
            else {
                assertKeyValuePair(iterator.next(), kvp(encode(type, value), "cf_a", ROW, ""));
                assertKeyValuePair(iterator.next(), kvp(encode(type, value), "cf_a_car", "car", "1"));
            }
        }
        assertFalse(iterator.hasNext());
        scanner.close();
    }
}
