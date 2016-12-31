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
import com.facebook.presto.accumulo.index.metrics.MetricCacheKey;
import com.facebook.presto.accumulo.index.metrics.MetricsStorage;
import com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision;
import com.facebook.presto.accumulo.index.metrics.MetricsWriter;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Bytes;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.facebook.presto.accumulo.index.Indexer.getTruncatedTimestamps;
import static com.facebook.presto.accumulo.index.Indexer.splitTimestampRange;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.METRICS_TABLE_ROWS_COLUMN;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.METRICS_TABLE_ROW_ID;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.DAY;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.HOUR;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.MILLISECOND;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.MINUTE;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.SECOND;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.nio.ByteBuffer.wrap;
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
    private static final Long TIMESTAMP_VALUE = PARSER.parseDateTime("2001-08-22T03:04:05.321+0000").getMillis();
    private static final Long SECOND_TIMESTAMP = PARSER.parseDateTime("2001-08-22T03:04:05.000+0000").getMillis();
    private static final byte[] SECOND_TIMESTAMP_VALUE = encode(TIMESTAMP, PARSER.parseDateTime("2001-08-22T03:04:05.000+0000").getMillis());
    private static final Long MINUTE_TIMESTAMP = PARSER.parseDateTime("2001-08-22T03:04:00.000+0000").getMillis();
    private static final byte[] MINUTE_TIMESTAMP_VALUE = encode(TIMESTAMP, PARSER.parseDateTime("2001-08-22T03:04:00.000+0000").getMillis());
    private static final Long HOUR_TIMESTAMP = PARSER.parseDateTime("2001-08-22T03:00:00.000+0000").getMillis();
    private static final byte[] HOUR_TIMESTAMP_VALUE = encode(TIMESTAMP, PARSER.parseDateTime("2001-08-22T03:00:00.000+0000").getMillis());
    private static final Long DAY_TIMESTAMP = PARSER.parseDateTime("2001-08-22T00:00:00.000+0000").getMillis();
    private static final byte[] DAY_TIMESTAMP_VALUE = encode(TIMESTAMP, PARSER.parseDateTime("2001-08-22T00:00:00.000+0000").getMillis());

    private static final byte[] NULL_BYTE = bytes("\0");
    private static final byte[] AGE = bytes("age");
    private static final byte[] CF = bytes("cf");
    private static final byte[] FIRSTNAME = bytes("firstname");
    private static final byte[] SENDERS = bytes("arr");
    private static final byte[] BORN = bytes("born");

    private static final byte[] M1_ROWID = encode(VARCHAR, "row1");
    private static final byte[] AGE_VALUE = encode(BIGINT, 27L);
    private static final byte[] M1_FNAME_VALUE = encode(VARCHAR, "alice");
    private static final byte[] M1_ARR_VALUE = encode(new ArrayType(VARCHAR), AccumuloRowSerializer.getBlockFromArray(VARCHAR, ImmutableList.of("abc", "def", "ghi")));
    private static final byte[] BORN_VALUE = encode(TIMESTAMP, TIMESTAMP_VALUE);

    private static final byte[] M2_ROWID = encode(VARCHAR, "row2");
    private static final byte[] M2_FNAME_VALUE = encode(VARCHAR, "bob");
    private static final byte[] M2_ARR_VALUE = encode(new ArrayType(VARCHAR), AccumuloRowSerializer.getBlockFromArray(VARCHAR, ImmutableList.of("ghi", "mno", "abc")));

    private static final byte[] M3_ROWID = encode(VARCHAR, "row3");
    private static final byte[] M3_FNAME_VALUE = encode(VARCHAR, "carol");
    private static final byte[] M3_ARR_VALUE = encode(new ArrayType(VARCHAR), AccumuloRowSerializer.getBlockFromArray(VARCHAR, ImmutableList.of("def", "ghi", "jkl")));

    private Mutation m1;
    private Mutation m2;
    private Mutation m2v;
    private Mutation m3v;
    private AccumuloTable table;
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

        AccumuloColumnHandle c1 = new AccumuloColumnHandle("id", Optional.empty(), Optional.empty(), VARCHAR, 0, "");
        AccumuloColumnHandle c2 = new AccumuloColumnHandle("age", Optional.of("cf"), Optional.of("age"), BIGINT, 1, "");
        AccumuloColumnHandle c3 = new AccumuloColumnHandle("firstname", Optional.of("cf"), Optional.of("firstname"), VARCHAR, 2, "");
        AccumuloColumnHandle c4 = new AccumuloColumnHandle("arr", Optional.of("cf"), Optional.of("arr"), new ArrayType(VARCHAR), 3, "");
        AccumuloColumnHandle c5 = new AccumuloColumnHandle("born", Optional.of("cf"), Optional.of("born"), TIMESTAMP, 4, "");

        table = new AccumuloTable("default", "index_test_table", ImmutableList.of(c1, c2, c3, c4, c5), "id", false, LexicoderRowSerializer.class.getCanonicalName(), null, Optional.empty(), true, Optional.of("age,firstname,arr,born,firstname:born,arr:born"));

        m1 = new Mutation(M1_ROWID);
        m1.put(CF, AGE, AGE_VALUE);
        m1.put(CF, FIRSTNAME, M1_FNAME_VALUE);
        m1.put(CF, SENDERS, M1_ARR_VALUE);
        m1.put(CF, BORN, BORN_VALUE);

        m2 = new Mutation(M2_ROWID);
        m2.put(CF, AGE, AGE_VALUE);
        m2.put(CF, FIRSTNAME, M2_FNAME_VALUE);
        m2.put(CF, SENDERS, M2_ARR_VALUE);
        m2.put(CF, BORN, BORN_VALUE);

        ColumnVisibility visibility1 = new ColumnVisibility("private");
        m2v = new Mutation(M2_ROWID);
        m2v.put(CF, AGE, visibility1, AGE_VALUE);
        m2v.put(CF, FIRSTNAME, visibility1, M2_FNAME_VALUE);
        m2v.put(CF, SENDERS, visibility1, M2_ARR_VALUE);
        m2v.put(CF, BORN, visibility1, BORN_VALUE);

        ColumnVisibility visibility2 = new ColumnVisibility("moreprivate");
        m3v = new Mutation(M3_ROWID);
        m3v.put(CF, AGE, visibility2, AGE_VALUE);
        m3v.put(CF, FIRSTNAME, visibility2, M3_FNAME_VALUE);
        m3v.put(CF, SENDERS, visibility2, M3_ARR_VALUE);
        m3v.put(CF, BORN, visibility2, BORN_VALUE);
    }

    @AfterMethod
    public void cleanup()
            throws Exception
    {
        if (connector.tableOperations().exists(table.getFullTableName())) {
            connector.tableOperations().delete(table.getFullTableName());
        }

        if (connector.tableOperations().exists(table.getIndexTableName())) {
            connector.tableOperations().delete(table.getIndexTableName());
        }

        metricsStorage.drop(table);
    }

    @Test
    public void testMutationIndex()
            throws Exception
    {
        connector.tableOperations().create(table.getFullTableName());
        connector.tableOperations().create(table.getIndexTableName());
        metricsStorage.create(table);

        MultiTableBatchWriter multiTableBatchWriter = connector.createMultiTableBatchWriter(new BatchWriterConfig());
        BatchWriter dataWriter = multiTableBatchWriter.getBatchWriter(table.getFullTableName());
        MetricsWriter metricsWriter = table.getMetricsStorageInstance(connector).newWriter(table);
        Indexer indexer = new Indexer(connector, table, multiTableBatchWriter.getBatchWriter(table.getIndexTableName()), metricsWriter);

        dataWriter.addMutation(m1);
        indexer.index(m1);
        metricsWriter.flush();
        multiTableBatchWriter.flush();

        Scanner scan = connector.createScanner(table.getIndexTableName(), new Authorizations());

        Iterator<Entry<Key, Value>> iter = scan.iterator();
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), BORN_VALUE, "cf_born", "row1", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("abc"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row1", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), Bytes.concat(M1_FNAME_VALUE, NULL_BYTE, BORN_VALUE), "cf_firstname-cf_born", "row1", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("def"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row1", "");
        assertFalse(iter.hasNext());

        scan.close();

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_age", AGE_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_born", BORN_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "abc")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M1_FNAME_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "def")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "ghi")), 1);

        dataWriter.addMutation(m2);
        indexer.index(m2);
        metricsWriter.close();
        multiTableBatchWriter.close();

        scan = connector.createScanner(table.getIndexTableName(), new Authorizations());
        iter = scan.iterator();
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row2", "");
        assertKeyValuePair(iter.next(), BORN_VALUE, "cf_born", "row1", "");
        assertKeyValuePair(iter.next(), BORN_VALUE, "cf_born", "row2", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row2", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("abc"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row1", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("abc"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row2", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), Bytes.concat(M1_FNAME_VALUE, NULL_BYTE, BORN_VALUE), "cf_firstname-cf_born", "row1", "");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "row2", "");
        assertKeyValuePair(iter.next(), Bytes.concat(M2_FNAME_VALUE, NULL_BYTE, BORN_VALUE), "cf_firstname-cf_born", "row2", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("def"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row2", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row1", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row2", "");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "row2", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("mno"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row2", "");
        assertFalse(iter.hasNext());

        scan.close();

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_age", AGE_VALUE)), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_born", BORN_VALUE)), 2);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "abc")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M1_FNAME_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M2_FNAME_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "def")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "ghi")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "mno")), 1);
    }

    @Test
    public void testMutationIndexWithVisibilities()
            throws Exception
    {
        connector.tableOperations().create(table.getFullTableName());
        connector.tableOperations().create(table.getIndexTableName());
        metricsStorage.create(table);

        MultiTableBatchWriter multiTableBatchWriter = connector.createMultiTableBatchWriter(new BatchWriterConfig());
        BatchWriter dataWriter = multiTableBatchWriter.getBatchWriter(table.getFullTableName());
        MetricsWriter metricsWriter = table.getMetricsStorageInstance(connector).newWriter(table);
        Indexer indexer = new Indexer(connector, table, multiTableBatchWriter.getBatchWriter(table.getIndexTableName()), metricsWriter);

        dataWriter.addMutation(m1);
        indexer.index(m1);
        metricsWriter.flush();
        multiTableBatchWriter.flush();

        Scanner scan = connector.createScanner(table.getIndexTableName(), new Authorizations());

        Iterator<Entry<Key, Value>> iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), BORN_VALUE, "cf_born", "row1", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("abc"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row1", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), Bytes.concat(M1_FNAME_VALUE, NULL_BYTE, BORN_VALUE), "cf_firstname-cf_born", "row1", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("def"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row1", "");
        assertFalse(iter.hasNext());

        scan.close();

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_age", AGE_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_born", BORN_VALUE, "private")), 1);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "abc")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M1_FNAME_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "def")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "ghi")), 1);

        dataWriter.addMutation(m2v);
        indexer.index(m2v);
        metricsWriter.flush();
        multiTableBatchWriter.flush();

        scan = connector.createScanner(table.getIndexTableName(), new Authorizations("private"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row2", "private", "");
        assertKeyValuePair(iter.next(), BORN_VALUE, "cf_born", "row1", "");
        assertKeyValuePair(iter.next(), BORN_VALUE, "cf_born", "row2", "private", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("abc"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row1", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("abc"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row2", "private", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), Bytes.concat(M1_FNAME_VALUE, NULL_BYTE, BORN_VALUE), "cf_firstname-cf_born", "row1", "");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "row2", "private", "");
        assertKeyValuePair(iter.next(), Bytes.concat(M2_FNAME_VALUE, NULL_BYTE, BORN_VALUE), "cf_firstname-cf_born", "row2", "private", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("def"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row1", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row2", "private", "");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("mno"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row2", "private", "");
        assertFalse(iter.hasNext());
        scan.close();

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_age", AGE_VALUE, "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_born", BORN_VALUE, "private")), 2);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "abc", "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M1_FNAME_VALUE, "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M2_FNAME_VALUE, "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "def", "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "ghi", "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "mno", "private")), 1);

        dataWriter.addMutation(m3v);
        indexer.index(m3v);
        metricsWriter.close();
        multiTableBatchWriter.close();

        scan = connector.createScanner(table.getIndexTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row2", "private", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), BORN_VALUE, "cf_born", "row1", "");
        assertKeyValuePair(iter.next(), BORN_VALUE, "cf_born", "row2", "private", "");
        assertKeyValuePair(iter.next(), BORN_VALUE, "cf_born", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("abc"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row1", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("abc"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row2", "private", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), Bytes.concat(M1_FNAME_VALUE, NULL_BYTE, BORN_VALUE), "cf_firstname-cf_born", "row1", "");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "row2", "private", "");
        assertKeyValuePair(iter.next(), Bytes.concat(M2_FNAME_VALUE, NULL_BYTE, BORN_VALUE), "cf_firstname-cf_born", "row2", "private", "");
        assertKeyValuePair(iter.next(), M3_FNAME_VALUE, "cf_firstname", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), Bytes.concat(M3_FNAME_VALUE, NULL_BYTE, BORN_VALUE), "cf_firstname-cf_born", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("def"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row1", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("def"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row1", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row2", "private", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("jkl"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("jkl"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("mno"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row2", "private", "");
        assertFalse(iter.hasNext());

        scan.close();

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_age", AGE_VALUE, "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_born", BORN_VALUE, "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "abc", "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M1_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M2_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M3_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "def", "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "ghi", "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "jkl", "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "mno", "private", "moreprivate")), 1);
    }

    @Test
    public void testDeleteMutationIndex()
            throws Exception
    {
        // Populate the table with data
        testMutationIndex();

        MultiTableBatchWriter multiTableBatchWriter = connector.createMultiTableBatchWriter(new BatchWriterConfig());
        MetricsWriter metricsWriter = table.getMetricsStorageInstance(connector).newWriter(table);
        Indexer indexer = new Indexer(connector, table, multiTableBatchWriter.getBatchWriter(table.getIndexTableName()), metricsWriter);
        indexer.delete(connector.securityOperations().getUserAuthorizations("root"), M1_ROWID);
        metricsWriter.flush();
        multiTableBatchWriter.flush();

        Scanner scan = connector.createScanner(table.getIndexTableName(), new Authorizations());

        Iterator<Entry<Key, Value>> iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row2", "");
        assertKeyValuePair(iter.next(), BORN_VALUE, "cf_born", "row2", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row2", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("abc"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row2", "");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "row2", "");
        assertKeyValuePair(iter.next(), Bytes.concat(M2_FNAME_VALUE, NULL_BYTE, BORN_VALUE), "cf_firstname-cf_born", "row2", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row2", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row2", "");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "row2", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("mno"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row2", "");
        assertFalse(iter.hasNext());

        scan.close();

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_age", AGE_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_born", BORN_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "abc")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M1_FNAME_VALUE)), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M2_FNAME_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "def")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "ghi")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "mno")), 1);

        indexer.delete(connector.securityOperations().getUserAuthorizations("root"), M2_ROWID);
        metricsWriter.close();
        multiTableBatchWriter.close();

        scan = connector.createScanner(table.getIndexTableName(), new Authorizations());
        iter = scan.iterator();
        assertFalse(iter.hasNext());

        scan.close();

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_age", AGE_VALUE)), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_born", BORN_VALUE)), 0);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "abc")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M1_FNAME_VALUE)), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M2_FNAME_VALUE)), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "def")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "ghi")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "mno")), 0);
    }

    @Test
    public void testDeleteMutationIndexWithVisibilities()
            throws Exception
    {
        // Populate the table with data
        testMutationIndexWithVisibilities();

        MultiTableBatchWriter multiTableBatchWriter = connector.createMultiTableBatchWriter(new BatchWriterConfig());
        MetricsWriter metricsWriter = table.getMetricsStorageInstance(connector).newWriter(table);
        Indexer indexer = new Indexer(connector, table, multiTableBatchWriter.getBatchWriter(table.getIndexTableName()), metricsWriter);

        indexer.delete(connector.securityOperations().getUserAuthorizations("root"), M1_ROWID);
        metricsWriter.flush();
        multiTableBatchWriter.flush();

        Scanner scan = connector.createScanner(table.getIndexTableName(), new Authorizations("private", "moreprivate"));

        Iterator<Entry<Key, Value>> iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row2", "private", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), BORN_VALUE, "cf_born", "row2", "private", "");
        assertKeyValuePair(iter.next(), BORN_VALUE, "cf_born", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("abc"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row2", "private", "");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "row2", "private", "");
        assertKeyValuePair(iter.next(), Bytes.concat(M2_FNAME_VALUE, NULL_BYTE, BORN_VALUE), "cf_firstname-cf_born", "row2", "private", "");
        assertKeyValuePair(iter.next(), M3_FNAME_VALUE, "cf_firstname", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), Bytes.concat(M3_FNAME_VALUE, NULL_BYTE, BORN_VALUE), "cf_firstname-cf_born", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("def"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row2", "private", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("jkl"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("jkl"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("mno"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row2", "private", "");
        assertFalse(iter.hasNext());

        scan.close();

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_age", AGE_VALUE, "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_born", BORN_VALUE, "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "abc", "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M1_FNAME_VALUE, "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M2_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M3_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "def", "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "ghi", "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "jkl", "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "mno", "private", "moreprivate")), 1);

        indexer.delete(new Authorizations(), M2_ROWID);
        metricsWriter.flush();
        multiTableBatchWriter.flush();

        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row2", "private", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), BORN_VALUE, "cf_born", "row2", "private", "");
        assertKeyValuePair(iter.next(), BORN_VALUE, "cf_born", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("abc"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row2", "private", "");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "row2", "private", "");
        assertKeyValuePair(iter.next(), Bytes.concat(M2_FNAME_VALUE, NULL_BYTE, BORN_VALUE), "cf_firstname-cf_born", "row2", "private", "");
        assertKeyValuePair(iter.next(), M3_FNAME_VALUE, "cf_firstname", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), Bytes.concat(M3_FNAME_VALUE, NULL_BYTE, BORN_VALUE), "cf_firstname-cf_born", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("def"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row2", "private", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("jkl"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("jkl"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("mno"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row2", "private", "");
        assertFalse(iter.hasNext());

        scan.close();

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_age", AGE_VALUE, "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_born", BORN_VALUE, "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "abc", "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M1_FNAME_VALUE, "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M2_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M3_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "def", "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "ghi", "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "jkl", "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "mno", "private", "moreprivate")), 1);

        indexer.delete(connector.securityOperations().getUserAuthorizations("root"), M2_ROWID);
        metricsWriter.flush();
        multiTableBatchWriter.flush();

        scan = connector.createScanner(table.getIndexTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), BORN_VALUE, "cf_born", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), M3_FNAME_VALUE, "cf_firstname", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), Bytes.concat(M3_FNAME_VALUE, NULL_BYTE, BORN_VALUE), "cf_firstname-cf_born", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("def"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("jkl"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("jkl"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "row3", "moreprivate", "");
        assertFalse(iter.hasNext());
        scan.close();

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_age", AGE_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_born", BORN_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "abc", "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M1_FNAME_VALUE, "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M2_FNAME_VALUE, "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M3_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "def", "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "ghi", "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "jkl", "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "mno", "private", "moreprivate")), 0);

        indexer.delete(connector.securityOperations().getUserAuthorizations("root"), M3_ROWID);
        metricsWriter.close();
        multiTableBatchWriter.close();

        scan = connector.createScanner(table.getIndexTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertFalse(iter.hasNext());

        scan.close();

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_age", AGE_VALUE, "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_born", BORN_VALUE, "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "abc", "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M1_FNAME_VALUE, "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M2_FNAME_VALUE, "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M3_FNAME_VALUE, "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "def", "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "ghi", "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "jkl", "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "mno", "private", "moreprivate")), 0);
    }

    @Test
    public void testTimestampMetricsViaIndexer()
            throws Exception
    {
        connector.tableOperations().create(table.getFullTableName());
        connector.tableOperations().create(table.getIndexTableName());
        metricsStorage.create(table);

        MultiTableBatchWriter multiTableBatchWriter = connector.createMultiTableBatchWriter(new BatchWriterConfig());
        BatchWriter writer = multiTableBatchWriter.getBatchWriter(table.getFullTableName());
        MetricsWriter metricsWriter = table.getMetricsStorageInstance(connector).newWriter(table);
        Indexer indexer = new Indexer(connector, table, multiTableBatchWriter.getBatchWriter(table.getIndexTableName()), metricsWriter);

        writer.addMutations(ImmutableList.of(m1, m2, m2v, m3v));
        indexer.index(ImmutableList.of(m1, m2, m2v, m3v));
        metricsWriter.flush();
        multiTableBatchWriter.close();

        Scanner scan = connector.createScanner(table.getIndexTableName() + "_metrics", new Authorizations("private", "moreprivate"));
        Iterator<Entry<Key, Value>> iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "___card___", "2");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), DAY_TIMESTAMP_VALUE, "cf_born_tsd", "___card___", "2");
        assertKeyValuePair(iter.next(), DAY_TIMESTAMP_VALUE, "cf_born_tsd", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), DAY_TIMESTAMP_VALUE, "cf_born_tsd", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), HOUR_TIMESTAMP_VALUE, "cf_born_tsh", "___card___", "2");
        assertKeyValuePair(iter.next(), HOUR_TIMESTAMP_VALUE, "cf_born_tsh", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), HOUR_TIMESTAMP_VALUE, "cf_born_tsh", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), MINUTE_TIMESTAMP_VALUE, "cf_born_tsm", "___card___", "2");
        assertKeyValuePair(iter.next(), MINUTE_TIMESTAMP_VALUE, "cf_born_tsm", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), MINUTE_TIMESTAMP_VALUE, "cf_born_tsm", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), SECOND_TIMESTAMP_VALUE, "cf_born_tss", "___card___", "2");
        assertKeyValuePair(iter.next(), SECOND_TIMESTAMP_VALUE, "cf_born_tss", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), SECOND_TIMESTAMP_VALUE, "cf_born_tss", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), BORN_VALUE, "cf_born", "___card___", "2");
        assertKeyValuePair(iter.next(), BORN_VALUE, "cf_born", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), BORN_VALUE, "cf_born", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), METRICS_TABLE_ROW_ID.array(), new String(METRICS_TABLE_ROWS_COLUMN.array(), UTF_8), "___card___", "4");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "___card___", "2");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("abc"), NULL_BYTE, DAY_TIMESTAMP_VALUE), "cf_arr-cf_born_tsd", "___card___", "", "2");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("abc"), NULL_BYTE, DAY_TIMESTAMP_VALUE), "cf_arr-cf_born_tsd", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("abc"), NULL_BYTE, HOUR_TIMESTAMP_VALUE), "cf_arr-cf_born_tsh", "___card___", "", "2");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("abc"), NULL_BYTE, HOUR_TIMESTAMP_VALUE), "cf_arr-cf_born_tsh", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("abc"), NULL_BYTE, MINUTE_TIMESTAMP_VALUE), "cf_arr-cf_born_tsm", "___card___", "", "2");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("abc"), NULL_BYTE, MINUTE_TIMESTAMP_VALUE), "cf_arr-cf_born_tsm", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("abc"), NULL_BYTE, SECOND_TIMESTAMP_VALUE), "cf_arr-cf_born_tss", "___card___", "", "2");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("abc"), NULL_BYTE, SECOND_TIMESTAMP_VALUE), "cf_arr-cf_born_tss", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("abc"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "___card___", "", "2");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("abc"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "___card___", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(M1_FNAME_VALUE, NULL_BYTE, DAY_TIMESTAMP_VALUE), "cf_firstname-cf_born_tsd", "___card___", "", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(M1_FNAME_VALUE, NULL_BYTE, HOUR_TIMESTAMP_VALUE), "cf_firstname-cf_born_tsh", "___card___", "", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(M1_FNAME_VALUE, NULL_BYTE, MINUTE_TIMESTAMP_VALUE), "cf_firstname-cf_born_tsm", "___card___", "", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(M1_FNAME_VALUE, NULL_BYTE, SECOND_TIMESTAMP_VALUE), "cf_firstname-cf_born_tss", "___card___", "", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(M1_FNAME_VALUE, NULL_BYTE, BORN_VALUE), "cf_firstname-cf_born", "___card___", "", "1");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "___card___", "1");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(M2_FNAME_VALUE, NULL_BYTE, DAY_TIMESTAMP_VALUE), "cf_firstname-cf_born_tsd", "___card___", "", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(M2_FNAME_VALUE, NULL_BYTE, DAY_TIMESTAMP_VALUE), "cf_firstname-cf_born_tsd", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(M2_FNAME_VALUE, NULL_BYTE, HOUR_TIMESTAMP_VALUE), "cf_firstname-cf_born_tsh", "___card___", "", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(M2_FNAME_VALUE, NULL_BYTE, HOUR_TIMESTAMP_VALUE), "cf_firstname-cf_born_tsh", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(M2_FNAME_VALUE, NULL_BYTE, MINUTE_TIMESTAMP_VALUE), "cf_firstname-cf_born_tsm", "___card___", "", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(M2_FNAME_VALUE, NULL_BYTE, MINUTE_TIMESTAMP_VALUE), "cf_firstname-cf_born_tsm", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(M2_FNAME_VALUE, NULL_BYTE, SECOND_TIMESTAMP_VALUE), "cf_firstname-cf_born_tss", "___card___", "", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(M2_FNAME_VALUE, NULL_BYTE, SECOND_TIMESTAMP_VALUE), "cf_firstname-cf_born_tss", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(M2_FNAME_VALUE, NULL_BYTE, BORN_VALUE), "cf_firstname-cf_born", "___card___", "", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(M2_FNAME_VALUE, NULL_BYTE, BORN_VALUE), "cf_firstname-cf_born", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), M3_FNAME_VALUE, "cf_firstname", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(M3_FNAME_VALUE, NULL_BYTE, DAY_TIMESTAMP_VALUE), "cf_firstname-cf_born_tsd", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(M3_FNAME_VALUE, NULL_BYTE, HOUR_TIMESTAMP_VALUE), "cf_firstname-cf_born_tsh", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(M3_FNAME_VALUE, NULL_BYTE, MINUTE_TIMESTAMP_VALUE), "cf_firstname-cf_born_tsm", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(M3_FNAME_VALUE, NULL_BYTE, SECOND_TIMESTAMP_VALUE), "cf_firstname-cf_born_tss", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(M3_FNAME_VALUE, NULL_BYTE, BORN_VALUE), "cf_firstname-cf_born", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("def"), NULL_BYTE, DAY_TIMESTAMP_VALUE), "cf_arr-cf_born_tsd", "___card___", "", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("def"), NULL_BYTE, DAY_TIMESTAMP_VALUE), "cf_arr-cf_born_tsd", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("def"), NULL_BYTE, HOUR_TIMESTAMP_VALUE), "cf_arr-cf_born_tsh", "___card___", "", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("def"), NULL_BYTE, HOUR_TIMESTAMP_VALUE), "cf_arr-cf_born_tsh", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("def"), NULL_BYTE, MINUTE_TIMESTAMP_VALUE), "cf_arr-cf_born_tsm", "___card___", "", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("def"), NULL_BYTE, MINUTE_TIMESTAMP_VALUE), "cf_arr-cf_born_tsm", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("def"), NULL_BYTE, SECOND_TIMESTAMP_VALUE), "cf_arr-cf_born_tss", "___card___", "", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("def"), NULL_BYTE, SECOND_TIMESTAMP_VALUE), "cf_arr-cf_born_tss", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("def"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "___card___", "", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("def"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "___card___", "2");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, DAY_TIMESTAMP_VALUE), "cf_arr-cf_born_tsd", "___card___", "", "2");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, DAY_TIMESTAMP_VALUE), "cf_arr-cf_born_tsd", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, DAY_TIMESTAMP_VALUE), "cf_arr-cf_born_tsd", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, HOUR_TIMESTAMP_VALUE), "cf_arr-cf_born_tsh", "___card___", "", "2");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, HOUR_TIMESTAMP_VALUE), "cf_arr-cf_born_tsh", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, HOUR_TIMESTAMP_VALUE), "cf_arr-cf_born_tsh", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, MINUTE_TIMESTAMP_VALUE), "cf_arr-cf_born_tsm", "___card___", "", "2");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, MINUTE_TIMESTAMP_VALUE), "cf_arr-cf_born_tsm", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, MINUTE_TIMESTAMP_VALUE), "cf_arr-cf_born_tsm", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, SECOND_TIMESTAMP_VALUE), "cf_arr-cf_born_tss", "___card___", "", "2");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, SECOND_TIMESTAMP_VALUE), "cf_arr-cf_born_tss", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, SECOND_TIMESTAMP_VALUE), "cf_arr-cf_born_tss", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "___card___", "", "2");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("ghi"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), bytes("jkl"), "cf_arr", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("jkl"), NULL_BYTE, DAY_TIMESTAMP_VALUE), "cf_arr-cf_born_tsd", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("jkl"), NULL_BYTE, HOUR_TIMESTAMP_VALUE), "cf_arr-cf_born_tsh", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("jkl"), NULL_BYTE, MINUTE_TIMESTAMP_VALUE), "cf_arr-cf_born_tsm", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("jkl"), NULL_BYTE, SECOND_TIMESTAMP_VALUE), "cf_arr-cf_born_tss", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("jkl"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("mno"), NULL_BYTE, DAY_TIMESTAMP_VALUE), "cf_arr-cf_born_tsd", "___card___", "", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("mno"), NULL_BYTE, DAY_TIMESTAMP_VALUE), "cf_arr-cf_born_tsd", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("mno"), NULL_BYTE, HOUR_TIMESTAMP_VALUE), "cf_arr-cf_born_tsh", "___card___", "", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("mno"), NULL_BYTE, HOUR_TIMESTAMP_VALUE), "cf_arr-cf_born_tsh", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("mno"), NULL_BYTE, MINUTE_TIMESTAMP_VALUE), "cf_arr-cf_born_tsm", "___card___", "", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("mno"), NULL_BYTE, MINUTE_TIMESTAMP_VALUE), "cf_arr-cf_born_tsm", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("mno"), NULL_BYTE, SECOND_TIMESTAMP_VALUE), "cf_arr-cf_born_tss", "___card___", "", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("mno"), NULL_BYTE, SECOND_TIMESTAMP_VALUE), "cf_arr-cf_born_tss", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("mno"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "___card___", "", "1");
        assertKeyValuePair(iter.next(), Bytes.concat(bytes("mno"), NULL_BYTE, BORN_VALUE), "cf_arr-cf_born", "___card___", "private", "1");
        assertFalse(iter.hasNext());

        scan.close();
    }

    @Test
    public void testTruncateTimestamp()
    {
        Map<TimestampPrecision, Long> expected = ImmutableMap.of(
                SECOND, SECOND_TIMESTAMP,
                MINUTE, MINUTE_TIMESTAMP,
                HOUR, HOUR_TIMESTAMP,
                DAY, DAY_TIMESTAMP);
        assertEquals(getTruncatedTimestamps(TIMESTAMP_VALUE), expected);
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

    private Text text(String v)
    {
        return new Text(SERIALIZER.encode(TimestampType.TIMESTAMP, ts(v).getTime()));
    }

    private static Timestamp ts(String date)
    {
        return new Timestamp(PARSER.parseDateTime(date).getMillis());
    }

    protected ByteBuffer bb(Long v)
    {
        return wrap(SERIALIZER.encode(TIMESTAMP, v));
    }

    protected ByteBuffer bb(byte[] v)
    {
        return wrap(v);
    }

    protected ByteBuffer bb(String v)
    {
        return wrap(v.getBytes(UTF_8));
    }

    private static byte[] encode(Type type, Object v)
    {
        return SERIALIZER.encode(type, v);
    }

    private MetricCacheKey mck(String family, String range)
    {
        return new MetricCacheKey(table.getSchema(), table.getTable(), new Text(family), new Authorizations(), new Range(new Text(range)));
    }

    private MetricCacheKey mck(String family, String range, String... auths)
    {
        return mck(family, range.getBytes(UTF_8), auths);
    }

    private MetricCacheKey mck(String family, byte[] range, String... auths)
    {
        return new MetricCacheKey(table.getSchema(), table.getTable(), new Text(family), new Authorizations(auths), new Range(new Text(range)));
    }

    private static void assertKeyValuePair(Entry<Key, Value> e, byte[] row, String cf, String cq, String value)
    {
        assertEquals(e.getKey().getRow().copyBytes(), row);
        assertEquals(e.getKey().getColumnFamily().toString(), cf);
        assertEquals(e.getKey().getColumnQualifier().toString(), cq);
        assertEquals(e.getValue().toString(), value);
    }

    private static void assertKeyValuePair(Entry<Key, Value> e, byte[] row, String cf, String cq, String cv, String value)
    {
        assertEquals(e.getKey().getRow().copyBytes(), row);
        assertEquals(e.getKey().getColumnFamily().toString(), cf);
        assertEquals(e.getKey().getColumnQualifier().toString(), cq);
        assertEquals(e.getKey().getColumnVisibility().toString(), cv);
        assertEquals(e.getValue().toString(), value);
    }

    private static byte[] bytes(String s)
    {
        return s.getBytes(UTF_8);
    }
}
