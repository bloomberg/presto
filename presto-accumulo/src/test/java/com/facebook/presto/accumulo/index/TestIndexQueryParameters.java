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

import com.facebook.presto.accumulo.index.storage.ShardedIndexStorage;
import com.facebook.presto.accumulo.model.AccumuloRange;
import com.facebook.presto.accumulo.model.IndexColumn;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Bytes;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.accumulo.index.Indexer.NULL_BYTE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

public class TestIndexQueryParameters
{
    private static final AccumuloRowSerializer SERIALIZER = new LexicoderRowSerializer();
    private static final byte[] AGE_5 = SERIALIZER.encode(INTEGER, 5);
    private static final byte[] AGE_10 = SERIALIZER.encode(INTEGER, 10);
    private static final byte[] AGE_50 = SERIALIZER.encode(INTEGER, 50);
    private static final byte[] AGE_100 = SERIALIZER.encode(INTEGER, 100);
    private static final byte[] FOO = SERIALIZER.encode(VARCHAR, "foo");
    private static final byte[] BAR = SERIALIZER.encode(VARCHAR, "bar");

    private static final DateTimeFormatter PARSER = ISODateTimeFormat.dateTimeParser();
    private static final byte[] MIN_TIMESTAMP_VALUE = SERIALIZER.encode(TIMESTAMP, PARSER.parseDateTime("2001-08-22T03:04:05.321+0000").getMillis());
    private static final byte[] MAX_TIMESTAMP_VALUE = SERIALIZER.encode(TIMESTAMP, PARSER.parseDateTime("2001-09-22T03:04:05.321+0000").getMillis());

    private static final IndexColumn INDEX_COLUMN = new IndexColumn("foo", ImmutableList.of(), ImmutableList.of("email", "age", "born"));

    @Test
    public void testIndexQueryParameters()
    {
        IndexQueryParameters parameters = new IndexQueryParameters(INDEX_COLUMN);
        assertEquals(parameters.getIndexColumn(), INDEX_COLUMN);

        parameters.appendColumn("cf_email".getBytes(UTF_8), ImmutableList.of(new AccumuloRange(FOO), new AccumuloRange(BAR)), false);
        parameters.appendColumn("cf_age".getBytes(UTF_8), ImmutableList.of(new AccumuloRange(AGE_5, AGE_10), new AccumuloRange(AGE_50, AGE_100)), false);
        parameters.appendColumn("cf_born".getBytes(UTF_8), ImmutableList.of(new AccumuloRange(MIN_TIMESTAMP_VALUE, MAX_TIMESTAMP_VALUE)), false);

        assertEquals(parameters.getIndexFamily(), new Text("cf_email-cf_age-cf_born"));
        assertEquals(parameters.getMetricParameters().asMap().size(), 1);
        assertEquals(parameters.getRanges(), ImmutableList.of(
                new Range(new Text(Bytes.concat(FOO, NULL_BYTE, AGE_5, NULL_BYTE, MIN_TIMESTAMP_VALUE)), new Text(Bytes.concat(FOO, NULL_BYTE, AGE_10, NULL_BYTE, MAX_TIMESTAMP_VALUE))),
                new Range(new Text(Bytes.concat(FOO, NULL_BYTE, AGE_50, NULL_BYTE, MIN_TIMESTAMP_VALUE)), new Text(Bytes.concat(FOO, NULL_BYTE, AGE_100, NULL_BYTE, MAX_TIMESTAMP_VALUE))),
                new Range(new Text(Bytes.concat(BAR, NULL_BYTE, AGE_5, NULL_BYTE, MIN_TIMESTAMP_VALUE)), new Text(Bytes.concat(BAR, NULL_BYTE, AGE_10, NULL_BYTE, MAX_TIMESTAMP_VALUE))),
                new Range(new Text(Bytes.concat(BAR, NULL_BYTE, AGE_50, NULL_BYTE, MIN_TIMESTAMP_VALUE)), new Text(Bytes.concat(BAR, NULL_BYTE, AGE_100, NULL_BYTE, MAX_TIMESTAMP_VALUE)))));
    }

    @Test
    public void testIndexQueryParametersExactValue()
    {
        IndexQueryParameters parameters = new IndexQueryParameters(new IndexColumn("foo", ImmutableList.of(), ImmutableList.of("email")));
        assertEquals(parameters.getIndexColumn(), new IndexColumn("foo", ImmutableList.of(), ImmutableList.of("email")));

        parameters.appendColumn("cf_email".getBytes(UTF_8), ImmutableList.of(new AccumuloRange(FOO)), false);

        assertEquals(parameters.getIndexFamily(), new Text("cf_email"));
        assertEquals(parameters.getMetricParameters().asMap().size(), 1);
        assertEquals(parameters.getRanges(), ImmutableList.of(new Range(new Text(FOO), new Text(Bytes.concat(FOO)))));
        assertEquals(parameters.split(4), ImmutableList.of(parameters));
    }

    @Test
    public void testIndexQueryParametersMultipleExactValues()
    {
        IndexQueryParameters parameters = new IndexQueryParameters(new IndexColumn("foo", ImmutableList.of(), ImmutableList.of("email", "born")));
        assertEquals(parameters.getIndexColumn(), new IndexColumn("foo", ImmutableList.of(), ImmutableList.of("email", "born")));

        parameters.appendColumn("cf_email".getBytes(UTF_8), ImmutableList.of(new AccumuloRange(FOO), new AccumuloRange(BAR)), false);
        parameters.appendColumn("cf_born".getBytes(UTF_8), ImmutableList.of(new AccumuloRange(MIN_TIMESTAMP_VALUE, MIN_TIMESTAMP_VALUE)), false);

        assertEquals(parameters.getIndexFamily(), new Text("cf_email-cf_born"));
        assertEquals(parameters.getMetricParameters().asMap().size(), 1);
        assertEquals(parameters.getRanges(), ImmutableList.of(
                new Range(new Text(Bytes.concat(FOO, NULL_BYTE, MIN_TIMESTAMP_VALUE)), new Text(Bytes.concat(FOO, NULL_BYTE, MIN_TIMESTAMP_VALUE))),
                new Range(new Text(Bytes.concat(BAR, NULL_BYTE, MIN_TIMESTAMP_VALUE)), new Text(Bytes.concat(BAR, NULL_BYTE, MIN_TIMESTAMP_VALUE)))));
        assertEquals(parameters.split(4), ImmutableList.of(parameters));
    }

    @Test
    public void testIndexQueryParametersTruncateTimestamp()
    {
        IndexQueryParameters parameters = new IndexQueryParameters(INDEX_COLUMN);
        assertEquals(parameters.getIndexColumn(), INDEX_COLUMN);

        parameters.appendColumn("cf_email".getBytes(UTF_8), ImmutableList.of(new AccumuloRange(FOO), new AccumuloRange(BAR)), false);
        parameters.appendColumn("cf_age".getBytes(UTF_8), ImmutableList.of(new AccumuloRange(AGE_5, AGE_10), new AccumuloRange(AGE_50, AGE_100)), false);
        parameters.appendColumn("cf_born".getBytes(UTF_8), ImmutableList.of(new AccumuloRange(MIN_TIMESTAMP_VALUE, MAX_TIMESTAMP_VALUE)), true);

        assertEquals(parameters.getIndexFamily(), new Text("cf_email-cf_age-cf_born"));
        assertEquals(parameters.getRanges(), ImmutableList.of(
                new Range(new Text(Bytes.concat(FOO, NULL_BYTE, AGE_5, NULL_BYTE, MIN_TIMESTAMP_VALUE)), new Text(Bytes.concat(FOO, NULL_BYTE, AGE_10, NULL_BYTE, MAX_TIMESTAMP_VALUE))),
                new Range(new Text(Bytes.concat(FOO, NULL_BYTE, AGE_50, NULL_BYTE, MIN_TIMESTAMP_VALUE)), new Text(Bytes.concat(FOO, NULL_BYTE, AGE_100, NULL_BYTE, MAX_TIMESTAMP_VALUE))),
                new Range(new Text(Bytes.concat(BAR, NULL_BYTE, AGE_5, NULL_BYTE, MIN_TIMESTAMP_VALUE)), new Text(Bytes.concat(BAR, NULL_BYTE, AGE_10, NULL_BYTE, MAX_TIMESTAMP_VALUE))),
                new Range(new Text(Bytes.concat(BAR, NULL_BYTE, AGE_50, NULL_BYTE, MIN_TIMESTAMP_VALUE)), new Text(Bytes.concat(BAR, NULL_BYTE, AGE_100, NULL_BYTE, MAX_TIMESTAMP_VALUE)))));

        assertEquals(parameters.getMetricParameters().asMap().size(), 5);
    }

    @Test
    public void testSplit()
    {
        IndexColumn shardedIndexColumn = new IndexColumn("foo", ImmutableList.of(), ImmutableList.of("born"));

        IndexQueryParameters parameters = new IndexQueryParameters(shardedIndexColumn);
        assertEquals(parameters.getIndexColumn(), shardedIndexColumn);

        parameters.appendColumn("cf_born".getBytes(UTF_8), ImmutableList.of(new AccumuloRange(MIN_TIMESTAMP_VALUE, MAX_TIMESTAMP_VALUE)), true);

        for (Range r : parameters.getRanges()) {
            System.out.println(String.format("%s %s", SERIALIZER.decode(TIMESTAMP, r.getStartKey().getRow().copyBytes()), SERIALIZER.decode(TIMESTAMP, Arrays.copyOfRange(r.getEndKey().getRow().copyBytes(), 0, 9))));
        }

        List<List<Range>> splitParams = parameters.split(4).stream().map(IndexQueryParameters::getRanges).collect(Collectors.toList());
        System.out.println(splitParams);

        for (List<Range> ranges : splitParams) {
            System.out.println("foo");
            for (Range r : ranges) {
                System.out.println(String.format("%s %s", SERIALIZER.decode(TIMESTAMP, r.getStartKey().getRow().copyBytes()), SERIALIZER.decode(TIMESTAMP, Arrays.copyOfRange(r.getEndKey().getRow().copyBytes(), 0, 9))));
            }
        }

        assertEquals(splitParams, ImmutableList.of(
                ImmutableList.of(new Range(new Text(SERIALIZER.encode(TIMESTAMP, 998449445321L)), true, new Text(SERIALIZER.encode(TIMESTAMP, 999119045321L)), true)),
                ImmutableList.of(new Range(new Text(SERIALIZER.encode(TIMESTAMP, 999119045321L)), true, new Text(SERIALIZER.encode(TIMESTAMP, 999788645321L)), true)),
                ImmutableList.of(new Range(new Text(SERIALIZER.encode(TIMESTAMP, 999788645321L)), true, new Text(SERIALIZER.encode(TIMESTAMP, 1000458245321L)), true)),
                ImmutableList.of(new Range(new Text(SERIALIZER.encode(TIMESTAMP, 1000458245321L)), true, new Text(SERIALIZER.encode(TIMESTAMP, 1001127845321L)), true))));
    }

    @Test
    public void testSplitWithShardedIndexStorage()
    {
        ShardedIndexStorage shardedIndexStorage = new ShardedIndexStorage(3);
        IndexColumn shardedIndexColumn = new IndexColumn("foo", ImmutableList.of(shardedIndexStorage), ImmutableList.of("born"));

        IndexQueryParameters parameters = new IndexQueryParameters(shardedIndexColumn);
        assertEquals(parameters.getIndexColumn(), shardedIndexColumn);

        parameters.appendColumn("cf_born".getBytes(UTF_8), ImmutableList.of(new AccumuloRange(MIN_TIMESTAMP_VALUE, MAX_TIMESTAMP_VALUE)), true);

        List<List<Range>> splitParams = parameters.split(4).stream().map(IndexQueryParameters::getRanges).collect(Collectors.toList());
        ImmutableList.Builder<List<Range>> expectedBuilder = ImmutableList.builder();

        List<Pair<Long, Long>> values = ImmutableList.of(
                Pair.of(998449445321L, 999119045321L),
                Pair.of(999119045321L, 999788645321L),
                Pair.of(999788645321L, 1000458245321L),
                Pair.of(1000458245321L, 1001127845321L));

        for (Pair<Long, Long> startEnd : values) {
            ImmutableList.Builder<Range> rangeBuilder = ImmutableList.builder();
            List<byte[]> startShards = shardedIndexStorage.encodeAllShards(SERIALIZER.encode(TIMESTAMP, startEnd.getLeft()));
            List<byte[]> endShards = shardedIndexStorage.encodeAllShards(SERIALIZER.encode(TIMESTAMP, startEnd.getRight()));
            for (int i = 0; i < startShards.size(); ++i) {
                rangeBuilder.add(new Range(new Text(startShards.get(i)), true, new Text(endShards.get(i)), true));
            }
            expectedBuilder.add(rangeBuilder.build());
        }

        assertEquals(splitParams, expectedBuilder.build());
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testNoAppendIndexFamily()
    {
        new IndexQueryParameters(INDEX_COLUMN).getIndexFamily();
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testNoAppendRanges()
    {
        new IndexQueryParameters(INDEX_COLUMN).getRanges();
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testNoAppendMetricParameters()
    {
        new IndexQueryParameters(INDEX_COLUMN).getMetricParameters();
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testAppendAfterTimestampTruncate()
    {
        IndexQueryParameters parameters = new IndexQueryParameters(INDEX_COLUMN);
        parameters.appendColumn("cf_born".getBytes(UTF_8), ImmutableList.of(new AccumuloRange(MIN_TIMESTAMP_VALUE, MAX_TIMESTAMP_VALUE)), true);
        parameters.appendColumn("cf_email".getBytes(UTF_8), ImmutableList.of(new AccumuloRange(FOO), new AccumuloRange(BAR)), false);
    }

    @Test
    public void testAppendTwoTimestamps()
    {
        IndexQueryParameters parameters = new IndexQueryParameters(INDEX_COLUMN);
        assertEquals(parameters.getIndexColumn(), INDEX_COLUMN);

        parameters.appendColumn("cf_born".getBytes(UTF_8), ImmutableList.of(new AccumuloRange(MIN_TIMESTAMP_VALUE, MAX_TIMESTAMP_VALUE)), true);
        parameters.appendColumn("cf_born".getBytes(UTF_8), ImmutableList.of(new AccumuloRange(MIN_TIMESTAMP_VALUE, MAX_TIMESTAMP_VALUE)), true);
    }
}
