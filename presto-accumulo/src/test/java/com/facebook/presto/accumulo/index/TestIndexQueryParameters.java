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

import com.facebook.presto.accumulo.model.AccumuloRange;
import com.facebook.presto.accumulo.model.IndexColumn;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Bytes;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.annotations.Test;

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
    private static final byte[] MIN_SECOND_TIMESTAMP_VALUE = SERIALIZER.encode(TIMESTAMP, PARSER.parseDateTime("2001-08-22T03:04:05.000+0000").getMillis());
    private static final byte[] MAX_SECOND_TIMESTAMP_VALUE = SERIALIZER.encode(TIMESTAMP, PARSER.parseDateTime("2001-09-22T03:04:05.000+0000").getMillis());
    private static final byte[] MINUTE_TIMESTAMP_VALUE = SERIALIZER.encode(TIMESTAMP, PARSER.parseDateTime("2001-08-22T03:04:00.000+0000").getMillis());
    private static final byte[] HOUR_TIMESTAMP_VALUE = SERIALIZER.encode(TIMESTAMP, PARSER.parseDateTime("2001-08-22T03:00:00.000+0000").getMillis());
    private static final byte[] DAY_TIMESTAMP_VALUE = SERIALIZER.encode(TIMESTAMP, PARSER.parseDateTime("2001-08-22T00:00:00.000+0000").getMillis());

    private static final IndexColumn INDEX_COLUMN = new IndexColumn(ImmutableList.of("email", "age", "born"));

    @Test
    public void testIndexQueryParameters()
    {
        IndexQueryParameters parameters = new IndexQueryParameters(INDEX_COLUMN);
        assertEquals(parameters.getIndexColumn(), INDEX_COLUMN);

        parameters.appendColumn("cf_email".getBytes(UTF_8), ImmutableList.of(new AccumuloRange(FOO), new AccumuloRange(BAR)), false);

        assertEquals(parameters.getIndexFamily(), new Text("cf_email"));
        assertEquals(parameters.getMetricParameters().asMap().size(), 1);
        assertEquals(parameters.getRanges(), ImmutableList.of(new Range(new Text(FOO)), new Range(new Text(BAR))));

        parameters.appendColumn("cf_age".getBytes(UTF_8), ImmutableList.of(new AccumuloRange(AGE_5, AGE_10), new AccumuloRange(AGE_50, AGE_100)), false);

        assertEquals(parameters.getIndexFamily(), new Text("cf_email-cf_age"));
        assertEquals(parameters.getMetricParameters().asMap().size(), 1);
        assertEquals(parameters.getRanges(), ImmutableList.of(
                new Range(new Text(Bytes.concat(FOO, NULL_BYTE, AGE_5)), new Text(Bytes.concat(FOO, NULL_BYTE, AGE_10))),
                new Range(new Text(Bytes.concat(FOO, NULL_BYTE, AGE_50)), new Text(Bytes.concat(FOO, NULL_BYTE, AGE_100))),
                new Range(new Text(Bytes.concat(BAR, NULL_BYTE, AGE_5)), new Text(Bytes.concat(BAR, NULL_BYTE, AGE_10))),
                new Range(new Text(Bytes.concat(BAR, NULL_BYTE, AGE_50)), new Text(Bytes.concat(BAR, NULL_BYTE, AGE_100)))));

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
    public void testIndexQueryParametersTruncateTimestamp()
    {
        IndexQueryParameters parameters = new IndexQueryParameters(INDEX_COLUMN);
        assertEquals(parameters.getIndexColumn(), INDEX_COLUMN);

        parameters.appendColumn("cf_email".getBytes(UTF_8), ImmutableList.of(new AccumuloRange(FOO), new AccumuloRange(BAR)), false);

        assertEquals(parameters.getIndexFamily(), new Text("cf_email"));
        assertEquals(parameters.getMetricParameters().asMap().size(), 1);
        assertEquals(parameters.getRanges(), ImmutableList.of(new Range(new Text(FOO)), new Range(new Text(BAR))));

        parameters.appendColumn("cf_age".getBytes(UTF_8), ImmutableList.of(new AccumuloRange(AGE_5, AGE_10), new AccumuloRange(AGE_50, AGE_100)), false);

        assertEquals(parameters.getIndexFamily(), new Text("cf_email-cf_age"));
        assertEquals(parameters.getMetricParameters().asMap().size(), 1);
        assertEquals(parameters.getRanges(), ImmutableList.of(
                new Range(new Text(Bytes.concat(FOO, NULL_BYTE, AGE_5)), new Text(Bytes.concat(FOO, NULL_BYTE, AGE_10))),
                new Range(new Text(Bytes.concat(FOO, NULL_BYTE, AGE_50)), new Text(Bytes.concat(FOO, NULL_BYTE, AGE_100))),
                new Range(new Text(Bytes.concat(BAR, NULL_BYTE, AGE_5)), new Text(Bytes.concat(BAR, NULL_BYTE, AGE_10))),
                new Range(new Text(Bytes.concat(BAR, NULL_BYTE, AGE_50)), new Text(Bytes.concat(BAR, NULL_BYTE, AGE_100)))));

        parameters.appendColumn("cf_born".getBytes(UTF_8), ImmutableList.of(new AccumuloRange(MIN_TIMESTAMP_VALUE, MAX_TIMESTAMP_VALUE)), true);

        assertEquals(parameters.getIndexFamily(), new Text("cf_email-cf_age-cf_born"));
        assertEquals(parameters.getRanges(), ImmutableList.of(
                new Range(new Text(Bytes.concat(FOO, NULL_BYTE, AGE_5, NULL_BYTE, MIN_TIMESTAMP_VALUE)), new Text(Bytes.concat(FOO, NULL_BYTE, AGE_10, NULL_BYTE, MAX_TIMESTAMP_VALUE))),
                new Range(new Text(Bytes.concat(FOO, NULL_BYTE, AGE_50, NULL_BYTE, MIN_TIMESTAMP_VALUE)), new Text(Bytes.concat(FOO, NULL_BYTE, AGE_100, NULL_BYTE, MAX_TIMESTAMP_VALUE))),
                new Range(new Text(Bytes.concat(BAR, NULL_BYTE, AGE_5, NULL_BYTE, MIN_TIMESTAMP_VALUE)), new Text(Bytes.concat(BAR, NULL_BYTE, AGE_10, NULL_BYTE, MAX_TIMESTAMP_VALUE))),
                new Range(new Text(Bytes.concat(BAR, NULL_BYTE, AGE_50, NULL_BYTE, MIN_TIMESTAMP_VALUE)), new Text(Bytes.concat(BAR, NULL_BYTE, AGE_100, NULL_BYTE, MAX_TIMESTAMP_VALUE)))));

        assertEquals(parameters.getMetricParameters().asMap().size(), 5);
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

    @Test
    public void testAppendTwoTimestamps()
    {
        IndexQueryParameters parameters = new IndexQueryParameters(INDEX_COLUMN);
        assertEquals(parameters.getIndexColumn(), INDEX_COLUMN);

        parameters.appendColumn("cf_born".getBytes(UTF_8), ImmutableList.of(new AccumuloRange(MIN_TIMESTAMP_VALUE, MAX_TIMESTAMP_VALUE)), true);
        parameters.appendColumn("cf_born".getBytes(UTF_8), ImmutableList.of(new AccumuloRange(MIN_TIMESTAMP_VALUE, MAX_TIMESTAMP_VALUE)), true);
    }
}
