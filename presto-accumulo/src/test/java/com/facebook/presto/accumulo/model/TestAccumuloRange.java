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
package com.facebook.presto.accumulo.model;

import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import com.google.common.primitives.Bytes;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestAccumuloRange
{
    private final JsonCodec<AccumuloRange> codec = JsonCodec.jsonCodec(AccumuloRange.class);

    @Test
    public void testJsonRoundTrip()
    {
        AccumuloRowSerializer serializer = new LexicoderRowSerializer();
        byte[] a = serializer.encode(VARCHAR, "abc");
        byte[] b = serializer.encode(VARCHAR, "d");
        AccumuloRange expected = new AccumuloRange(a, b);
        String json = codec.toJson(expected);
        AccumuloRange actual = codec.fromJson(json);
        assertEquals(actual, expected);
    }

    @Test
    public void testStartNullEndNull()
    {
        byte[] a = null;
        byte[] b = null;
        AccumuloRange range = new AccumuloRange(a, b).getPaddedRange();
        assertEquals(range.getStart(), null);
        assertEquals(range.getEnd(), null);
        assertEquals(range.isStartKeyInclusive(), true);
        assertEquals(range.isEndKeyInclusive(), true);
        assertEquals(range.isInfiniteStartKey(), true);
        assertEquals(range.isInfiniteStopKey(), true);

        range = new AccumuloRange(a, false, b, false).getPaddedRange();
        assertEquals(range.getStart(), null);
        assertEquals(range.getEnd(), null);
        assertEquals(range.isStartKeyInclusive(), false);
        assertEquals(range.isEndKeyInclusive(), false);
        assertEquals(range.isInfiniteStartKey(), true);
        assertEquals(range.isInfiniteStopKey(), true);
    }

    @Test
    public void testStartNotNullEndNull()
    {
        AccumuloRowSerializer serializer = new LexicoderRowSerializer();
        byte[] a = serializer.encode(VARCHAR, "abc");
        byte[] b = null;
        AccumuloRange range = new AccumuloRange(a, b).getPaddedRange();
        assertEquals(range.getStart(), a);
        assertEquals(range.getEnd(), new byte[] {(byte) 0x7f, (byte) 255, (byte) 255});
        assertEquals(range.isStartKeyInclusive(), true);
        assertEquals(range.isEndKeyInclusive(), true);
        assertEquals(range.isInfiniteStartKey(), false);
        assertEquals(range.isInfiniteStopKey(), false);

        range = new AccumuloRange(a, false, b, false).getPaddedRange();
        assertEquals(range.getStart(), a);
        assertEquals(range.getEnd(), new byte[] {(byte) 0x7f, (byte) 255, (byte) 255});
        assertEquals(range.isStartKeyInclusive(), false);
        assertEquals(range.isEndKeyInclusive(), false);
        assertEquals(range.isInfiniteStartKey(), false);
        assertEquals(range.isInfiniteStopKey(), false);
    }

    @Test
    public void testStartNotNullEndNullAgainstRealRange()
    {
        AccumuloRowSerializer serializer = new LexicoderRowSerializer();
        byte[] a = serializer.encode(VARCHAR, "abc");
        byte[] b = null;
        AccumuloRange range = new AccumuloRange(a, b).getPaddedRange();
        assertEquals(range.getStart(), a);
        assertEquals(range.getEnd(), new byte[] {(byte) 0x7f, (byte) 255, (byte) 255});
        assertEquals(range.isStartKeyInclusive(), true);
        assertEquals(range.isEndKeyInclusive(), true);
        assertEquals(range.isInfiniteStartKey(), false);
        assertEquals(range.isInfiniteStopKey(), false);

        range = new AccumuloRange(a, false, b, false).getPaddedRange();
        assertEquals(range.getStart(), a);
        assertEquals(range.getEnd(), new byte[] {(byte) 0x7f, (byte) 255, (byte) 255});
        assertEquals(range.isStartKeyInclusive(), false);
        assertEquals(range.isEndKeyInclusive(), false);
        assertEquals(range.isInfiniteStartKey(), false);
        assertEquals(range.isInfiniteStopKey(), false);
    }

    @Test
    public void testStartNullEndNotNull()
    {
        AccumuloRowSerializer serializer = new LexicoderRowSerializer();
        byte[] a = null;
        byte[] b = serializer.encode(VARCHAR, "def");
        AccumuloRange range = new AccumuloRange(a, b).getPaddedRange();
        assertEquals(range.getStart(), new byte[] {0, 0, 0});
        assertEquals(range.getEnd(), b);
        assertEquals(range.isStartKeyInclusive(), true);
        assertEquals(range.isEndKeyInclusive(), true);
        assertEquals(range.isInfiniteStartKey(), false);
        assertEquals(range.isInfiniteStopKey(), false);

        range = new AccumuloRange(a, false, b, false).getPaddedRange();
        assertEquals(range.getStart(), new byte[] {0, 0, 0});
        assertEquals(range.getEnd(), b);
        assertEquals(range.isStartKeyInclusive(), false);
        assertEquals(range.isEndKeyInclusive(), false);
        assertEquals(range.isInfiniteStartKey(), false);
        assertEquals(range.isInfiniteStopKey(), false);
    }

    @Test
    public void testStartNotNullEndNotNullSameLength()
    {
        AccumuloRowSerializer serializer = new LexicoderRowSerializer();
        byte[] a = serializer.encode(VARCHAR, "abc");
        byte[] b = serializer.encode(VARCHAR, "def");
        AccumuloRange range = new AccumuloRange(a, b).getPaddedRange();
        assertEquals(range.getStart(), a);
        assertEquals(range.getEnd(), b);
        assertEquals(range.isStartKeyInclusive(), true);
        assertEquals(range.isEndKeyInclusive(), true);
        assertEquals(range.isInfiniteStartKey(), false);
        assertEquals(range.isInfiniteStopKey(), false);

        range = new AccumuloRange(a, false, b, false).getPaddedRange();
        assertEquals(range.getStart(), a);
        assertEquals(range.getEnd(), b);
        assertEquals(range.isStartKeyInclusive(), false);
        assertEquals(range.isEndKeyInclusive(), false);
        assertEquals(range.isInfiniteStartKey(), false);
        assertEquals(range.isInfiniteStopKey(), false);
    }

    @Test
    public void testStartNotNullEndNotNullStartDiffLength()
    {
        AccumuloRowSerializer serializer = new LexicoderRowSerializer();
        byte[] a = serializer.encode(VARCHAR, "a");
        byte[] b = serializer.encode(VARCHAR, "def");
        AccumuloRange range = new AccumuloRange(a, b).getPaddedRange();
        assertEquals(range.getStart(), Bytes.concat(a, new byte[] {0, 0}));
        assertEquals(range.getEnd(), b);
        assertEquals(range.isStartKeyInclusive(), true);
        assertEquals(range.isEndKeyInclusive(), true);
        assertEquals(range.isInfiniteStartKey(), false);
        assertEquals(range.isInfiniteStopKey(), false);

        range = new AccumuloRange(a, false, b, false).getPaddedRange();
        assertEquals(range.getStart(), Bytes.concat(a, new byte[] {0, 0}));
        assertEquals(range.getEnd(), b);
        assertEquals(range.isStartKeyInclusive(), false);
        assertEquals(range.isEndKeyInclusive(), false);
        assertEquals(range.isInfiniteStartKey(), false);
        assertEquals(range.isInfiniteStopKey(), false);
    }

    @Test
    public void testStartNotNullEndNotNullEndtDiffLength()
    {
        AccumuloRowSerializer serializer = new LexicoderRowSerializer();
        byte[] a = serializer.encode(VARCHAR, "abc");
        byte[] b = serializer.encode(VARCHAR, "d");
        AccumuloRange range = new AccumuloRange(a, b).getPaddedRange();
        assertEquals(range.getStart(), a);
        assertEquals(range.getEnd(), Bytes.concat(b, new byte[] {(byte) 255, (byte) 255}));
        assertEquals(range.isStartKeyInclusive(), true);
        assertEquals(range.isEndKeyInclusive(), true);
        assertEquals(range.isInfiniteStartKey(), false);
        assertEquals(range.isInfiniteStopKey(), false);

        range = new AccumuloRange(a, false, b, false).getPaddedRange();
        assertEquals(range.getStart(), a);
        assertEquals(range.getEnd(), Bytes.concat(b, new byte[] {(byte) 255, (byte) 255}));
        assertEquals(range.isStartKeyInclusive(), false);
        assertEquals(range.isEndKeyInclusive(), false);
        assertEquals(range.isInfiniteStartKey(), false);
        assertEquals(range.isInfiniteStopKey(), false);
    }
}
