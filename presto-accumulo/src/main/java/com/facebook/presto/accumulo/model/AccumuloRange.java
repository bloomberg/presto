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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Bytes;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/**
 * This is a lightweight wrapper for {@link org.apache.accumulo.core.data.Range}
 * that stores the raw byte arrays of the original values.  This is because the constructor
 * of Accumulo's Range changes the byte arrays based on the inclusivity/exclusivity of the values
 * and it becomes bothersome to obtain the original bytes.
 */
public class AccumuloRange
{
    private final byte[] start;
    private final boolean isStartKeyInclusive;
    private final byte[] end;
    private final boolean isEndKeyInclusive;
    private final Range range;

    public AccumuloRange()
    {
        this(null, true, null, true);
    }

    public AccumuloRange(byte[] row)
    {
        this(row, true, row, true);
    }

    public AccumuloRange(byte[] start, byte[] end)
    {
        this(start, true, end, true);
    }

    @JsonCreator
    public AccumuloRange(
            @JsonProperty("start") byte[] start,
            @JsonProperty("isStartKeyInclusive") boolean isStartKeyInclusive,
            @JsonProperty("end") byte[] end,
            @JsonProperty("isEndKeyInclusive") boolean isEndKeyInclusive)
    {
        if (start != null) {
            if (end != null) {
                if (start.length == end.length) {
                    this.start = start;
                    this.end = end;
                }
                else if (start.length < end.length) {
                    byte[] zeroes = new byte[end.length - start.length];
                    Arrays.fill(zeroes, (byte) 0);
                    this.start = Bytes.concat(start, zeroes);
                    this.end = end;
                }
                else {
                    byte[] ffs = new byte[start.length - end.length];
                    Arrays.fill(ffs, (byte) 255);
                    this.start = start;
                    this.end = Bytes.concat(end, ffs);
                }
            }
            else {
                this.start = start;
                this.end = new byte[start.length];
                Arrays.fill(this.end, (byte) 255);
            }
        }
        else {
            if (end != null) {
                this.start = new byte[end.length];
                Arrays.fill(this.start, (byte) 0);
                this.end = end;
            }
            else {
                // both null
                this.start = null;
                this.end = null;
            }
        }

        this.isStartKeyInclusive = isStartKeyInclusive;
        this.isEndKeyInclusive = isEndKeyInclusive;

        this.range = new Range(start != null ? new Text(start) : null, isStartKeyInclusive, end != null ? new Text(end) : null, isEndKeyInclusive);
    }

    public AccumuloRange(Range range)
    {
        requireNonNull(range, "range is null");

        this.start = range.getStartKey() != null ? range.getStartKey().getRow().copyBytes() : null;
        this.isStartKeyInclusive = range.isStartKeyInclusive();
        this.end = range.getEndKey() != null ? range.getEndKey().getRow().copyBytes() : null;
        this.isEndKeyInclusive = range.isEndKeyInclusive();

        this.range = range;
    }

    @JsonIgnore
    public Range getRange()
    {
        return range;
    }

    @JsonProperty
    public byte[] getStart()
    {
        return start;
    }

    @JsonProperty
    public boolean isStartKeyInclusive()
    {
        return isStartKeyInclusive;
    }

    @JsonProperty
    public byte[] getEnd()
    {
        return end;
    }

    @JsonProperty
    public boolean isEndKeyInclusive()
    {
        return isEndKeyInclusive;
    }

    @JsonIgnore
    public boolean isInfiniteStartKey()
    {
        return start == null;
    }

    @JsonIgnore
    public boolean isInfiniteStopKey()
    {
        return end == null;
    }
}
