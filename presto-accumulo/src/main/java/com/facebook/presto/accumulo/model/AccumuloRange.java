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

import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;

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

    public AccumuloRange(byte[] start, boolean isStartKeyInclusive, byte[] end, boolean isEndKeyInclusive)
    {
        this.start = start;
        this.isStartKeyInclusive = isStartKeyInclusive;
        this.end = end;
        this.isEndKeyInclusive = isEndKeyInclusive;

        this.range = new Range(start != null ? new Text(start) : null, isStartKeyInclusive, end != null ? new Text(end) : null, isEndKeyInclusive);
    }

    public Range getRange()
    {
        return range;
    }

    public byte[] getStart()
    {
        return start;
    }

    public boolean isStartKeyInclusive()
    {
        return isStartKeyInclusive;
    }

    public byte[] getEnd()
    {
        return end;
    }

    public boolean isEndKeyInclusive()
    {
        return isEndKeyInclusive;
    }

    public boolean isInfiniteStartKey()
    {
        return start == null;
    }

    public boolean isInfiniteStopKey()
    {
        return end == null;
    }
}
