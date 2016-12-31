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

import com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision;
import com.facebook.presto.accumulo.model.AccumuloRange;
import com.facebook.presto.accumulo.model.IndexColumn;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.primitives.Bytes;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.accumulo.index.Indexer.EMPTY_BYTE;
import static com.facebook.presto.accumulo.index.Indexer.HYPHEN_BYTE;
import static com.facebook.presto.accumulo.index.Indexer.NULL_BYTE;
import static com.facebook.presto.accumulo.index.Indexer.TIMESTAMP_CARDINALITY_FAMILIES;
import static com.facebook.presto.accumulo.index.Indexer.splitTimestampRange;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class IndexQueryParameters
{
    private final IndexColumn column;
    private final Text indexFamily = new Text();
    private final List<AccumuloRange> ranges = new ArrayList<>();
    private final Multimap<Text, Range> metricParameters = MultimapBuilder.hashKeys().arrayListValues().build();

    public IndexQueryParameters(IndexColumn column)
    {
        this.column = requireNonNull(column);
    }

    public void appendColumn(byte[] indexFamily, Collection<AccumuloRange> ranges, boolean truncateTimestamp)
    {
        // Append hyphen byte if this is not the first column
        if (this.indexFamily.getLength() > 0) {
            this.indexFamily.append(HYPHEN_BYTE, 0, HYPHEN_BYTE.length);
        }

        // Append the index family
        this.indexFamily.append(indexFamily, 0, indexFamily.length);

        // Early-out if this is the first column
        if (this.ranges.size() == 0) {
            this.ranges.addAll(ranges);
            return;
        }

        List<AccumuloRange> newRanges = new ArrayList<>();
        for (AccumuloRange previousRange : this.ranges) {
            for (AccumuloRange newRange : ranges) {
                // Here, we use an empty string if the start keys for either are empty
                byte[] newStart = Bytes.concat(
                        previousRange.isInfiniteStartKey() ? EMPTY_BYTE : previousRange.getStart(),
                        NULL_BYTE,
                        newRange.isInfiniteStartKey() ? EMPTY_BYTE : newRange.getStart());
                byte[] newEnd = Bytes.concat(
                        previousRange.isInfiniteStopKey() ? EMPTY_BYTE : previousRange.getEnd(),
                        NULL_BYTE,
                        newRange.isInfiniteStopKey() ? EMPTY_BYTE : newRange.getEnd());

                // If both are inclusive, then we can maintain inclusivity, else false
                boolean newStartInclusive = previousRange.isStartKeyInclusive() && newRange.isStartKeyInclusive();
                boolean newEndInclusive = previousRange.isEndKeyInclusive() && newRange.isEndKeyInclusive();

                newRanges.add(new AccumuloRange(newStart, newStartInclusive, newEnd, newEndInclusive));
            }
        }

        if (truncateTimestamp) {
            checkState(metricParameters.size() == 0, "Multiple calls to appendColumn where truncateTimestamp is true");
            addMetricParameters(ranges);
        }

        this.ranges.clear();
        this.ranges.addAll(newRanges);
    }

    private void addMetricParameters(Collection<AccumuloRange> ranges)
    {
        for (AccumuloRange previousRange : this.ranges) {
            for (AccumuloRange newRange : ranges) {
                for (Map.Entry<TimestampPrecision, Collection<Range>> entry : splitTimestampRange(newRange.getRange()).asMap().entrySet()) {
                    // Append the precision family to the index family
                    Text precisionIndexFamily = new Text(this.indexFamily);
                    byte[] precisionFamily = TIMESTAMP_CARDINALITY_FAMILIES.get(entry.getKey());
                    precisionIndexFamily.append(precisionFamily, 0, precisionFamily.length);

                    for (Range precisionRange : entry.getValue()) {
                        // Here, we use an empty string if the start keys for either are empty
                        byte[] newStart = Bytes.concat(
                                previousRange.isInfiniteStartKey() ? EMPTY_BYTE : previousRange.getStart(),
                                NULL_BYTE,
                                precisionRange.isInfiniteStartKey() ? EMPTY_BYTE : Arrays.copyOfRange(precisionRange.getStartKey().getRow().copyBytes(), 0, 9));
                        byte[] newEnd = Bytes.concat(
                                previousRange.isInfiniteStopKey() ? EMPTY_BYTE : previousRange.getEnd(),
                                NULL_BYTE,
                                precisionRange.isInfiniteStopKey() ? EMPTY_BYTE : Arrays.copyOfRange(precisionRange.getEndKey().getRow().copyBytes(), 0, 9));

                        // If both are inclusive, then we can maintain inclusivity, else false
                        boolean newStartInclusive = previousRange.isStartKeyInclusive() && newRange.isStartKeyInclusive();
                        boolean newEndInclusive = previousRange.isEndKeyInclusive() && newRange.isEndKeyInclusive();

                        metricParameters.put(precisionIndexFamily, new Range(new Text(newStart), newStartInclusive, new Text(newEnd), newEndInclusive));
                    }
                }
            }
        }
    }

    public IndexColumn getIndexColumn()
    {
        return column;
    }

    public Text getIndexFamily()
    {
        checkState(indexFamily.getLength() > 0, "Call to getIndexFamily without an append operation");
        return indexFamily;
    }

    public Collection<Range> getRanges()
    {
        checkState(ranges.size() > 0, "Call to getRanges without an append operation");
        return ImmutableList.copyOf(ranges.stream().map(AccumuloRange::getRange).collect(Collectors.toList()));
    }

    public Multimap<Text, Range> getMetricParameters()
    {
        // If we have metric parameters, then a Timestamp column was present and we can return it
        if (metricParameters.size() > 0) {
            return ImmutableMultimap.copyOf(metricParameters);
        }

        // Else, build the metric parameters from the single entry of family -> ranges
        ImmutableMultimap.Builder<Text, Range> builder = ImmutableMultimap.builder();
        builder.putAll(getIndexFamily(), getRanges());
        return builder.build();
    }
}
