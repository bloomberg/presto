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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.primitives.Bytes;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.facebook.presto.accumulo.index.Indexer.EMPTY_BYTE;
import static com.facebook.presto.accumulo.index.Indexer.HYPHEN_BYTE;
import static com.facebook.presto.accumulo.index.Indexer.NULL_BYTE;
import static com.facebook.presto.accumulo.index.Indexer.TIMESTAMP_CARDINALITY_FAMILIES;
import static com.facebook.presto.accumulo.index.Indexer.isExact;
import static com.facebook.presto.accumulo.index.Indexer.splitTimestampRange;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class IndexQueryParameters
{
    private final IndexColumn column;
    private final Multimap<Text, Range> metricParameters;

    private List<AccumuloRange> ranges = null;
    private Optional<Long> cardinality = Optional.empty();
    private Text indexFamily = null;
    private boolean appendedTimestampColumn = false;

    private final List<AppendParameters> appendParameters = new ArrayList<>();

    private class AppendParameters
    {
        byte[] indexFamily;
        List<AccumuloRange> appendRanges;
        boolean truncateTimestamp;

        public AppendParameters(byte[] indexFamily, Collection<AccumuloRange> appendRanges, boolean truncateTimestamp)
        {
            this.indexFamily = Arrays.copyOf(indexFamily, indexFamily.length);
            this.appendRanges = ImmutableList.copyOf(appendRanges);
            this.truncateTimestamp = truncateTimestamp;
        }
    }

    public IndexQueryParameters(IndexColumn column)
    {
        this.column = requireNonNull(column);
        this.metricParameters = MultimapBuilder.hashKeys().arrayListValues().build();
    }

    @JsonCreator
    public IndexQueryParameters(
            @JsonProperty("column") IndexColumn column,
            @JsonProperty("indexFamily") String indexFamily,
            @JsonProperty("ranges") List<AccumuloRange> ranges)
    {
        this.column = requireNonNull(column);
        this.indexFamily = new Text(requireNonNull(indexFamily).getBytes(UTF_8));
        this.ranges = ImmutableList.copyOf(requireNonNull(ranges));
        this.metricParameters = ImmutableMultimap.of();
    }

    public void appendColumn(byte[] indexFamily, Collection<AccumuloRange> appendRanges, boolean truncateTimestamp)
    {
        checkState(this.indexFamily == null && ranges == null, "Cannot append after ranges have been generated");

        if (!truncateTimestamp) {
            checkState(!appendedTimestampColumn, "Cannot append a non-truncated-timestamp-column after a (truncated) timestamp column has been appended");
        }
        else {
            appendedTimestampColumn = true;
        }

        appendParameters.add(new AppendParameters(indexFamily, appendRanges, truncateTimestamp));
    }

    @JsonProperty("column")
    public IndexColumn getIndexColumn()
    {
        return column;
    }

    @JsonIgnore
    public Text getIndexFamily()
    {
        checkState(indexFamily != null || appendParameters.size() > 0, "Call to getIndexFamily without an append operation");
        if (indexFamily == null) {
            generateIndexFamily();
        }

        return indexFamily;
    }

    @JsonProperty("indexFamily")
    public String getIndexFamilyAsString()
    {
        return getIndexFamily().toString();
    }

    @JsonProperty("ranges")
    public List<AccumuloRange> getAccumuloRanges()
    {
        // This method is used by a worker -- they'd be set by Jackson
        checkState(ranges != null, "Ranges are not set!");
        return ImmutableList.copyOf(ranges);
    }

    @JsonIgnore
    public Multimap<Text, Range> getMetricParameters()
    {
        checkState(appendParameters.size() > 0, "Call to getMetricParameters without an append operation");
        if (ranges == null) {
            generateRanges();
        }

        return ImmutableMultimap.copyOf(metricParameters);
    }

    @JsonIgnore
    public List<Range> getRanges()
    {
        checkState(ranges != null || appendParameters.size() > 0, "Call to getRanges without an append operation");
        if (ranges == null) {
            generateRanges();
        }

        return ImmutableList.copyOf(ranges.stream().map(AccumuloRange::getRange).collect(Collectors.toList()));
    }

    @JsonIgnore
    public boolean hasCardinality()
    {
        return cardinality.isPresent();
    }

    @JsonIgnore
    public long getCardinality()
    {
        return cardinality.get();
    }

    @JsonIgnore
    public void setCardinality(long cardinality)
    {
        this.cardinality = Optional.of(cardinality);
    }

    @JsonIgnore
    public List<IndexQueryParameters> split(int n)
    {
        checkArgument(n > 0, "Number of splits must be greater than zero");

        if (ranges == null) {
            generateRanges();
        }

        // If any one of the ranges is an exact range, we won't be able to split it
        // Just return the parameters as-is
        AtomicBoolean split = new AtomicBoolean(true);
        ranges.forEach(range -> split.set(split.get() && !isExact(range.getRange())));

        if (!split.get()) {
            return ImmutableList.of(this);
        }

        List<AccumuloRange> paddedRanges = generatePaddedRanges();

        List<List<AccumuloRange>> distributeRanges = new ArrayList<>(n);
        for (int i = 0; i < n; ++i) {
            distributeRanges.add(new ArrayList<>());
        }

        Iterator<AccumuloRange> rangeIterator = ranges.iterator();
        for (AccumuloRange paddedRange : paddedRanges) {
            List<AccumuloRange> splitRanges = splitRanges(rangeIterator.next(), paddedRange.getStart(), paddedRange.getEnd(), n);
            for (int i = 0; i < splitRanges.size(); ++i) {
                distributeRanges.get(i).add(splitRanges.get(i));
            }
        }

        return ImmutableList.copyOf(distributeRanges.stream().map(ranges -> new IndexQueryParameters(column, getIndexFamilyAsString(), ranges)).collect(Collectors.toList()));
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(column, metricParameters, ranges, cardinality, indexFamily, appendedTimestampColumn, appendParameters);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }

        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        IndexQueryParameters other = (IndexQueryParameters) obj;
        return Objects.equals(this.column, other.column)
                && Objects.equals(this.metricParameters, other.metricParameters)
                && Objects.equals(this.ranges, other.ranges)
                && Objects.equals(this.cardinality, other.cardinality)
                && Objects.equals(this.indexFamily, other.indexFamily)
                && Objects.equals(this.appendedTimestampColumn, other.appendedTimestampColumn)
                && Objects.equals(this.appendParameters, other.appendParameters);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("column", column)
                .add("numMetricParameters", metricParameters.size())
                .add("numRanges", ranges == null ? null : ranges.size())
                .add("cardinality", cardinality)
                .add("indexFamily", indexFamily)
                .add("appendedTimestampColumn", appendedTimestampColumn)
                .add("numAppendParameters", appendParameters.size())
                .toString();
    }

    private void generateIndexFamily()
    {
        indexFamily = new Text();

        for (AppendParameters params : appendParameters) {
            // Append hyphen byte if this is not the first column
            if (indexFamily.getLength() > 0) {
                indexFamily.append(HYPHEN_BYTE, 0, HYPHEN_BYTE.length);
            }

            // Append the index family
            indexFamily.append(params.indexFamily, 0, params.indexFamily.length);
        }
    }

    private void generateRanges()
    {
        ranges = new ArrayList<>();
        for (AppendParameters params : appendParameters) {
            // Add metric parameters *before* appending the index ranges
            addMetricParameters(params.appendRanges, params.truncateTimestamp);

            if (ranges.size() == 0) {
                ranges.addAll(params.appendRanges);
            }
            else {
                // Append the ranges
                List<AccumuloRange> newRanges = new ArrayList<>();
                for (AccumuloRange baseRange : ranges) {
                    for (AccumuloRange appendRange : params.appendRanges) {
                        newRanges.add(appendRange(baseRange, appendRange));
                    }
                }

                ranges.clear();
                ranges.addAll(newRanges);
            }
        }
    }

    private List<AccumuloRange> generatePaddedRanges()
    {
        List<AccumuloRange> ranges = new ArrayList<>();
        for (AppendParameters params : appendParameters) {
            if (ranges.size() == 0) {
                params.appendRanges.forEach(range -> ranges.add(range.getPaddedRange()));
            }
            else {
                // Append the ranges
                List<AccumuloRange> newRanges = new ArrayList<>();
                for (AccumuloRange baseRange : ranges) {
                    for (AccumuloRange appendRange : params.appendRanges) {
                        newRanges.add(appendRange(baseRange, appendRange.getPaddedRange()));
                    }
                }

                ranges.clear();
                ranges.addAll(newRanges);
            }
        }
        return ranges;
    }

    private List<AccumuloRange> splitRanges(AccumuloRange range, byte[] a, byte[] b, int n)
    {
        checkArgument(n > 0, "Number of splits must be greater than zero");

        List<AccumuloRange> splits = new ArrayList<>();

        BigInteger start = new BigInteger(a);
        BigInteger end = new BigInteger(b);
        BigInteger slice = end.subtract(start).divide(BigInteger.valueOf(n));

        byte[] previous = range.getStart();
        for (int i = 1; i < n; ++i) {
            byte[] split = start.add(slice.multiply(BigInteger.valueOf(i))).toByteArray();
            splits.add(new AccumuloRange(previous, split));
            previous = split;
        }
        splits.add(new AccumuloRange(previous, range.getEnd()));

        return splits;
    }

    private AccumuloRange appendRange(AccumuloRange baseRange, AccumuloRange appendRange)
    {
        byte[] newStart;
        if (baseRange.isInfiniteStartKey()) {
            if (appendRange.isInfiniteStartKey()) {
                newStart = EMPTY_BYTE;
            }
            else {
                newStart = Bytes.concat(new byte[appendRange.getStart().length + 1], appendRange.getStart());
            }
        }
        else {
            if (appendRange.isInfiniteStartKey()) {
                newStart = baseRange.getStart();
            }
            else {
                newStart = Bytes.concat(baseRange.getStart(), NULL_BYTE, appendRange.getStart());
            }
        }

        byte[] newEnd;
        if (baseRange.isInfiniteStopKey()) {
            if (appendRange.isInfiniteStopKey()) {
                newEnd = new byte[baseRange.getStart().length + 1 + appendRange.getStart().length];
                Arrays.fill(newEnd, (byte) -1);
            }
            else {
                byte[] fillBytes = new byte[baseRange.getStart().length];
                Arrays.fill(fillBytes, (byte) -1);
                newEnd = Bytes.concat(fillBytes, NULL_BYTE, appendRange.getEnd());
            }
        }
        else {
            if (appendRange.isInfiniteStopKey()) {
                byte[] fillBytes = new byte[appendRange.getStart().length];
                Arrays.fill(fillBytes, (byte) -1);
                newEnd = Bytes.concat(baseRange.getEnd(), NULL_BYTE, fillBytes);
            }
            else {
                newEnd = Bytes.concat(baseRange.getEnd(), NULL_BYTE, appendRange.getEnd());
            }
        }

        // If both are inclusive, then we can maintain inclusivity, else false
        boolean newStartInclusive = baseRange.isStartKeyInclusive() && appendRange.isStartKeyInclusive();
        boolean newEndInclusive = baseRange.isEndKeyInclusive() && appendRange.isEndKeyInclusive();

        return new AccumuloRange(newStart, newStartInclusive, newEnd, newEndInclusive);
    }

    private void addMetricParameters(Collection<AccumuloRange> appendRanges, boolean truncateTimestamp)
    {
        if (indexFamily == null) {
            generateIndexFamily();
        }

        // Clear the parameters to append
        metricParameters.clear();

        // If no ranges are set, then we won't be appending anything to the old ranges
        if (ranges.size() == 0) {
            // Not a timestamp? The metric parameters are the same
            if (!truncateTimestamp) {
                metricParameters.putAll(indexFamily, appendRanges.stream().map(AccumuloRange::getRange).collect(Collectors.toList()));
            }
            else {
                // Otherwise, set the metric parameters
                for (AccumuloRange appendRange : appendRanges) {
                    // We can't rollup open-ended timestamps
                    if (appendRange.isInfiniteStartKey() || appendRange.isInfiniteStopKey()) {
                        // Append the range as-is for millisecond precision and continue to the next one
                        metricParameters.put(indexFamily, appendRange.getRange());
                        continue;
                    }

                    for (Map.Entry<TimestampPrecision, Collection<Range>> entry : splitTimestampRange(appendRange.getRange()).asMap().entrySet()) {
                        // Append the precision family to the index family
                        Text precisionIndexFamily = new Text(indexFamily);
                        byte[] precisionFamily = TIMESTAMP_CARDINALITY_FAMILIES.get(entry.getKey());
                        precisionIndexFamily.append(precisionFamily, 0, precisionFamily.length);

                        for (Range precisionRange : entry.getValue()) {
                            metricParameters.put(precisionIndexFamily, precisionRange);
                        }
                    }
                }
            }
        }
        else {
            if (!truncateTimestamp) {
                for (AccumuloRange previousRange : ranges) {
                    for (AccumuloRange newRange : appendRanges) {
                        metricParameters.put(indexFamily, appendRange(previousRange, newRange).getRange());
                    }
                }
            }
            else {
                appendTimestampMetricParameters(appendRanges);
            }
        }
    }

    private void appendTimestampMetricParameters(Collection<AccumuloRange> appendRanges)
    {
        Text tmp = new Text();
        for (AccumuloRange appendRange : appendRanges) {
            // We can't rollup open-ended timestamps
            if (appendRange.isInfiniteStartKey() || appendRange.isInfiniteStopKey()) {
                // Append the range as-is for millisecond precision and continue to the next one
                metricParameters.put(indexFamily, appendRange.getRange());
                continue;
            }

            for (Map.Entry<TimestampPrecision, Collection<Range>> entry : splitTimestampRange(appendRange.getRange()).asMap().entrySet()) {
                // Append the precision family to the index family
                byte[] precisionFamily = TIMESTAMP_CARDINALITY_FAMILIES.get(entry.getKey());
                Text precisionIndexFamily = new Text(indexFamily);
                precisionIndexFamily.append(precisionFamily, 0, precisionFamily.length);

                for (AccumuloRange baseRange : ranges) {
                    for (Range precisionRange : entry.getValue()) {
                        byte[] precisionStart = precisionRange.isInfiniteStartKey() ? null : Arrays.copyOfRange(precisionRange.getStartKey().getRow(tmp).getBytes(), 0, 9);
                        byte[] precisionEnd = precisionRange.isInfiniteStopKey() ? null : Arrays.copyOfRange(precisionRange.getEndKey().getRow(tmp).getBytes(), 0, 9);

                        byte[] newStart;
                        if (baseRange.isInfiniteStartKey()) {
                            if (precisionStart == null) {
                                newStart = EMPTY_BYTE;
                            }
                            else {
                                newStart = Bytes.concat(new byte[precisionStart.length + 1], precisionStart);
                            }
                        }
                        else {
                            if (precisionRange.isInfiniteStartKey()) {
                                newStart = baseRange.getStart();
                            }
                            else {
                                newStart = Bytes.concat(baseRange.getStart(), NULL_BYTE, precisionStart);
                            }
                        }

                        byte[] newEnd;
                        if (baseRange.isInfiniteStopKey()) {
                            if (precisionEnd == null) {
                                newEnd = new byte[baseRange.getStart().length + 1 + precisionStart.length];
                                Arrays.fill(newEnd, (byte) -1);
                            }
                            else {
                                byte[] fullBytes = new byte[baseRange.getStart().length];
                                Arrays.fill(fullBytes, (byte) -1);
                                newEnd = Bytes.concat(fullBytes, NULL_BYTE, precisionEnd);
                            }
                        }
                        else {
                            if (precisionEnd == null) {
                                byte[] fillBytes = new byte[precisionStart.length];
                                Arrays.fill(fillBytes, (byte) -1);
                                newEnd = Bytes.concat(baseRange.getEnd(), NULL_BYTE, fillBytes);
                            }
                            else {
                                newEnd = Bytes.concat(baseRange.getEnd(), NULL_BYTE, precisionEnd);
                            }
                        }

                        // If both are inclusive, then we can maintain inclusivity, else false
                        boolean newStartInclusive = baseRange.isStartKeyInclusive() && appendRange.isStartKeyInclusive();
                        boolean newEndInclusive = baseRange.isEndKeyInclusive() && appendRange.isEndKeyInclusive();

                        metricParameters.put(precisionIndexFamily, new Range(new Text(newStart), newStartInclusive, new Text(newEnd), newEndInclusive));
                    }
                }
            }
        }
    }
}
