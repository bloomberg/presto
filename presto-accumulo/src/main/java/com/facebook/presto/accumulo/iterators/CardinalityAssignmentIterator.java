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
package com.facebook.presto.accumulo.iterators;

import com.facebook.presto.accumulo.index.metrics.AccumuloMetricsStorage;
import com.google.common.primitives.Bytes;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.accumulo.index.metrics.AccumuloMetricsStorage.CARDINALITY_CQ;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class CardinalityAssignmentIterator
        implements SortedKeyValueIterator<Key, Value>
{
    public static final String INDEX_COLUMN_FAMILY = "idxCF";
    private static final Logger LOG = Logger.getLogger(CardinalityAssignmentIterator.class);

    private SortedKeyValueIterator<Key, Value> source;
    private Key key;
    private Value value;
    private boolean localHasTop = true;
    private boolean sourceHasTop = true;
    private Text columnFamily;
    private Text columnFamilyCar;
    private SortedMap<Key, Value> decodedEntries = new TreeMap<>();
    private boolean isFullMajorCompaction = false;

    public void setSource(SortedKeyValueIterator<Key, Value> source)
    {
        this.source = requireNonNull(source, "source is null");
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
            throws IOException
    {
        setSource(source);
        columnFamily = new Text(options.get(INDEX_COLUMN_FAMILY));
        columnFamilyCar = new Text(Bytes.concat(columnFamily.copyBytes(), AccumuloMetricsStorage.CARDINALITY_CF));
        isFullMajorCompaction = env.isFullMajorCompaction();
        LOG.info(format("is full major compaction: %s", isFullMajorCompaction));
    }

    @Override
    public boolean hasTop()
    {
        return localHasTop;
    }

    @Override
    public void next()
            throws IOException
    {
        if (decodedEntries.isEmpty()) {
            if (sourceHasTop) {
                if (isFullMajorCompaction) {
                    decodeTopKeyValueWithRewrite();
                }
                else {
                    decodeTopKeyValue();
                }
            }
            else {
                localHasTop = false;
            }
        }
        else {
            key = decodedEntries.firstKey();
            value = decodedEntries.remove(key);
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
            throws IOException
    {
        source.seek(range, columnFamilies, inclusive);
        sourceHasTop = source.hasTop();
        localHasTop = sourceHasTop;

        if (sourceHasTop) {
            if (isFullMajorCompaction) {
                decodeTopKeyValueWithRewrite();
            }
            else {
                decodeTopKeyValue();
            }
        }
    }

    private void decodeTopKeyValue()
            throws IOException
    {
        checkState(source.hasTop(), "source does not have top");

        for (Entry<Key, Value> entry : WholeRowIterator.decodeRow(source.getTopKey(), source.getTopValue()).entrySet()) {
            decodedEntries.put(entry.getKey(), entry.getValue());
        }

        key = decodedEntries.firstKey();
        value = decodedEntries.remove(this.key);

        source.next();
        sourceHasTop = source.hasTop();
    }

    private void decodeTopKeyValueWithRewrite()
            throws IOException
    {
        checkState(source.hasTop(), "source does not have top");

        LOG.info(format("source: %s -> %s", source.getTopKey(), source.getTopValue()));

        Text rowId = new Text();
        Map<ColumnVisibility, AtomicInteger> visibilityToCardinality = new HashMap<>();
        for (Entry<Key, Value> entry : WholeRowIterator.decodeRow(source.getTopKey(), source.getTopValue()).entrySet()) {
            entry.getKey().getRow(rowId);
            LOG.info(format("decoded: %s -> %s", entry.getKey(), entry.getValue()));
            if (entry.getKey().compareColumnFamily(columnFamily) == 0) {
                // Skip deleted index entries
                if (entry.getKey().isDeleted()) {
                    continue;
                }
                visibilityToCardinality.computeIfAbsent(entry.getKey().getColumnVisibilityParsed(), key -> new AtomicInteger(0)).incrementAndGet();
                decodedEntries.put(entry.getKey(), entry.getValue());
            }
            else if (entry.getKey().compareColumnFamily(columnFamilyCar) != 0) {
                // skip the cardinality entry, but maintain all others
                decodedEntries.put(entry.getKey(), entry.getValue());
            }
        }

        visibilityToCardinality.forEach((visibility, cardinality) -> {
            Key cardinalityKey = new Key(rowId, columnFamilyCar, new Text(CARDINALITY_CQ), visibility, System.currentTimeMillis());
            decodedEntries.put(cardinalityKey, new Value(cardinality.toString().getBytes(UTF_8)));
        });

        key = decodedEntries.firstKey();
        value = decodedEntries.remove(this.key);

        source.next();
        sourceHasTop = source.hasTop();
    }

    @Override
    public Key getTopKey()
    {
        return this.key;
    }

    @Override
    public Value getTopValue()
    {
        return this.value;
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env)
    {
        CardinalityAssignmentIterator iterator = new CardinalityAssignmentIterator();
        iterator.setSource(source.deepCopy(env));
        iterator.columnFamily = new Text(this.columnFamily);
        iterator.columnFamilyCar = new Text(this.columnFamilyCar);
        return iterator;
    }

    public static void setIndexColumnFamily(IteratorSetting setting, String idxCF)
    {
        setting.addOption(INDEX_COLUMN_FAMILY, idxCF);
    }
}
