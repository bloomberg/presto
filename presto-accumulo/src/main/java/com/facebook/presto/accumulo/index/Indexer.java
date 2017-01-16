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

import com.facebook.presto.accumulo.Types;
import com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision;
import com.facebook.presto.accumulo.index.metrics.MetricsWriter;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.model.IndexColumn;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.primitives.Bytes;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.io.Text;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.accumulo.AccumuloErrorCode.UNEXPECTED_ACCUMULO_ERROR;
import static com.facebook.presto.accumulo.index.Indexer.Destination.INDEX;
import static com.facebook.presto.accumulo.index.Indexer.Destination.METRIC;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.DAY;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.HOUR;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.MILLISECOND;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.MINUTE;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.SECOND;
import static com.facebook.presto.accumulo.serializers.AccumuloRowSerializer.getBlockFromArray;
import static com.facebook.presto.accumulo.serializers.AccumuloRowSerializer.getBlockFromMap;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.ByteBuffer.wrap;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * This utility class assists the Presto connector, and external applications,
 * in populating the index table and metrics table for Accumulo-backed Presto tables.
 * <p>
 * This class is totally not thread safe.
 * <p>
 * Metric mutations are aggregated locally and must be flushed manually using {@link MetricsWriter#flush()}.
 * <p>
 * Sample usage of an Indexer:
 * <p>
 * <pre>
 * <code>
 * MultiTableBatchWriter multiTableBatchWriter = conn.createMultiTableBatchWriter(new BatchWriterConfig());
 * BatchWriter tableWriter = multiTableBatchWriter.getBatchWriter(table.getFullTableName());
 * MetricsWriter metricsWriter = table.getMetricsStorageInstance(CONFIG).newWriter(table);
 * Indexer indexer = new Indexer(
 *     userAuths,
 *     table,
 *     multiTableBatchWriter.getBatchWriter(table.getIndexTableName()),
 *     metricsWriter);
 *
 * // Gather the mutations you want to write to the 'normal' data table
 * List&ltMutation&gt; mutationsToNormalTable = // some mutations
 * tableWriter.addMutations(mutationsToNormalTable);
 *
 * // And write them to the indexer as well
 * indexer.index(mutationsToNormalTable)
 *
 * // Flush MetricsWriter (important!!!) and flush using the MTBW
 * metricsWriter.flush();
 * multiTableBatchWriter.flush();
 *
 * // Finished adding all mutations? Make sure you flush metrics before closing the MTBW
 * metricsWriter.flush();
 * multiTableBatchWriter.close();
 * </code>
 * </pre>
 */
@NotThreadSafe
public class Indexer
{
    public static final byte[] EMPTY_BYTE = new byte[0];
    public static final byte[] HYPHEN_BYTE = new byte[] {'-'};
    public static final byte[] NULL_BYTE = new byte[] {'\0'};

    public static final Map<TimestampPrecision, byte[]> TIMESTAMP_CARDINALITY_FAMILIES = ImmutableMap.of(
            MILLISECOND, "".getBytes(UTF_8),
            SECOND, "_tss".getBytes(UTF_8),
            MINUTE, "_tsm".getBytes(UTF_8),
            HOUR, "_tsh".getBytes(UTF_8),
            DAY, "_tsd".getBytes(UTF_8));

    private static final byte UNDERSCORE = '_';
    public static final int WHOLE_ROW_ITERATOR_PRIORITY = 21;

    private final AccumuloTable table;
    private final BatchWriter indexWriter;
    private final Connector connector;
    private final MetricsWriter metricsWriter;
    private final List<IndexColumn> indexColumns;
    private final AccumuloRowSerializer serializer;
    private final boolean truncateTimestamps;

    public Indexer(
            Connector connector,
            AccumuloTable table,
            BatchWriter indexWriter,
            MetricsWriter metricsWriter)
            throws TableNotFoundException
    {
        this.connector = requireNonNull(connector, "connector is null");
        this.table = requireNonNull(table, "connector is null");
        this.indexWriter = requireNonNull(indexWriter, "indexWriter is null");
        this.metricsWriter = requireNonNull(metricsWriter, "metricsWriter is null");

        this.serializer = table.getSerializerInstance();
        this.truncateTimestamps = table.isTruncateTimestamps();

        // Initialize metadata
        indexColumns = table.getParsedIndexColumns();

        // If there are no indexed columns, throw an exception
        if (indexColumns.isEmpty()) {
            throw new PrestoException(NOT_SUPPORTED, "No indexed columns in table metadata. Refusing to index a table with no indexed columns");
        }
    }

    /**
     * Index the given mutation, adding mutations to the index and metrics storage and incrementing the number of rows in the table
     * <p>
     * Like typical use of a BatchWriter, this method does not flush mutations to the underlying index table.
     * For higher throughput the modifications to the metrics table are tracked in memory and added to the metrics storage when the indexer is flushed or closed.
     *
     * @param mutation Mutation to index
     */
    public void index(Mutation mutation)
            throws MutationsRejectedException
    {
        index(mutation, true);
    }

    /**
     * Index the given mutation, adding mutations to the index and metrics storage.
     * This function will reject any mutation that contains any 'delete' updates.
     * Use {@link Indexer#delete(Authorizations, Mutation)} if you need to delete index entries
     * or {@link Indexer#delete(Authorizations, byte[])} if you want to delete an entire now.
     * <p>
     * Like typical use of a BatchWriter, this method does not flush mutations to the underlying index table.
     * For higher throughput the modifications to the metrics table are tracked in memory and added to the metrics storage when the indexer is flushed or closed.
     *
     * @param mutation Mutation to index
     * @param increment True to increment the row count in the metrics table
     */
    public void index(Mutation mutation, boolean increment)
            throws MutationsRejectedException
    {
        checkArgument(mutation.getUpdates().size() > 0, "Mutation must have at least one column update");
        checkArgument(mutation.getUpdates().stream().noneMatch(ColumnUpdate::isDeleted), "Mutation must not contain any delete entries. Use Indexer#delete, then index the Mutation");

        // Increment the cardinality for the number of rows in the table
        if (increment) {
            metricsWriter.incrementRowCount();
        }

        // Convert the list of updates into a data structure we can use for indexing
        Multimap<Pair<ByteBuffer, ByteBuffer>, Triple<byte[], Long, byte[]>> updates = MultimapBuilder.hashKeys().arrayListValues().build();
        for (ColumnUpdate columnUpdate : mutation.getUpdates()) {
            ByteBuffer family = wrap(columnUpdate.getColumnFamily());
            ByteBuffer qualifier = wrap(columnUpdate.getColumnQualifier());
            updates.put(Pair.of(family, qualifier), Triple.of(columnUpdate.getColumnVisibility(), columnUpdate.getTimestamp(), columnUpdate.getValue()));
        }

        applyUpdate(mutation.getRow(), updates, false);
    }

    public void index(Iterable<Mutation> mutations)
            throws MutationsRejectedException
    {
        for (Mutation mutation : mutations) {
            index(mutation, true);
        }
    }

    /**
     * Deprecated.  Converts the given <code>columnUpdates</code> parameter to the new data structure and calls {@link Indexer#update(byte[], Multimap, Authorizations)}
     *
     * @param rowBytes Serialized bytes of the row ID to update
     * @param columnUpdates Map of a Triple containing column family, column qualifier, and NEW column visibility to the new column value.
     * @param auths Authorizations to scan the table for deleting the entries.  For proper deletes, these authorizations must encapsulate whatever the visibility of the existing row is, otherwise you'll have duplicate values
     * @deprecated Use {@link Indexer#update(byte[], Multimap, Authorizations)}
     */
    @Deprecated
    public void update(byte[] rowBytes, Map<Triple<String, String, ColumnVisibility>, Object> columnUpdates, Authorizations auths)
            throws MutationsRejectedException, TableNotFoundException
    {
        Multimap<Pair<ByteBuffer, ByteBuffer>, Pair<ColumnVisibility, Object>> updates = MultimapBuilder.hashKeys().arrayListValues().build();
        columnUpdates.entrySet().forEach(
                update -> updates.put(
                        Pair.of(wrap(update.getKey().getLeft().getBytes(UTF_8)), wrap(update.getKey().getMiddle().getBytes(UTF_8))),
                        Pair.of(update.getKey().getRight(), update.getValue())));
        update(rowBytes, updates, auths);
    }

    /**
     * Update the index value and metrics for the given row ID using the provided column updates.
     * <p>
     * This method uses a Scanner to fetch the existing values of row, applying delete Mutations to
     * the given columns with the visibility they were written with (using the Authorizations to scan
     * the table), and then applying the new updates.
     *
     * @param rowBytes Serialized bytes of the row ID to update
     * @param columnUpdates Multimap of a Pair of the column family/qualifier to all updates for this column containing the visibility and Java Object for this column type
     * @param auths Authorizations to scan the table for deleting the entries.  For proper deletes, these authorizations must encapsulate whatever the visibility of the existing row is, otherwise you'll have duplicate values
     */
    public void update(byte[] rowBytes, Multimap<Pair<ByteBuffer, ByteBuffer>, Pair<ColumnVisibility, Object>> columnUpdates, Authorizations auths)
            throws MutationsRejectedException, TableNotFoundException
    {
        // Delete the column updates
        long deleteTimestamp = System.currentTimeMillis();
        Scanner scanner = null;
        try {
            scanner = connector.createScanner(table.getFullTableName(), auths);
            scanner.setRange(Range.exact(new Text(rowBytes)));

            for (Pair<ByteBuffer, ByteBuffer> update : columnUpdates.keySet()) {
                scanner.fetchColumn(new Text(update.getLeft().array()), new Text(update.getRight().array()));
            }

            // Scan the table to create a list of items to delete the index entries of
            Text text = new Text();
            Multimap<Pair<ByteBuffer, ByteBuffer>, Triple<byte[], Long, byte[]>> deleteEntries = MultimapBuilder.hashKeys().arrayListValues().build();
            for (Entry<Key, Value> entry : scanner) {
                ByteBuffer family = wrap(entry.getKey().getColumnFamily(text).copyBytes());
                ByteBuffer qualifier = wrap(entry.getKey().getColumnQualifier(text).copyBytes());
                byte[] visibility = entry.getKey().getColumnVisibility(text).copyBytes();
                deleteEntries.put(Pair.of(family, qualifier), Triple.of(visibility, deleteTimestamp, entry.getValue().get()));
            }

            applyUpdate(rowBytes, deleteEntries, true);
        }
        finally {
            if (scanner != null) {
                scanner.close();
            }
        }

        // Encode the values of the column updates and gather the entries
        long updateTimestamp = deleteTimestamp + 1;
        Multimap<Pair<ByteBuffer, ByteBuffer>, Triple<byte[], Long, byte[]>> updateEntries = MultimapBuilder.hashKeys().arrayListValues().build();

        for (Entry<Pair<ByteBuffer, ByteBuffer>, Pair<ColumnVisibility, Object>> update : columnUpdates.entries()) {
            AccumuloColumnHandle handle = table.getColumn(new String(update.getKey().getLeft().array(), UTF_8), new String(update.getKey().getRight().array(), UTF_8));

            if (!handle.getFamily().isPresent() || !handle.getQualifier().isPresent()) {
                throw new PrestoException(StandardErrorCode.INVALID_TABLE_PROPERTY, "Row ID column cannot be indexed");
            }

            byte[] value;
            if (Types.isArrayType(handle.getType())) {
                value = serializer.encode(handle.getType(), getBlockFromArray(Types.getElementType(handle.getType()), (List<?>) update.getValue().getRight()));
            }
            else if (Types.isMapType(handle.getType())) {
                value = serializer.encode(handle.getType(), getBlockFromMap(Types.getElementType(handle.getType()), (Map<?, ?>) update.getValue().getRight()));
            }
            else {
                value = serializer.encode(handle.getType(), update.getValue().getRight());
            }

            updateEntries.put(update.getKey(), Triple.of(update.getValue().getLeft().getExpression(), updateTimestamp, value));
        }

        // Apply the update mutations
        applyUpdate(rowBytes, updateEntries, false);
    }

    /**
     * Deletes the index entries associated with the given row ID, as well as decrementing the associated metrics.
     * <p>
     * This method creates a new BatchScanner per call, so use {@link Indexer#delete(Authorizations, Iterable)} if deleting multiple entries.
     * <p>
     * Like typical use of a BatchWriter, this method does not flush mutations to the underlying index table.
     * For higher throughput the modifications to the metrics table are tracked in memory and added to the metrics table when the indexer is flushed or closed.
     *
     * @param auths Authorizations for scanning and deleting index/metric entries
     * @param rowId The row to delete
     */
    public void delete(Authorizations auths, byte[] rowId)
            throws MutationsRejectedException, TableNotFoundException
    {
        delete(auths, ImmutableList.of(rowId));
    }

    /**
     * Deletes the index entries associated with the given row ID, as well as decrementing the associated metrics.
     * <p>
     * This method creates a new BatchScanner per call.
     * <p>
     * Like the typical use of a BatchWriter, this method does not flush mutations to the underlying index table.
     * For higher throughput the modifications to the metrics table are tracked in memory and added to the metrics table when the indexer is flushed or closed.
     *
     * @param auths Authorizations for scanning and deleting index/metric entries
     * @param rowIds The row to delete
     */
    public void delete(Authorizations auths, Iterable<byte[]> rowIds)
            throws MutationsRejectedException, TableNotFoundException
    {
        ImmutableList.Builder<Range> rangeBuilder = ImmutableList.builder();
        rowIds.forEach(x -> rangeBuilder.add(new Range(new Text(x))));
        List<Range> ranges = rangeBuilder.build();

        BatchScanner scanner = null;
        try {
            scanner = connector.createBatchScanner(table.getFullTableName(), auths, 10);
            scanner.setRanges(ranges);

            IteratorSetting setting = new IteratorSetting(WHOLE_ROW_ITERATOR_PRIORITY, WholeRowIterator.class);
            scanner.addScanIterator(setting);

            long deleteTimestamp = System.currentTimeMillis();

            // Scan the table to create a list of items to delete the index entries of
            Text text = new Text();
            for (Entry<Key, Value> row : scanner) {
                byte[] rowId = null;
                Multimap<Pair<ByteBuffer, ByteBuffer>, Triple<byte[], Long, byte[]>> deleteEntries = MultimapBuilder.hashKeys().arrayListValues().build();
                for (Entry<Key, Value> entry : WholeRowIterator.decodeRow(row.getKey(), row.getValue()).entrySet()) {
                    rowId = rowId == null ? entry.getKey().getRow(text).copyBytes() : rowId;
                    ByteBuffer family = wrap(entry.getKey().getColumnFamily(text).copyBytes());
                    ByteBuffer qualifier = wrap(entry.getKey().getColumnQualifier(text).copyBytes());
                    byte[] visibility = entry.getKey().getColumnVisibility(text).copyBytes();
                    deleteEntries.put(Pair.of(family, qualifier), Triple.of(visibility, deleteTimestamp, entry.getValue().get()));
                }
                applyUpdate(rowId, deleteEntries, true);
                metricsWriter.decrementRowCount();
            }
        }
        catch (IOException e) {
            throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "IOException decoding whole row", e);
        }
        finally {
            if (scanner != null) {
                scanner.close();
            }
        }
    }

    /**
     * Deletes the columns within a mutation, as well as decrementing the associated metrics.  The row count is unaffected.
     * To delete an entire row of indexes and decrement the row metric, use {@link Indexer#delete(Authorizations, byte[])}
     * <p>
     * Like the typical use of a BatchWriter, this method does not flush mutations to the underlying index table.
     * For higher throughput the modifications to the metrics table are tracked in memory and added to the metrics table when the indexer is flushed or closed.
     *
     * @param auths Authorizations for scanning and deleting index/metric entries
     * @param mutation The mutation to delete index entries from
     */
    public void delete(Authorizations auths, Mutation mutation)
            throws MutationsRejectedException, TableNotFoundException
    {
        Scanner scanner = null;
        try {
            scanner = connector.createScanner(table.getFullTableName(), auths);
            scanner.setRange(new Range(new Text(mutation.getRow())));

            Text text = new Text();
            Multimap<Pair<ByteBuffer, ByteBuffer>, Triple<byte[], Long, byte[]>> deleteEntries = MultimapBuilder.hashKeys().arrayListValues().build();
            // Scan the table to create a list of items to delete the index entries of
            for (Entry<Key, Value> entry : scanner) {
                entry.getKey().getRow(text);
                ByteBuffer family = wrap(entry.getKey().getColumnFamily(text).copyBytes());
                ByteBuffer qualifier = wrap(entry.getKey().getColumnQualifier(text).copyBytes());

                // If this mutation contains a delete entry for this column
                Optional<ColumnUpdate> deleteUpdate = mutation.getUpdates().stream().filter(update -> wrap(update.getColumnFamily()).equals(family) && wrap(update.getColumnQualifier()).equals(qualifier) && update.isDeleted()).findAny();
                if (deleteUpdate.isPresent()) {
                    byte[] visibility = entry.getKey().getColumnVisibility(text).copyBytes();
                    deleteEntries.put(Pair.of(family, qualifier), Triple.of(visibility, deleteUpdate.get().getTimestamp(), entry.getValue().get()));
                }
            }

            if (!deleteEntries.isEmpty()) {
                applyUpdate(mutation.getRow(), deleteEntries, true);
            }
        }
        finally {
            if (scanner != null) {
                scanner.close();
            }
        }
    }

    public static class IndexValues
    {
        ListMultimap<Pair<ByteBuffer, ByteBuffer>, Pair<Long, byte[]>> columnValues = MultimapBuilder.hashKeys().arrayListValues().build();

        public void addValue(Pair<ByteBuffer, ByteBuffer> column, Pair<Long, byte[]> value)
        {
            columnValues.put(column, value);
        }

        public List<byte[]> getValues(Pair<ByteBuffer, ByteBuffer> column)
        {
            return columnValues.get(column).stream().map(Pair::getRight).collect(Collectors.toList());
        }

        public int size()
        {
            return columnValues.keySet().size();
        }

        public long getTimestamp()
        {
            OptionalLong timestamp = columnValues.values().stream().mapToLong(Pair::getLeft).max();
            if (timestamp.isPresent()) {
                return timestamp.getAsLong();
            }

            throw new PrestoException(StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR, "getTimestamp called with an empty list");
        }
    }

    public enum Destination
    {
        INDEX,
        METRIC
    }

    private void applyUpdate(byte[] row, Multimap<Pair<ByteBuffer, ByteBuffer>, Triple<byte[], Long, byte[]>> updates, boolean delete)
            throws MutationsRejectedException
    {
        // Start stepping through each index we want to create
        for (IndexColumn indexColumn : indexColumns) {
            // We'll first need to gather all of the values together by column visibility
            // This ensures the index entries we create will have the same visibility labels as the mutations
            // A design choice here is to ignore the timestamp and round it up to the highest value when we create the index entries
            Map<ColumnVisibility, IndexValues> visibilityToIndexValues = new HashMap<>();
            byte[] indexFamilyBuilder = new byte[0];
            for (String column : indexColumn.getColumns()) {
                AccumuloColumnHandle handle = table.getColumn(column);

                // Family/qualifier are guaranteed to exist due to pre-check in metadata
                ByteBuffer family = wrap(handle.getFamily().get().getBytes(UTF_8));
                ByteBuffer qualifier = wrap(handle.getQualifier().get().getBytes(UTF_8));
                Pair<ByteBuffer, ByteBuffer> familyQualifierPair = Pair.of(family, qualifier);

                // Append to the index family family
                indexFamilyBuilder = indexFamilyBuilder.length == 0
                        ? getIndexColumnFamily(family.array(), qualifier.array())
                        : Bytes.concat(indexFamilyBuilder, HYPHEN_BYTE, getIndexColumnFamily(family.array(), qualifier.array()));

                // Populate the map of visibility to the entries containing that visibility
                updates.get(familyQualifierPair)
                        .stream()
                        .map(x -> Triple.of(new ColumnVisibility(x.getLeft()), x.getMiddle(), x.getRight())) // Convert byte[] to ColumnVisibility
                        .collect(Collectors.groupingBy(Triple::getLeft)) // Group by Column Visibility
                        .entrySet() // Iterate through each group
                        .forEach(visibilityEntry -> {
                            // Get the index values for this visibility and append the updates
                            IndexValues indexValues = visibilityToIndexValues.computeIfAbsent(visibilityEntry.getKey(), x -> new IndexValues());
                            visibilityEntry.getValue().forEach(valueEntry -> indexValues.addValue(familyQualifierPair, Pair.of(valueEntry.getMiddle(), valueEntry.getRight())));
                        });
            }

            byte[] indexFamily = indexFamilyBuilder;
            // Now that we have gathered all the values, we must ensure the number of values gathered for each column visibility is equal to the expected number
            // If so, we can create the index for this entry
            for (Entry<ColumnVisibility, IndexValues> indexValueEntry : visibilityToIndexValues.entrySet().stream()
                    .filter(x -> indexColumn.getNumColumns() == x.getValue().size()) // remove all values where there aren't enough values
                    .collect(Collectors.toList())) {
                Multimap<Destination, Pair<byte[], byte[]>> indexValues = MultimapBuilder.hashKeys(2).arrayListValues().build();
                ColumnVisibility visibility = indexValueEntry.getKey();
                long timestamp = indexValueEntry.getValue().getTimestamp();
                for (String column : indexColumn.getColumns()) {
                    AccumuloColumnHandle handle = table.getColumn(column);
                    byte[] family = handle.getFamily().get().getBytes(UTF_8);
                    byte[] qualifier = handle.getQualifier().get().getBytes(UTF_8);
                    Type type = handle.getType();
                    for (byte[] value : indexValueEntry.getValue().getValues(Pair.of(wrap(family), wrap(qualifier)))) {
                        if (Types.isArrayType(type)) {
                            Type elementType = Types.getElementType(type);
                            List<?> elements = serializer.decode(type, value);
                            if (indexValues.size() == 0) {
                                for (Object element : elements) {
                                    indexValues.put(INDEX, Pair.of(indexFamily, serializer.encode(elementType, element)));
                                    indexValues.put(METRIC, Pair.of(indexFamily, serializer.encode(elementType, element)));

                                    if (elementType.equals(TIMESTAMP) && truncateTimestamps) {
                                        for (Entry<TimestampPrecision, Long> entry : getTruncatedTimestamps((Long) element).entrySet()) {
                                            indexValues.put(METRIC, Pair.of(Bytes.concat(indexFamily, TIMESTAMP_CARDINALITY_FAMILIES.get(entry.getKey())), serializer.encode(elementType, entry.getValue())));
                                        }
                                    }
                                }
                            }
                            else {
                                Multimap<Destination, Pair<byte[], byte[]>> newIndexValues = MultimapBuilder.hashKeys(2).arrayListValues().build();
                                for (Pair<byte[], byte[]> previousValue : indexValues.get(INDEX)) {
                                    for (Object element : elements) {
                                        newIndexValues.put(INDEX, Pair.of(previousValue.getLeft(), Bytes.concat(previousValue.getRight(), NULL_BYTE, serializer.encode(elementType, element))));
                                    }
                                }

                                for (Pair<byte[], byte[]> previousValue : indexValues.get(METRIC)) {
                                    for (Object element : elements) {
                                        newIndexValues.put(METRIC, Pair.of(previousValue.getLeft(), Bytes.concat(previousValue.getRight(), NULL_BYTE, serializer.encode(elementType, element))));

                                        if (elementType.equals(TIMESTAMP) && truncateTimestamps) {
                                            for (Entry<TimestampPrecision, Long> entry : getTruncatedTimestamps((Long) element).entrySet()) {
                                                newIndexValues.put(METRIC, Pair.of(Bytes.concat(previousValue.getLeft(), TIMESTAMP_CARDINALITY_FAMILIES.get(entry.getKey())), Bytes.concat(previousValue.getRight(), NULL_BYTE, serializer.encode(TIMESTAMP, entry.getValue()))));
                                            }
                                        }
                                    }
                                }
                                indexValues = newIndexValues;
                            }
                        }
                        else {
                            if (indexValues.size() == 0) {
                                indexValues.put(INDEX, Pair.of(indexFamily, value));
                                indexValues.put(METRIC, Pair.of(indexFamily, value));

                                if (type.equals(TIMESTAMP) && truncateTimestamps) {
                                    for (Entry<TimestampPrecision, Long> entry : getTruncatedTimestamps(serializer.decode(TIMESTAMP, value)).entrySet()) {
                                        indexValues.put(METRIC, Pair.of(Bytes.concat(indexFamily, TIMESTAMP_CARDINALITY_FAMILIES.get(entry.getKey())), serializer.encode(TIMESTAMP, entry.getValue())));
                                    }
                                }
                            }
                            else {
                                Multimap<Destination, Pair<byte[], byte[]>> newIndexValues = MultimapBuilder.hashKeys(2).arrayListValues().build();
                                for (Pair<byte[], byte[]> previousValue : indexValues.get(INDEX)) {
                                    newIndexValues.put(INDEX, Pair.of(previousValue.getLeft(), Bytes.concat(previousValue.getRight(), NULL_BYTE, value)));
                                }

                                for (Pair<byte[], byte[]> previousValue : indexValues.get(METRIC)) {
                                    newIndexValues.put(METRIC, Pair.of(previousValue.getLeft(), Bytes.concat(previousValue.getRight(), NULL_BYTE, value)));

                                    if (type.equals(TIMESTAMP) && truncateTimestamps) {
                                        for (Entry<TimestampPrecision, Long> entry : getTruncatedTimestamps(serializer.decode(TIMESTAMP, value)).entrySet()) {
                                            newIndexValues.put(METRIC, Pair.of(Bytes.concat(previousValue.getLeft(), TIMESTAMP_CARDINALITY_FAMILIES.get(entry.getKey())), Bytes.concat(previousValue.getRight(), NULL_BYTE, serializer.encode(TIMESTAMP, entry.getValue()))));
                                        }
                                    }
                                }
                                indexValues = newIndexValues;
                            }
                        }
                    }
                }

                // Now that we have aggregated the values for this index column, add or delete them
                for (Pair<byte[], byte[]> indexValue : indexValues.get(INDEX)) {
                    if (delete) {
                        deleteIndexMutation(wrap(indexValue.getRight()), wrap(indexValue.getLeft()), row, visibility, timestamp);
                    }
                    else {
                        addIndexMutation(wrap(indexValue.getRight()), wrap(indexValue.getLeft()), row, visibility, timestamp);
                    }
                }

                for (Pair<byte[], byte[]> indexValue : indexValues.get(METRIC)) {
                    if (delete) {
                        metricsWriter.decrementCardinality(wrap(indexValue.getRight()), wrap(indexValue.getLeft()), visibility);
                    }
                    else {
                        metricsWriter.incrementCardinality(wrap(indexValue.getRight()), wrap(indexValue.getLeft()), visibility);
                    }
                }
            }
        }
    }

    private void addIndexMutation(ByteBuffer row, ByteBuffer family, byte[] qualifier, ColumnVisibility visibility, long timestamp)
            throws MutationsRejectedException
    {
        // Create the mutation and add it to the batch writer
        Mutation indexMutation = new Mutation(row.array());
        indexMutation.put(family.array(), qualifier, visibility, timestamp, EMPTY_BYTE);
        indexWriter.addMutation(indexMutation);
    }

    private void deleteIndexMutation(ByteBuffer row, ByteBuffer family, byte[] qualifier, ColumnVisibility visibility, long timestamp)
            throws MutationsRejectedException
    {
        // Create the mutation and add it to the batch writer
        Mutation indexMutation = new Mutation(row.array());
        indexMutation.putDelete(family.array(), qualifier, visibility, timestamp);
        indexWriter.addMutation(indexMutation);
    }

    /**
     * Gets the column family of the index table based on the given column family and qualifier.
     *
     * @param columnFamily Presto column family
     * @param columnQualifier Presto column qualifier
     * @return ByteBuffer of the given index column family
     */

    public static byte[] getIndexColumnFamily(byte[] columnFamily, byte[] columnQualifier)
    {
        return ArrayUtils.addAll(ArrayUtils.add(columnFamily, UNDERSCORE), columnQualifier);
    }

    /**
     * Gets a set of locality groups that should be added to the index table (not the metrics table).
     *
     * @param table Table for the locality groups, see AccumuloClient#getTable
     * @return Mapping of locality group to column families in the locality group, 1:1 mapping in
     * this case
     */
    public static Map<String, Set<Text>> getLocalityGroups(AccumuloTable table)
    {
        Map<String, Set<Text>> groups = new HashMap<>();
        for (IndexColumn indexColumn : table.getParsedIndexColumns()) {
            byte[] family = new byte[0];
            for (String column : indexColumn.getColumns()) {
                AccumuloColumnHandle columnHandle = table.getColumn(column);
                byte[] concatFamily = getIndexColumnFamily(columnHandle.getFamily().get().getBytes(UTF_8), columnHandle.getQualifier().get().getBytes(UTF_8));
                family = family.length == 0
                        ? concatFamily
                        : Bytes.concat(family, HYPHEN_BYTE, concatFamily);
            }

            // Create a Text version of the index column family
            Text indexColumnFamily = new Text(family);

            // Add this to the locality groups,
            // it is a 1:1 mapping of locality group to column families
            groups.put(indexColumnFamily.toString(), ImmutableSet.of(indexColumnFamily));
        }
        return groups;
    }

    /**
     * Gets the fully-qualified index table name for the given table.
     *
     * @param schema Schema name
     * @param table Table name
     * @return Qualified index table name
     */
    public static String getIndexTableName(String schema, String table)
    {
        return schema.equals("default") ? table + "_idx" : schema + '.' + table + "_idx";
    }

    /**
     * Gets the fully-qualified index table name for the given table.
     *
     * @param tableName Schema table name
     * @return Qualified index table name
     */
    public static String getIndexTableName(SchemaTableName tableName)
    {
        return getIndexTableName(tableName.getSchemaName(), tableName.getTableName());
    }

    /**
     * Gets a Boolean value indicating if the given Range is an exact value
     *
     * @param range Range to check
     * @return True if exact, false otherwise
     */
    public static boolean isExact(Range range)
    {
        return !range.isInfiniteStartKey() && !range.isInfiniteStopKey()
                && range.getStartKey().followingKey(PartialKey.ROW).equals(range.getEndKey());
    }

    public static Map<TimestampPrecision, Long> getTruncatedTimestamps(long value)
    {
        return ImmutableMap.of(
                DAY, value / 86400000L * 86400000L,
                HOUR, value / 3600000L * 3600000L,
                MINUTE, value / 60000L * 60000L,
                SECOND, value / 1000L * 1000L);
    }

    public static Multimap<TimestampPrecision, Range> splitTimestampRange(Range value)
    {
        requireNonNull(value);

        AccumuloRowSerializer serializer = new LexicoderRowSerializer();

        // Selfishly refusing to split any infinite-ended ranges
        if (value.isInfiniteStopKey() || value.isInfiniteStartKey() || isExact(value)) {
            return ImmutableMultimap.of(MILLISECOND, value);
        }

        Multimap<TimestampPrecision, Range> splitTimestampRange = MultimapBuilder.enumKeys(TimestampPrecision.class).arrayListValues().build();

        Text text = new Text();
        value.getStartKey().getRow(text);
        boolean startKeyInclusive = text.getLength() == 9;
        Timestamp startTime = new Timestamp(serializer.decode(TIMESTAMP, Arrays.copyOfRange(text.getBytes(), 0, 9)));

        value.getEndKey().getRow(text);
        Timestamp endTime = new Timestamp(serializer.decode(TIMESTAMP, Arrays.copyOfRange(text.getBytes(), 0, 9)));
        boolean endKeyInclusive = text.getLength() == 10;
        if (startTime.getTime() + 1000L > endTime.getTime()) {
            return ImmutableMultimap.of(MILLISECOND, value);
        }

        Pair<TimestampPrecision, Timestamp> nextTime = Pair.of(getPrecision(startTime), startTime);
        Pair<TimestampPrecision, Range> previousRange = null;

        switch (nextTime.getLeft()) {
            case MILLISECOND:
                break;
            case SECOND:
                int compare = Long.compare(startTime.getTime() + 1000L, endTime.getTime());
                if (compare < 0) {
                    previousRange = Pair.of(SECOND, new Range(new Text(serializer.encode(TIMESTAMP, startTime.getTime()))));
                }
                break;
            case MINUTE:
                compare = Long.compare(startTime.getTime() + 60000L, endTime.getTime());
                if (compare < 0) {
                    previousRange = Pair.of(MINUTE, new Range(new Text(serializer.encode(TIMESTAMP, startTime.getTime()))));
                }
                else {
                    previousRange = Pair.of(SECOND, new Range(new Text(serializer.encode(TIMESTAMP, startTime.getTime()))));
                }
                break;
            case HOUR:
                compare = Long.compare(startTime.getTime() + 3600000L, endTime.getTime());
                if (compare < 0) {
                    previousRange = Pair.of(HOUR, new Range(new Text(serializer.encode(TIMESTAMP, startTime.getTime()))));
                }
                else {
                    previousRange = Pair.of(MINUTE, new Range(new Text(serializer.encode(TIMESTAMP, startTime.getTime()))));
                }
                break;
            case DAY:
                compare = Long.compare(startTime.getTime() + 86400000L, endTime.getTime());
                if (compare < 0) {
                    previousRange = Pair.of(DAY, new Range(new Text(serializer.encode(TIMESTAMP, startTime.getTime()))));
                }
                else {
                    previousRange = Pair.of(HOUR, new Range(new Text(serializer.encode(TIMESTAMP, startTime.getTime()))));
                }
                break;
        }

        boolean cont = true;
        do {
            Pair<TimestampPrecision, Timestamp> prevTime = nextTime;
            nextTime = getNextTimestamp(prevTime.getRight(), endTime);

            Pair<TimestampPrecision, Range> nextRange;
            if (prevTime.getLeft() == MILLISECOND && nextTime.getLeft() == MILLISECOND) {
                nextRange = Pair.of(MILLISECOND, new Range(new Text(serializer.encode(TIMESTAMP, prevTime.getRight().getTime())), startKeyInclusive, new Text(serializer.encode(TIMESTAMP, nextTime.getRight().getTime())), true));
            }
            else if (nextTime.getLeft() == MILLISECOND) {
                Pair<TimestampPrecision, Timestamp> followingTime = getNextTimestamp(nextTime.getRight(), endTime);
                nextRange = Pair.of(MILLISECOND, new Range(new Text(serializer.encode(TIMESTAMP, nextTime.getRight().getTime())), true, new Text(serializer.encode(TIMESTAMP, followingTime.getRight().getTime())), endKeyInclusive));
                cont = false;
            }
            else {
                nextRange = Pair.of(nextTime.getLeft(), new Range(new Text(serializer.encode(TIMESTAMP, nextTime.getRight().getTime()))));
            }

            // Combine this range into previous range
            if (previousRange != null) {
                if (previousRange.getLeft().equals(nextRange.getLeft())) {
                    previousRange = Pair.of(previousRange.getLeft(), new Range(
                            previousRange.getRight().getStartKey(),
                            previousRange.getRight().isStartKeyInclusive(),
                            nextRange.getRight().getEndKey(),
                            nextRange.getRight().isEndKeyInclusive()));
                }
                else {
                    // Add range to map and roll over
                    splitTimestampRange.put(previousRange.getLeft(), previousRange.getRight());
                    previousRange = nextRange;
                }
            }
            else {
                previousRange = nextRange;
            }
        }
        while (cont && !nextTime.getRight().equals(endTime));

        Timestamp s = new Timestamp(serializer.decode(TIMESTAMP, Arrays.copyOfRange(previousRange.getRight().getStartKey().getRow().getBytes(), 0, 9)));
        splitTimestampRange.put(previousRange.getLeft(), previousRange.getRight());
        return ImmutableMultimap.copyOf(splitTimestampRange);
    }

    private static Pair<TimestampPrecision, Timestamp> getNextTimestamp(Timestamp start, Timestamp end)
    {
        Timestamp nextTimestamp = advanceTimestamp(start, end);
        TimestampPrecision nextPrecision = getPrecision(nextTimestamp);
        if (nextTimestamp.equals(end)) {
            return Pair.of(nextPrecision, nextTimestamp);
        }

        TimestampPrecision followingPrecision = getPrecision(advanceTimestamp(nextTimestamp, end));

        int compare = nextPrecision.compareTo(followingPrecision);
        if (compare == 0) {
            return Pair.of(nextPrecision, nextTimestamp);
        }
        else if (compare < 0) {
            return Pair.of(nextPrecision, nextTimestamp);
        }
        else {
            return Pair.of(followingPrecision, nextTimestamp);
        }
    }

    private static Timestamp advanceTimestamp(Timestamp start, Timestamp end)
    {
        switch (getPrecision(start)) {
            case DAY:
                long time = start.getTime() + 86400000L;
                if (time < end.getTime()) {
                    return new Timestamp(time);
                }
            case HOUR:
                time = start.getTime() + 3600000L;
                if (time < end.getTime()) {
                    return new Timestamp(time);
                }
            case MINUTE:
                time = start.getTime() + 60000L;
                if (time < end.getTime()) {
                    return new Timestamp(time);
                }
            case SECOND:
                time = start.getTime() + 1000L;
                if (time < end.getTime()) {
                    return new Timestamp(time);
                }
            case MILLISECOND:
                if ((start.getTime() - 999L) % 1000 == 0) {
                    return new Timestamp(Math.min(start.getTime() + 1, end.getTime()));
                }
                else {
                    return new Timestamp(Math.min((start.getTime() / 1000L * 1000L) + 999L, end.getTime()));
                }
            default:
                throw new PrestoException(NOT_FOUND, "Unknown precision:" + getPrecision(start));
        }
    }

    private static TimestampPrecision getPrecision(Timestamp time)
    {
        if (time.getTime() % 1000 == 0) {
            if (time.getTime() % 60000L == 0) {
                if (time.getTime() % 3600000L == 0) {
                    if (time.getTime() % 86400000L == 0) {
                        return DAY;
                    }
                    else {
                        return HOUR;
                    }
                }
                else {
                    return MINUTE;
                }
            }
            else {
                return SECOND;
            }
        }
        else {
            return MILLISECOND;
        }
    }
}
