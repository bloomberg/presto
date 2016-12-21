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
import com.facebook.presto.accumulo.index.metrics.MetricsWriter;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.model.IndexColumn;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.io.Text;

import javax.annotation.concurrent.NotThreadSafe;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.facebook.presto.accumulo.serializers.AccumuloRowSerializer.getBlockFromArray;
import static com.facebook.presto.accumulo.serializers.AccumuloRowSerializer.getBlockFromMap;
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
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final byte UNDERSCORE = '_';

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
     * Index the given mutation, adding mutations to the index and metrics storage
     * <p>
     * Like typical use of a BatchWriter, this method does not flush mutations to the underlying index table.
     * For higher throughput the modifications to the metrics table are tracked in memory and added to the metrics storage when the indexer is flushed or closed.
     *
     * @param mutation Mutation to index
     */
    public void index(Mutation mutation)
            throws MutationsRejectedException
    {
        checkArgument(mutation.getUpdates().size() > 0, "Mutation must have at least one column update");
        checkArgument(mutation.getUpdates().stream().noneMatch(ColumnUpdate::isDeleted), "Mutation must not contain any delete entries. Use Indexer#delete, then index the Mutation");

        // Increment the cardinality for the number of rows in the table
        metricsWriter.incrementRowCount();

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
            index(mutation);
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

            long deleteTimestamp = System.currentTimeMillis();

            // Scan the table to create a list of items to delete the index entries of
            Text text = new Text();
            Text previousRowId = null;
            Multimap<Pair<ByteBuffer, ByteBuffer>, Triple<byte[], Long, byte[]>> deleteEntries = MultimapBuilder.hashKeys().arrayListValues().build();
            for (Entry<Key, Value> entry : scanner) {
                entry.getKey().getRow(text);
                if (previousRowId == null) {
                    metricsWriter.decrementRowCount();
                    previousRowId = new Text(text);
                }
                else if (!previousRowId.equals(text)) {
                    metricsWriter.decrementRowCount();
                    applyUpdate(previousRowId.copyBytes(), deleteEntries, true);
                    previousRowId.set(text);
                    deleteEntries.clear();
                }

                ByteBuffer family = wrap(entry.getKey().getColumnFamily(text).copyBytes());
                ByteBuffer qualifier = wrap(entry.getKey().getColumnQualifier(text).copyBytes());
                byte[] visibility = entry.getKey().getColumnVisibility(text).copyBytes();
                deleteEntries.put(Pair.of(family, qualifier), Triple.of(visibility, deleteTimestamp, entry.getValue().get()));
            }

            // Apply final updates
            if (previousRowId != null && deleteEntries.size() > 0) {
                applyUpdate(previousRowId.copyBytes(), deleteEntries, true);
            }
        }
        finally {
            if (scanner != null) {
                scanner.close();
            }
        }
    }

    private void applyUpdate(byte[] row, Multimap<Pair<ByteBuffer, ByteBuffer>, Triple<byte[], Long, byte[]>> updates, boolean delete)
            throws MutationsRejectedException
    {
        for (IndexColumn column : indexColumns) {
            AccumuloColumnHandle handle = table.getColumn(column.getColumn());

            if (!handle.getFamily().isPresent() || !handle.getQualifier().isPresent()) {
                throw new PrestoException(StandardErrorCode.INVALID_TABLE_PROPERTY, "Row ID column cannot be indexed");
            }

            // If this mutation has an updated column family
            ByteBuffer family = wrap(handle.getFamily().get().getBytes(UTF_8));
            ByteBuffer qualifier = wrap(handle.getQualifier().get().getBytes(UTF_8));
            for (Triple<byte[], Long, byte[]> value : updates.get(Pair.of(family, qualifier))) {
                Type type = handle.getType();
                ColumnVisibility visibility = new ColumnVisibility(value.getLeft());
                ByteBuffer indexFamily = getIndexColumnFamily(family.array(), qualifier.array());

                // If this is an array type, then index each individual element in the array
                if (Types.isArrayType(type)) {
                    Type elementType = Types.getElementType(type);
                    List<?> elements = serializer.decode(type, value.getRight());
                    for (Object element : elements) {
                        if (!delete) {
                            addIndexMutation(wrap(serializer.encode(elementType, element)), indexFamily, row, visibility, truncateTimestamps && elementType == TIMESTAMP);
                        }
                        else {
                            deleteIndexMutation(wrap(serializer.encode(elementType, element)), indexFamily, row, visibility, truncateTimestamps && elementType == TIMESTAMP);
                        }
                    }
                }
                else if (!delete) {
                    addIndexMutation(wrap(value.getRight()), indexFamily, row, visibility, truncateTimestamps && type == TIMESTAMP);
                }
                else {
                    deleteIndexMutation(wrap(value.getRight()), indexFamily, row, visibility, truncateTimestamps && type == TIMESTAMP);
                }
            }
        }
    }

    private void addIndexMutation(ByteBuffer row, ByteBuffer family, byte[] qualifier, ColumnVisibility visibility, boolean truncateTimestamp)
            throws MutationsRejectedException
    {
        addIndexMutation(row, family, qualifier, visibility, System.currentTimeMillis(), truncateTimestamp);
    }

    private void addIndexMutation(ByteBuffer row, ByteBuffer family, byte[] qualifier, ColumnVisibility visibility, long timestamp, boolean truncateTimestamp)
            throws MutationsRejectedException
    {
        // Create the mutation and add it to the batch writer
        Mutation indexMutation = new Mutation(row.array());
        indexMutation.put(family.array(), qualifier, visibility, timestamp, EMPTY_BYTES);
        indexWriter.addMutation(indexMutation);

        // Increment the cardinality metrics for this value of index
        // metrics is a mapping of row ID to column family
        metricsWriter.incrementCardinality(row, family, visibility, truncateTimestamp);
    }

    private void deleteIndexMutation(ByteBuffer row, ByteBuffer family, byte[] qualifier, ColumnVisibility visibility, boolean truncateTimestamp)
            throws MutationsRejectedException
    {
        deleteIndexMutation(row, family, qualifier, visibility, System.currentTimeMillis(), truncateTimestamp);
    }

    private void deleteIndexMutation(ByteBuffer row, ByteBuffer family, byte[] qualifier, ColumnVisibility visibility, long timestamp, boolean truncateTimestamp)
            throws MutationsRejectedException
    {
        // Create the mutation and add it to the batch writer
        Mutation indexMutation = new Mutation(row.array());
        indexMutation.putDelete(family.array(), qualifier, visibility, timestamp);
        indexWriter.addMutation(indexMutation);
        metricsWriter.decrementCardinality(row, family, visibility, truncateTimestamp);
    }

    /**
     * Gets the column family of the index table based on the given column family and qualifier.
     *
     * @param columnFamily Presto column family
     * @param columnQualifier Presto column qualifier
     * @return ByteBuffer of the given index column family
     */

    public static ByteBuffer getIndexColumnFamily(byte[] columnFamily, byte[] columnQualifier)
    {
        return wrap(ArrayUtils.addAll(ArrayUtils.add(columnFamily, UNDERSCORE), columnQualifier));
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
            AccumuloColumnHandle columnHandle = table.getColumn(indexColumn.getColumn());
            // Create a Text version of the index column family
            Text indexColumnFamily = new Text(getIndexColumnFamily(columnHandle.getFamily().get().getBytes(UTF_8), columnHandle.getQualifier().get().getBytes(UTF_8)).array());

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
}
