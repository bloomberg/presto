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
package com.facebook.presto.accumulo.io;

import com.facebook.presto.accumulo.Types;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeUtils;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.FirstEntryInRowIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.accumulo.AccumuloErrorCode.ACCUMULO_TABLE_DNE;
import static com.facebook.presto.accumulo.AccumuloErrorCode.IO_ERROR;
import static com.facebook.presto.accumulo.AccumuloErrorCode.UNEXPECTED_ACCUMULO_ERROR;
import static com.facebook.presto.accumulo.io.PrestoBatchWriter.ROW_ID_COLUMN;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class AccumuloPageSource
        implements UpdatablePageSource
{
    private static final int BATCH_SIZE = 2048;
    private static final Pair<Text, Text> ROW_COLUMN = Pair.of(ROW_ID_COLUMN, ROW_ID_COLUMN);

    private final AccumuloRowSerializer serializer;
    private final BatchScanner scanner;
    private final Iterator<Entry<Key, Value>> iterator;
    private final List<AccumuloColumnHandle> columns;
    private final Map<Pair<Text, Text>, Pair<Integer, Type>> columnToIndexType = new HashMap<>();
    private final Map<Pair<Text, Text>, Integer> columnToTimestampIndex = new HashMap<>();
    private final Map<Pair<Text, Text>, Integer> columnToVisibilityIndex = new HashMap<>();

    private Optional<Integer> rowColumnIndex = Optional.empty();
    private Optional<Type> rowColumnType = Optional.empty();
    private boolean hasNext;

    private Optional<Integer> deleteRowIndex = Optional.empty();
    private Optional<Type> deleteRowType = Optional.empty();
    private final PrestoBatchWriter batchWriter;

    public AccumuloPageSource(
            Connector connector,
            Authorizations auths,
            AccumuloTable table,
            Collection<Range> ranges,
            List<AccumuloColumnHandle> columns)
    {
        requireNonNull(connector, "connector is null");
        requireNonNull(auths, "auths is null");
        requireNonNull(ranges, "table is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.serializer = requireNonNull(table).getSerializerInstance();

        try {
            scanner = connector.createBatchScanner(table.getFullTableName(), auths, 10);
        }
        catch (TableNotFoundException e) {
            throw new PrestoException(ACCUMULO_TABLE_DNE, format("Accumulo table %s does not exist", table.getFullTableName()));
        }

        scanner.setRanges(ranges.isEmpty() ? ImmutableList.of(new Range()) : ranges);
        scanner.addScanIterator(new IteratorSetting(Integer.MAX_VALUE, WholeRowIterator.class));

        if (columns.size() > 0) {
            scanner.fetchColumn(ROW_COLUMN.getLeft(), ROW_COLUMN.getRight());
        }
        else {
            scanner.addScanIterator(new IteratorSetting(Integer.MAX_VALUE - 1, FirstEntryInRowIterator.class));
        }

        for (int i = 0; i < columns.size(); ++i) {
            AccumuloColumnHandle column = columns.get(i);
            if (column.getFamily().isPresent() && column.getQualifier().isPresent()) {
                scanner.fetchColumn(new Text(column.getFamily().get()), new Text(column.getQualifier().get()));

                Pair<Text, Text> pair = Pair.of(new Text(column.getFamily().get()), new Text(column.getQualifier().get()));
                if (!column.isHidden()) {
                    columnToIndexType.put(pair, Pair.of(i, column.getType()));
                }
                else if (column.isTimestamp()) {
                    columnToTimestampIndex.put(pair, i);
                }
                else if (column.isVisibility()) {
                    columnToVisibilityIndex.put(pair, i);
                }
                else {
                    throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, "Column is hidden but not a timestamp or visibility");
                }
            }
            else if (!column.isDelete()) {
                rowColumnIndex = Optional.of(i);
                rowColumnType = Optional.of(column.getType());
            }
            else {
                deleteRowIndex = Optional.of(i);
                deleteRowType = Optional.of(column.getType());
            }
        }

        iterator = scanner.iterator();
        hasNext = iterator.hasNext();

        if (deleteRowType.isPresent()) {
            try {
                batchWriter = new PrestoBatchWriter(connector, auths, table);
            }
            catch (AccumuloException | AccumuloSecurityException e) {
                throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, format("Error opening batch writer for table %s", table.getFullTableName()), e);
            }
            catch (TableNotFoundException e) {
                throw new PrestoException(ACCUMULO_TABLE_DNE, format("Accumulo table %s does not exist", table.getFullTableName()));
            }
        }
        else {
            batchWriter = null;
        }
    }

    @Override
    public Page getNextPage()
    {
        // We first pull the batch into memory so we know how many rows we are going to write
        List<Pair<Key, Value>> rows = new ArrayList<>();
        int numRead = 0;
        while (iterator.hasNext() && numRead < BATCH_SIZE) {
            Entry<Key, Value> rowEntry = iterator.next();
            rows.add(Pair.of(new Key(rowEntry.getKey()), new Value(rowEntry.getValue())));
            ++numRead;
        }

        hasNext = iterator.hasNext();

        // If we aren't requesting any data, return a Page with a number of positions equal to the number of rows consumed
        if (columns.isEmpty()) {
            return new Page(rows.size());
        }

        // Create our block builders, one per column
        BlockBuilder[] blockBuilders = getBlockBuilders(rows.size());

        // An array to track which fields are null
        boolean[] nullFields = new boolean[blockBuilders.length];

        Text row = new Text();
        Text family = new Text();
        Text qualifier = new Text();
        Pair<Text, Text> lookup = Pair.of(family, qualifier);
        for (Pair<Key, Value> rowEntry : rows) {
            // Initialize the null fields array
            Arrays.fill(nullFields, true);

            // Decode the row
            Set<Entry<Key, Value>> decodedRow;
            try {
                decodedRow = WholeRowIterator.decodeRow(rowEntry.getKey(), rowEntry.getValue()).entrySet();
            }
            catch (IOException e) {
                throw new PrestoException(IO_ERROR, "Failed to decode whole row");
            }

            // For each entry in the row
            for (Entry<Key, Value> entry : decodedRow) {
                // Get the column information
                entry.getKey().getColumnFamily(family);
                entry.getKey().getColumnQualifier(qualifier);

                // Set the row column if it is part of the page and it hasn't been set yet (not null)
                if (rowColumnIndex.isPresent() && nullFields[rowColumnIndex.get()]) {
                    entry.getKey().getRow(row);

                    if (rowColumnType.isPresent()) {
                        TypeUtils.writeNativeValue(rowColumnType.get(), blockBuilders[rowColumnIndex.get()], serializer.decode(rowColumnType.get(), row.copyBytes()));
                        nullFields[rowColumnIndex.get()] = false;
                    }
                    else {
                        throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, "Row column type is not set but row column index is set");
                    }
                }

                // Set the delete row column if it is part of the page and it hasn't been set yet (not null)
                if (deleteRowIndex.isPresent() && nullFields[deleteRowIndex.get()]) {
                    entry.getKey().getRow(row);

                    if (deleteRowType.isPresent()) {
                        TypeUtils.writeNativeValue(deleteRowType.get(), blockBuilders[deleteRowIndex.get()], serializer.decode(deleteRowType.get(), row.copyBytes()));
                        nullFields[deleteRowIndex.get()] = false;
                    }
                    else {
                        throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, "Delete row column type is not set but delete row index is set");
                    }
                }

                // Skip the row column as it is sourced from the row ID
                if (lookup.equals(ROW_COLUMN)) {
                    continue;
                }

                // Get the type and index, writing the value of the column
                if (columnToTimestampIndex.containsKey(lookup)) {
                    Integer index = columnToTimestampIndex.get(lookup);
                    TypeUtils.writeNativeValue(TIMESTAMP, blockBuilders[index], entry.getKey().getTimestamp());
                    nullFields[index] = false;
                }

                if (columnToVisibilityIndex.containsKey(lookup)) {
                    Integer index = columnToVisibilityIndex.get(lookup);
                    TypeUtils.writeNativeValue(VARCHAR, blockBuilders[index], new String(entry.getKey().getColumnVisibility().copyBytes(), UTF_8));
                    nullFields[index] = false;
                }

                if (columnToIndexType.containsKey(lookup)) {
                    Pair<Integer, Type> indexType = columnToIndexType.get(lookup);
                    Integer index = indexType.getKey();
                    Type type = indexType.getRight();

                    Object toWrite;
                    if (Types.isArrayType(type)) {
                        toWrite = AccumuloRowSerializer.getBlockFromArray(Types.getElementType(type), serializer.decode(type, entry.getValue().get()));
                    }
                    else if (Types.isRowType(type)) {
                        toWrite = AccumuloRowSerializer.getBlockFromRow(type, serializer.decode(type, entry.getValue().get()));
                    }
                    else if (Types.isMapType(type)) {
                        toWrite = AccumuloRowSerializer.getBlockFromMap(type, serializer.decode(type, entry.getValue().get()));
                    }
                    else {
                        toWrite = serializer.decode(type, entry.getValue().get());
                    }

                    TypeUtils.writeNativeValue(type, blockBuilders[index], toWrite);

                    nullFields[index] = false;
                }
            }

            // Append any nulls
            for (int i = 0; i < nullFields.length; ++i) {
                if (nullFields[i]) {
                    blockBuilders[i].appendNull();
                }
            }
        }

        // Build the blocks and return the page
        Block[] blocks = new Block[blockBuilders.length];
        for (int i = 0; i < blockBuilders.length; ++i) {
            blocks[i] = blockBuilders[i].build();
        }

        return new Page(blocks);
    }

    private BlockBuilder[] getBlockBuilders(int expectedSize)
    {
        BlockBuilder[] builders = new BlockBuilder[columns.size()];
        for (int i = 0; i < columns.size(); ++i) {
            builders[i] = columns.get(i).getType().createBlockBuilder(new BlockBuilderStatus(), expectedSize);
        }
        return builders;
    }

    @Override
    public long getTotalBytes()
    {
        return 0;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return !hasNext;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        scanner.close();
    }

    @Override
    public void deleteRows(Block rowIds)
    {
        if (!deleteRowType.isPresent()) {
            throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, "Delete row type is not set!");
        }

        Type deleteType = deleteRowType.get();

        List<Object> deletes = new ArrayList<>();
        // For each position within the page, i.e. row
        for (int position = 0; position < rowIds.getPositionCount(); ++position) {
            if (rowIds.isNull(position)) {
                continue;
            }

            Object rowId = TypeUtils.readNativeValue(deleteType, rowIds, position);

            if (rowId == null) {
                throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, "Row ID is null");
            }

            Object toWrite;
            if (Types.isArrayType(deleteType)) {
                toWrite = AccumuloRowSerializer.getArrayFromBlock(Types.getElementType(deleteType), (Block) rowId);
            }
            else if (Types.isRowType(deleteType)) {
                toWrite = AccumuloRowSerializer.getRowFromBlock(deleteType, (Block) rowId);
            }
            else if (Types.isMapType(deleteType)) {
                toWrite = AccumuloRowSerializer.getMapFromBlock(deleteType, (Block) rowId);
            }
            else if (deleteType.getJavaType() == Slice.class) {
                Slice slice = (Slice) rowId;
                toWrite = deleteType.equals(VARCHAR) ? slice.toStringUtf8() : slice.getBytes();
            }
            else {
                toWrite = rowId;
            }

            deletes.add(toWrite);
        }

        try {
            batchWriter.deleteRows(deletes);
        }
        catch (MutationsRejectedException e) {
            throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Mutation rejected by server on flush", e);
        }
        catch (TableNotFoundException e) {
            throw new PrestoException(NOT_FOUND, "Table does not exist", e);
        }
        catch (AccumuloException | AccumuloSecurityException e) {
            throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Unknown Accumulo exception on flush", e);
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        try {
            batchWriter.close();
        }
        catch (MutationsRejectedException e) {
            throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Mutation rejected by server on flush", e);
        }
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
        getFutureValue(finish());
    }
}
