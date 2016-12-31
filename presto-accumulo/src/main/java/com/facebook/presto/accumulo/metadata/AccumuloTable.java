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
package com.facebook.presto.accumulo.metadata;

import com.facebook.presto.accumulo.index.Indexer;
import com.facebook.presto.accumulo.index.metrics.AccumuloMetricsStorage;
import com.facebook.presto.accumulo.index.metrics.MetricsStorage;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.model.IndexColumn;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.TimestampType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.accumulo.core.client.Connector;
import org.apache.commons.lang3.tuple.Pair;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * This class encapsulates metadata regarding an Accumulo table in Presto.
 */
public class AccumuloTable
{
    private final boolean external;
    private final String schema;
    private final String serializerClassName;
    private final Optional<String> scanAuthorizations;
    private final List<ColumnMetadata> columnsMetadata;
    private final Optional<String> metricsStorageClass;
    private final boolean truncateTimestamps;
    private final Optional<String> indexColumns;

    private final boolean indexed;
    private final List<AccumuloColumnHandle> columns;
    private final String rowId;
    private final String table;
    private final SchemaTableName schemaTableName;
    private final List<IndexColumn> parsedIndexColumns;
    private final Map<String, AccumuloColumnHandle> columnNameToHandle;
    private final Map<Pair<String, String>, AccumuloColumnHandle> columnFamQualToHandle;

    @JsonCreator
    public AccumuloTable(
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("columns") List<AccumuloColumnHandle> columns,
            @JsonProperty("rowId") String rowId,
            @JsonProperty("external") boolean external,
            @JsonProperty("serializerClassName") String serializerClassName,
            @JsonProperty("scanAuthorizations") Optional<String> scanAuthorizations,
            @JsonProperty("metricsStorageClass") Optional<String> metricsStorageClass,
            @JsonProperty("truncateTimestamps") boolean truncateTimestamps,
            @JsonProperty("indexColumns") Optional<String> indexColumns)
    {
        this.external = requireNonNull(external, "external is null");
        this.rowId = requireNonNull(rowId, "rowId is null");
        checkArgument(columns.stream().filter(column -> column.getName().equals(rowId)).count() == 1, format("Row ID %s is not present in column definition", rowId));

        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns are null"));
        this.serializerClassName = requireNonNull(serializerClassName, "serializerClassName is null");
        this.scanAuthorizations = scanAuthorizations;
        this.metricsStorageClass = requireNonNull(metricsStorageClass, "metricsStorageClass is null");
        this.truncateTimestamps = truncateTimestamps;
        this.indexColumns = requireNonNull(indexColumns, "indexColumns is null");
        this.indexed = indexColumns.isPresent() && indexColumns.get().length() > 0;
        if (indexColumns.isPresent() && !indexColumns.get().isEmpty()) {
            this.parsedIndexColumns =
                    Splitter.on(',').trimResults().omitEmptyStrings().splitToList(indexColumns.get()).stream()
                            .map(indexColumn -> {
                                ImmutableList.Builder<String> builder = ImmutableList.builder();
                                List<String> parsedIndexColumns = Splitter.on(':').trimResults().omitEmptyStrings().splitToList(indexColumn);
                                for (int i = 0; i < parsedIndexColumns.size(); ++i) {
                                    String column = parsedIndexColumns.get(i);
                                    List<AccumuloColumnHandle> columnHandle = this.columns.stream().filter(x -> x.getName().equals(column)).collect(Collectors.toList());
                                    checkArgument(columnHandle.size() == 1, "Specified index column is not defined: " + column);
                                    checkArgument(!column.equals(rowId), "Specified index column cannot be the row ID: " + column);
                                    if (columnHandle.get(0).getType().equals(TimestampType.TIMESTAMP)) {
                                        checkArgument(i + 1 == parsedIndexColumns.size(), "Timestamp-type columns must be at the end of a composite index");
                                    }
                                    builder.add(column);
                                }
                                return new IndexColumn(builder.build());
                            })
                            .collect(Collectors.toList());
        }
        else {
            this.parsedIndexColumns = ImmutableList.of();
        }

        // Extract the ColumnMetadata from the handles for faster access
        ImmutableMap.Builder<String, AccumuloColumnHandle> columnHandleBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<Pair<String, String>, AccumuloColumnHandle> columnFamQualBuilder = ImmutableMap.builder();
        ImmutableList.Builder<ColumnMetadata> columnMetadataBuilder = ImmutableList.builder();
        for (AccumuloColumnHandle column : this.columns) {
            columnMetadataBuilder.add(column.getColumnMetadata());
            columnHandleBuilder.put(column.getName(), column);

            if (column.getFamily().isPresent() && column.getQualifier().isPresent()) {
                columnFamQualBuilder.put(Pair.of(column.getFamily().get(), column.getQualifier().get()), column);
            }
        }

        this.columnsMetadata = columnMetadataBuilder.build();
        this.columnNameToHandle = columnHandleBuilder.build();
        this.columnFamQualToHandle = columnFamQualBuilder.build();
        this.schemaTableName = new SchemaTableName(this.schema, this.table);
    }

    @JsonProperty
    public String getRowId()
    {
        return rowId;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonIgnore
    public String getIndexTableName()
    {
        return Indexer.getIndexTableName(schema, table);
    }

    @JsonIgnore
    public String getFullTableName()
    {
        return getFullTableName(schema, table);
    }

    @JsonProperty
    public List<AccumuloColumnHandle> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public Optional<String> getScanAuthorizations()
    {
        return scanAuthorizations;
    }

    @JsonProperty
    public String getSerializerClassName()
    {
        return serializerClassName;
    }

    @JsonIgnore
    public List<ColumnMetadata> getColumnsMetadata()
    {
        return columnsMetadata;
    }

    @JsonProperty
    public boolean isExternal()
    {
        return external;
    }

    @JsonProperty
    public Optional<String> getMetricsStorageClass()
    {
        return metricsStorageClass;
    }

    @JsonProperty
    public boolean isTruncateTimestamps()
    {
        return truncateTimestamps;
    }

    @JsonProperty
    public Optional<String> getIndexColumns()
    {
        return indexColumns;
    }

    @JsonIgnore
    public AccumuloColumnHandle getColumn(String column)
    {
        AccumuloColumnHandle handle = columnNameToHandle.get(column);
        if (handle == null) {
            throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, "Failed to find column: " + column);
        }
        return handle;
    }

    @JsonIgnore
    public AccumuloColumnHandle getColumn(String family, String qualifier)
    {
        AccumuloColumnHandle handle = columnFamQualToHandle.get(Pair.of(family, qualifier));
        if (handle == null) {
            throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, "Failed to find column for family/qualifier: " + family + ":" + qualifier);
        }

        return handle;
    }

    @JsonIgnore
    public List<IndexColumn> getParsedIndexColumns()
    {
        return parsedIndexColumns;
    }

    @JsonIgnore
    public boolean isIndexed()
    {
        return indexed;
    }

    @JsonIgnore
    public AccumuloRowSerializer getSerializerInstance()
    {
        try {
            return (AccumuloRowSerializer) Class.forName(serializerClassName).getConstructor().newInstance();
        }
        catch (ClassNotFoundException | InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new PrestoException(NOT_FOUND, "Configured serializer class not found", e);
        }
    }

    @JsonIgnore
    public MetricsStorage getMetricsStorageInstance(Connector connector)
    {
        try {
            return (MetricsStorage) Class.forName(metricsStorageClass.orElse(AccumuloMetricsStorage.class.getCanonicalName())).getConstructor(Connector.class).newInstance(connector);
        }
        catch (ClassNotFoundException | IllegalAccessException | InstantiationException | InvocationTargetException | NoSuchMethodException e) {
            e.printStackTrace();
            throw new PrestoException(NOT_FOUND, "Configured metrics storage class not found", e);
        }
    }

    @JsonIgnore
    public static String getFullTableName(String schema, String table)
    {
        return schema.equals("default") ? table : schema + '.' + table;
    }

    @JsonIgnore
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaName", schema)
                .add("tableName", table)
                .add("columns", columns)
                .add("rowIdName", rowId)
                .add("external", external)
                .add("serializerClassName", serializerClassName)
                .add("scanAuthorizations", scanAuthorizations)
                .add("metricsStorageClass", metricsStorageClass)
                .add("truncateTimestamps", truncateTimestamps)
                .toString();
    }
}
