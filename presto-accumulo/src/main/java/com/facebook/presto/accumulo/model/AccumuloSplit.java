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

import com.facebook.presto.accumulo.index.IndexQueryParameters;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.accumulo.core.data.Range;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class AccumuloSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final String rowId;
    private final String schema;
    private final String table;
    private final String serializerClassName;
    private final String scanAuthorizations;
    private final List<AccumuloColumnConstraint> constraints;
    private final List<AccumuloRange> ranges;
    private final Collection<AccumuloRange> rowIdRanges;
    private final Optional<IndexQueryParameters> indexQueryParameters;

    @JsonCreator
    public AccumuloSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("rowId") String rowId,
            @JsonProperty("serializerClassName") String serializerClassName,
            @JsonProperty("ranges") List<AccumuloRange> ranges,
            @JsonProperty("rowIdRanges") Collection<AccumuloRange> rowIdRanges,
            @JsonProperty("indexQueryParameters") Optional<IndexQueryParameters> indexQueryParameters,
            @JsonProperty("constraints") List<AccumuloColumnConstraint> constraints,
            @JsonProperty("scanAuthorizations") String scanAuthorizations)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.rowId = requireNonNull(rowId, "rowId is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.serializerClassName = requireNonNull(serializerClassName, "serializerClassName is null");
        this.constraints = ImmutableList.copyOf(requireNonNull(constraints, "constraints is null"));
        this.scanAuthorizations = requireNonNull(scanAuthorizations, "scanAuthorizations is null");
        this.ranges = ImmutableList.copyOf(requireNonNull(ranges, "ranges is null"));
        this.rowIdRanges = ImmutableList.copyOf(requireNonNull(rowIdRanges, "rowIdRanges is null"));
        this.indexQueryParameters = requireNonNull(indexQueryParameters, "indexQueryParameters is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
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
    public String getFullTableName()
    {
        return (this.getSchema().equals("default") ? "" : this.getSchema() + ".") + this.getTable();
    }

    @JsonIgnore
    public String getIndexTableName()
    {
        return getFullTableName() + "_idx";
    }

    @JsonProperty
    public String getSerializerClassName()
    {
        return this.serializerClassName;
    }

    @JsonProperty("ranges")
    public List<AccumuloRange> getWrappedRanges()
    {
        return ranges;
    }

    @JsonProperty
    public Collection<AccumuloRange> getRowIdRanges()
    {
        return rowIdRanges;
    }

    @JsonProperty
    public Optional<IndexQueryParameters> getIndexQueryParameters()
    {
        return indexQueryParameters;
    }

    @JsonIgnore
    public List<Range> getRanges()
    {
        return ranges.stream().map(AccumuloRange::getRange).collect(Collectors.toList());
    }

    @JsonProperty
    public List<AccumuloColumnConstraint> getConstraints()
    {
        return constraints;
    }

    @SuppressWarnings("unchecked")
    @JsonIgnore
    public Class<? extends AccumuloRowSerializer> getSerializerClass()
    {
        try {
            return (Class<? extends AccumuloRowSerializer>) Class.forName(serializerClassName);
        }
        catch (ClassNotFoundException e) {
            throw new PrestoException(NOT_FOUND, "Configured serializer class not found", e);
        }
    }

    @JsonProperty
    public String getScanAuthorizations()
    {
        return scanAuthorizations;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("schema", schema)
                .add("table", table)
                .add("rowId", rowId)
                .add("serializerClassName", serializerClassName)
                .add("numRanges", ranges.size())
                .add("indexQueryParameters", indexQueryParameters)
                .add("constraints", constraints)
                .add("scanAuthorizations", scanAuthorizations)
                .toString();
    }
}
