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

import com.facebook.presto.accumulo.index.storage.IndexStorage;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class IndexColumn
{
    private final String indexTable;
    private final List<IndexStorage> indexStorageMethods;
    private final List<String> columns;

    @JsonCreator
    public IndexColumn(
            @JsonProperty("indexTable") String indexTable,
            @JsonProperty("indexStorageMethods") List<IndexStorage> indexStorageMethods,
            @JsonProperty("columns") List<String> columns)
    {
        this.indexTable = requireNonNull(indexTable, "indexTable is null");
        this.indexStorageMethods = requireNonNull(indexStorageMethods, "indexStorageMethods is null");
        this.columns = requireNonNull(columns, "column is null");
    }

    @JsonProperty
    public List<String> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public String getIndexTable()
    {
        return indexTable;
    }

    @JsonProperty
    public List<IndexStorage> getIndexStorageMethods()
    {
        return indexStorageMethods;
    }

    @JsonIgnore
    public int getNumColumns()
    {
        return columns.size();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(indexTable, columns);
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

        IndexColumn other = (IndexColumn) obj;
        return Objects.equals(this.indexTable, other.indexTable) &&
                Objects.equals(this.columns, other.columns);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).add("indexTable", indexTable).add("columns", columns).toString();
    }
}
