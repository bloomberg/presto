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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nonnull;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class AccumuloColumnHandle
        implements ColumnHandle, Comparable<AccumuloColumnHandle>
{
    private final Optional<String> family;
    private final Optional<String> qualifier;
    private final Type type;
    private final String comment;
    private final String name;
    private final int ordinal;
    private final boolean timestamp;
    private final boolean visibility;
    private final boolean delete;

    @JsonCreator
    public AccumuloColumnHandle(
            @JsonProperty("name") String name,
            @JsonProperty("family") Optional<String> family,
            @JsonProperty("qualifier") Optional<String> qualifier,
            @JsonProperty("type") Type type,
            @JsonProperty("ordinal") int ordinal,
            @JsonProperty("comment") String comment,
            @JsonProperty("timestamp") boolean timestamp,
            @JsonProperty("visibility") boolean visibility,
            @JsonProperty("delete") boolean delete)
    {
        this.name = requireNonNull(name, "name is null");
        this.family = requireNonNull(family, "family is null");
        this.qualifier = requireNonNull(qualifier, "qualifier is null");
        this.type = requireNonNull(type, "type is null");
        this.ordinal = ordinal;
        checkArgument(ordinal >= 0, "ordinal must be >= zero");

        this.comment = requireNonNull(comment, "comment is null");
        this.timestamp = timestamp;
        this.visibility = visibility;
        this.delete = delete;
    }

    public AccumuloColumnHandle(String name,
            Optional<String> family,
            Optional<String> qualifier,
            Type type,
            int ordinal,
            String comment,
            boolean timestamp,
            boolean visibility)
    {
        this(name, family, qualifier, type, ordinal, comment, timestamp, visibility, false);
    }

    public AccumuloColumnHandle(String name,
            Optional<String> family,
            Optional<String> qualifier,
            Type type,
            int ordinal,
            String comment)
    {
        this(name, family, qualifier, type, ordinal, comment, false, false);
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Optional<String> getFamily()
    {
        return family;
    }

    @JsonProperty
    public Optional<String> getQualifier()
    {
        return qualifier;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public int getOrdinal()
    {
        return ordinal;
    }

    @JsonProperty
    public String getComment()
    {
        return comment;
    }

    @JsonProperty
    public boolean isTimestamp()
    {
        return timestamp;
    }

    @JsonProperty
    public boolean isVisibility()
    {
        return visibility;
    }

    @JsonProperty
    public boolean isDelete()
    {
        return delete;
    }

    @JsonIgnore
    public boolean isHidden()
    {
        return isTimestamp() || isVisibility();
    }

    @JsonIgnore
    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(name, type, comment, isHidden());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, family, qualifier, type, ordinal, comment, timestamp, visibility);
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

        AccumuloColumnHandle other = (AccumuloColumnHandle) obj;
        return Objects.equals(this.name, other.name)
                && Objects.equals(this.family, other.family)
                && Objects.equals(this.qualifier, other.qualifier)
                && Objects.equals(this.type, other.type)
                && Objects.equals(this.ordinal, other.ordinal)
                && Objects.equals(this.comment, other.comment)
                && Objects.equals(this.timestamp, other.timestamp)
                && Objects.equals(this.visibility, other.visibility)
                && Objects.equals(this.delete, other.delete);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("columnFamily", family.orElse(null))
                .add("columnQualifier", qualifier.orElse(null))
                .add("type", type)
                .add("ordinal", ordinal)
                .add("comment", comment)
                .add("timestamp", timestamp)
                .add("visibility", visibility)
                .add("delete", delete)
                .toString();
    }

    @Override
    public int compareTo(@Nonnull AccumuloColumnHandle obj)
    {
        return Integer.compare(this.getOrdinal(), obj.getOrdinal());
    }
}
