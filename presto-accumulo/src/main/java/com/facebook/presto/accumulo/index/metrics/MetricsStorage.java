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
package com.facebook.presto.accumulo.index.metrics;

import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.spi.SchemaTableName;
import org.apache.accumulo.core.client.Connector;

import java.nio.ByteBuffer;

import static java.nio.ByteBuffer.wrap;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Abstract class for storing index metrics.  Implementors will be notified when they'll need to create
 * a resource when a Presto table is created, dropped, or renamed.  Also contains functions to factory
 * implementations of {@link MetricsWriter} and {@link MetricsReader}.
 */
public abstract class MetricsStorage
{
    public enum TimestampPrecision
    {
        MILLISECOND,
        SECOND,
        MINUTE,
        HOUR,
        DAY
    }

    public static final ByteBuffer METRICS_TABLE_ROW_ID = wrap("___METRICS_TABLE___".getBytes(UTF_8));
    public static final ByteBuffer METRICS_TABLE_ROWS_COLUMN = wrap("___rows___".getBytes(UTF_8));

    protected final Connector connector;

    /**
     * Gets the default implementation of {@link MetricsStorage}, {@link AccumuloMetricsStorage}
     *
     * @param connector Accumulo Connector
     * @return The default implementation
     */
    public static MetricsStorage getDefault(Connector connector)
    {
        return new AccumuloMetricsStorage(connector);
    }

    public MetricsStorage(Connector connector)
    {
        this.connector = requireNonNull(connector, "connector is null");
    }

    /**
     * Gets a Boolean value indicating whether or not metric storage exists for the given Presto table
     *
     * @param table Presto table
     * @return True if it exists, false otherwise
     */
    public abstract boolean exists(SchemaTableName table);

    /**
     * Create metric storage for the given Presto table.
     * <p>
     * This method must always succeed (barring any exception),
     * i.e. if the metric storage already exists for the table, then do not
     * attempt to re-create it or throw an exception.
     *
     * @param table Presto table
     */
    public abstract void create(AccumuloTable table);

    /**
     * Rename the metric storage for the 'old' Presto table to the new Presto table
     *
     * @param oldTable Previous table info
     * @param newTable New table info
     */
    public abstract void rename(AccumuloTable oldTable, AccumuloTable newTable);

    /**
     * Drop metric storage for the given Presto table
     * <p>
     * This method must always succeed (barring any exception),
     * i.e. if the metric storage does not exist for the table, then do nothing
     * -- do not raise an exception.
     *
     * @param table Presto table
     */
    public abstract void drop(AccumuloTable table);

    /**
     * Creates a new instance of {@link MetricsWriter} for the given table
     *
     * @param table Presto table
     * @return Metrics writer
     */
    public abstract MetricsWriter newWriter(AccumuloTable table);

    /**
     * Creates a new instance of {@link MetricsReader}
     *
     * @return Metrics reader
     */
    public abstract MetricsReader newReader();
}
