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

import com.facebook.presto.spi.PrestoException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import java.util.Collection;
import java.util.Map;

import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.METRICS_TABLE_ROWS_COLUMN;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.METRICS_TABLE_ROW_ID;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;

/**
 * Abstract class used to read metrics regarding the table index.
 */
public abstract class MetricsReader
        implements AutoCloseable
{
    private static final Range METRICS_TABLE_ROWID_RANGE = new Range(new Text(METRICS_TABLE_ROW_ID.array()));
    private static final Text METRICS_TABLE_ROWS_COLUMN_TEXT = new Text(METRICS_TABLE_ROWS_COLUMN.array());
    private static final Authorizations EMPTY_AUTHS = new Authorizations();

    /**
     * Gets the number of rows for the given table based on the user authorizations
     *
     * @param schema Schema name
     * @param table Table name
     * @return Number of rows in the given table
     */
    public long getNumRowsInTable(String schema, String table)
            throws Exception
    {
        return getCardinality(new MetricCacheKey(schema, table, METRICS_TABLE_ROWS_COLUMN_TEXT, EMPTY_AUTHS, METRICS_TABLE_ROWID_RANGE));
    }

    /**
     * Gets the number of rows in the table where a column contains a specific value,
     * based on the data in the given {@link MetricCacheKey}.
     * <p>
     * Implementations must account for both exact and non-exact Accumulo Range objects.
     *
     * @param key Metric key
     * @return Cardinality of the given value/column combination
     */
    public abstract long getCardinality(MetricCacheKey key)
            throws Exception;

    /**
     * Gets the number of rows in the table where a column contains a specific value,
     * based on the data in the given collection of {@link MetricCacheKey}.  All Range objects
     * in the collection of keys contain <b>exact</b> Accumulo Range objects, i.e. they are a
     * single value (vs. a range of values).
     * <p>
     * Note that the returned map <b>must</b> contain an entry for each key in the given collection.
     *
     * @param keys Collection of metric keys
     * @return A map containing the cardinality
     */
    public abstract Map<MetricCacheKey, Long> getCardinalities(Collection<MetricCacheKey> keys)
            throws Exception;

    /**
     * Gets the number of rows in the table where a column contains a series of values,
     * based on the data in the given collection of {@link MetricCacheKey}.  All Range objects
     * in the collection of keys contain <b>non-exact</b> Accumulo Range objects, i.e. they are a
     * range of value (vs. a single range).
     *
     * @param keys Collection of metric keys
     * @return A map containing the cardinality
     */
    public abstract Long getCardinality(Collection<MetricCacheKey> keys)
            throws Exception;

    /**
     * Gets any key from the given non-empty collection, validating that all other keys
     * in the collection are the same (except for the Range in the key).
     * <p>
     * In order to simplify the implementation of {@link MetricsReader#getCardinalities}, we are making a (safe) assumption
     * that the CacheKeys will all contain the same combination of schema/table/familyw/authorizations.
     *
     * @param keys Non-empty collection of keys
     * @return Any key
     */
    public MetricCacheKey getAnyKey(Collection<MetricCacheKey> keys)
    {
        MetricCacheKey anyKey = keys.stream().findAny().get();
        keys.forEach(k -> {
            if (!k.schema.equals(anyKey.schema) || !k.table.equals(anyKey.table) || !k.family.equals(anyKey.family) || !k.auths.equals(anyKey.auths)) {
                throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, "loadAll called with a non-homogeneous collection of cache keys");
            }
        });
        return anyKey;
    }
}
