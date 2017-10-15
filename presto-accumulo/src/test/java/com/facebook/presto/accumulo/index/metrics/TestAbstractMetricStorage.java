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

import com.facebook.presto.accumulo.AccumuloQueryRunner;
import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.ArrayType;
import com.google.common.collect.ImmutableList;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.nio.ByteBuffer.wrap;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public abstract class TestAbstractMetricStorage
{
    protected AccumuloConfig config = new AccumuloConfig();
    protected AccumuloTable table;
    protected AccumuloTable table2;

    protected AccumuloColumnHandle c1 = new AccumuloColumnHandle("id", Optional.empty(), Optional.empty(), VARCHAR, 0, "");
    protected AccumuloColumnHandle c2 = new AccumuloColumnHandle("age", Optional.of("cf"), Optional.of("age"), BIGINT, 1, "");
    protected AccumuloColumnHandle c3 = new AccumuloColumnHandle("firstname", Optional.of("cf"), Optional.of("firstname"), VARCHAR, 2, "");
    protected AccumuloColumnHandle c4 = new AccumuloColumnHandle("arr", Optional.of("cf"), Optional.of("arr"), new ArrayType(VARCHAR), 3, "");

    protected LexicoderRowSerializer serializer = new LexicoderRowSerializer();
    protected org.apache.accumulo.core.client.Connector connector;
    protected MetricsStorage storage;

    public abstract MetricsStorage getMetricsStorage(AccumuloConfig config);

    @Test
    public abstract void testCreateTable()
            throws Exception;

    @Test
    public abstract void testCreateTableAlreadyExists()
            throws Exception;

    @Test
    public abstract void testDropTable()
            throws Exception;

    @Test
    public abstract void testDropTableDoesNotExist()
            throws Exception;

    @Test
    public abstract void testDropExternalTable()
            throws Exception;

    @Test
    public abstract void testRenameTable()
            throws Exception;

    @Test(expectedExceptions = PrestoException.class)
    public abstract void testRenameTableDoesNotExist()
            throws Exception;

    @Test(expectedExceptions = PrestoException.class)
    public abstract void testRenameTableNewTableExists()
            throws Exception;

    @Test
    public abstract void testExists()
            throws Exception;

    @BeforeClass
    public void setupClass()
            throws Exception
    {
        connector = AccumuloQueryRunner.getAccumuloConnector();
        storage = getMetricsStorage(config);
        table = new AccumuloTable("default", "abstract_metrics_storage", ImmutableList.of(c1, c2, c3, c4), "id", false, LexicoderRowSerializer.class.getCanonicalName(), null, Optional.of(storage.getClass().getCanonicalName()), false, Optional.of("age,firstname,arr"));
        table2 = new AccumuloTable("default", "abstract_metrics_storage_two", ImmutableList.of(c1, c2, c3, c4), "id", false, LexicoderRowSerializer.class.getCanonicalName(), null, Optional.of(storage.getClass().getCanonicalName()), false, Optional.of("age,firstname,arr"));
    }

    @Test
    public void testIncrementRows()
            throws Exception
    {
        storage.create(table);

        MetricsWriter writer = storage.newWriter(table);
        assertEquals(storage.newReader().getNumRowsInTable(table), 0);
        writer.incrementRowCount();
        writer.flush();
        assertEquals(storage.newReader().getNumRowsInTable(table), 1);
        writer.incrementRowCount();
        writer.flush();
        assertEquals(storage.newReader().getNumRowsInTable(table), 2);
        writer.incrementRowCount();
        writer.flush();
        assertEquals(storage.newReader().getNumRowsInTable(table), 3);
    }

    @Test
    public void testIncrementRowsWithVisibility()
            throws Exception
    {
        storage.create(table);

        MetricsWriter writer = storage.newWriter(table);
        assertEquals(storage.newReader().getNumRowsInTable(table), 0);
        writer.incrementRowCount();
        writer.flush();
        assertEquals(storage.newReader().getNumRowsInTable(table), 1);
        writer.incrementRowCount();
        writer.flush();
        assertEquals(storage.newReader().getNumRowsInTable(table), 2);
        writer.incrementRowCount();
        writer.flush();
        assertEquals(storage.newReader().getNumRowsInTable(table), 3);

        writer.incrementRowCount();
        writer.flush();
        assertEquals(storage.newReader().getNumRowsInTable(table), 4);

        writer.incrementRowCount();
        writer.flush();
        assertEquals(storage.newReader().getNumRowsInTable(table), 5);
    }

    @Test
    public void testGetCardinality()
            throws Exception
    {
        storage.create(table);

        MetricsWriter writer = storage.newWriter(table);

        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "abc", "cf_firstname", new Authorizations())), 0);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "def", "cf_firstname", new Authorizations())), 0);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "ghi", "cf_firstname", new Authorizations())), 0);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "1", "cf_age", new Authorizations())), 0);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "2", "cf_age", new Authorizations())), 0);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "3", "cf_age", new Authorizations())), 0);

        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("abc"), bb("cf_firstname"), new ColumnVisibility());
        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("def"), bb("cf_firstname"), new ColumnVisibility());
        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("ghi"), bb("cf_firstname"), new ColumnVisibility());
        writer.incrementCardinality(table.getParsedIndexColumns().get(0).getIndexTable(), bb("1"), bb("cf_age"), new ColumnVisibility());
        writer.incrementCardinality(table.getParsedIndexColumns().get(0).getIndexTable(), bb("2"), bb("cf_age"), new ColumnVisibility());
        writer.incrementCardinality(table.getParsedIndexColumns().get(0).getIndexTable(), bb("3"), bb("cf_age"), new ColumnVisibility());
        writer.flush();

        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "abc", "cf_firstname", new Authorizations())), 1);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "def", "cf_firstname", new Authorizations())), 1);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "ghi", "cf_firstname", new Authorizations())), 1);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "1", "cf_age", new Authorizations())), 1);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "2", "cf_age", new Authorizations())), 1);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "3", "cf_age", new Authorizations())), 1);

        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("abc"), bb("cf_firstname"), new ColumnVisibility());
        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("def"), bb("cf_firstname"), new ColumnVisibility());
        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("ghi"), bb("cf_firstname"), new ColumnVisibility());
        writer.incrementCardinality(table.getParsedIndexColumns().get(0).getIndexTable(), bb("1"), bb("cf_age"), new ColumnVisibility());
        writer.incrementCardinality(table.getParsedIndexColumns().get(0).getIndexTable(), bb("2"), bb("cf_age"), new ColumnVisibility());
        writer.incrementCardinality(table.getParsedIndexColumns().get(0).getIndexTable(), bb("3"), bb("cf_age"), new ColumnVisibility());
        writer.flush();

        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "abc", "cf_firstname", new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "def", "cf_firstname", new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "ghi", "cf_firstname", new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "1", "cf_age", new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "2", "cf_age", new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "3", "cf_age", new Authorizations())), 2);

        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("abc"), bb("cf_firstname"), new ColumnVisibility("foo"));
        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("def"), bb("cf_firstname"), new ColumnVisibility("foo"));
        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("ghi"), bb("cf_firstname"), new ColumnVisibility("foo"));
        writer.incrementCardinality(table.getParsedIndexColumns().get(0).getIndexTable(), bb("1"), bb("cf_age"), new ColumnVisibility("foo"));
        writer.incrementCardinality(table.getParsedIndexColumns().get(0).getIndexTable(), bb("2"), bb("cf_age"), new ColumnVisibility("foo"));
        writer.incrementCardinality(table.getParsedIndexColumns().get(0).getIndexTable(), bb("3"), bb("cf_age"), new ColumnVisibility("foo"));
        writer.flush();

        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "abc", "cf_firstname", new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "def", "cf_firstname", new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "ghi", "cf_firstname", new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "1", "cf_age", new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "2", "cf_age", new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "3", "cf_age", new Authorizations())), 2);

        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "abc", "cf_firstname", new Authorizations("foo"))), 3);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "def", "cf_firstname", new Authorizations("foo"))), 3);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "ghi", "cf_firstname", new Authorizations("foo"))), 3);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "1", "cf_age", new Authorizations("foo"))), 3);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "2", "cf_age", new Authorizations("foo"))), 3);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "3", "cf_age", new Authorizations("foo"))), 3);

        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("abc"), bb("cf_firstname"), new ColumnVisibility("bar"));
        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("def"), bb("cf_firstname"), new ColumnVisibility("bar"));
        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("ghi"), bb("cf_firstname"), new ColumnVisibility("bar"));
        writer.incrementCardinality(table.getParsedIndexColumns().get(0).getIndexTable(), bb("1"), bb("cf_age"), new ColumnVisibility("bar"));
        writer.incrementCardinality(table.getParsedIndexColumns().get(0).getIndexTable(), bb("2"), bb("cf_age"), new ColumnVisibility("bar"));
        writer.incrementCardinality(table.getParsedIndexColumns().get(0).getIndexTable(), bb("3"), bb("cf_age"), new ColumnVisibility("bar"));
        writer.flush();

        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "abc", "cf_firstname", new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "def", "cf_firstname", new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "ghi", "cf_firstname", new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "1", "cf_age", new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "2", "cf_age", new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "3", "cf_age", new Authorizations())), 2);

        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "abc", "cf_firstname", new Authorizations("foo"))), 3);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "def", "cf_firstname", new Authorizations("foo"))), 3);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "ghi", "cf_firstname", new Authorizations("foo"))), 3);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "1", "cf_age", new Authorizations("foo"))), 3);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "2", "cf_age", new Authorizations("foo"))), 3);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "3", "cf_age", new Authorizations("foo"))), 3);

        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "abc", "cf_firstname", new Authorizations("foo", "bar"))), 4);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "def", "cf_firstname", new Authorizations("foo", "bar"))), 4);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "ghi", "cf_firstname", new Authorizations("foo", "bar"))), 4);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "1", "cf_age", new Authorizations("foo", "bar"))), 4);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "2", "cf_age", new Authorizations("foo", "bar"))), 4);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "3", "cf_age", new Authorizations("foo", "bar"))), 4);
    }

    @Test
    public void testGetCardinalityWithRange()
            throws Exception
    {
        storage.create(table);

        MetricsWriter writer = storage.newWriter(table);

        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "a", "z", "cf_firstname", new Authorizations())), 0);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "0", "9", "cf_age", new Authorizations())), 0);

        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("abc"), bb("cf_firstname"), new ColumnVisibility());
        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("def"), bb("cf_firstname"), new ColumnVisibility());
        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("ghi"), bb("cf_firstname"), new ColumnVisibility());
        writer.incrementCardinality(table.getParsedIndexColumns().get(0).getIndexTable(), bb("1"), bb("cf_age"), new ColumnVisibility());
        writer.incrementCardinality(table.getParsedIndexColumns().get(0).getIndexTable(), bb("2"), bb("cf_age"), new ColumnVisibility());
        writer.incrementCardinality(table.getParsedIndexColumns().get(0).getIndexTable(), bb("3"), bb("cf_age"), new ColumnVisibility());
        writer.flush();

        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "a", "z", "cf_firstname", new Authorizations())), 3);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "0", "9", "cf_age", new Authorizations())), 3);

        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("abc"), bb("cf_firstname"), new ColumnVisibility());
        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("def"), bb("cf_firstname"), new ColumnVisibility());
        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("ghi"), bb("cf_firstname"), new ColumnVisibility());
        writer.incrementCardinality(table.getParsedIndexColumns().get(0).getIndexTable(), bb("1"), bb("cf_age"), new ColumnVisibility());
        writer.incrementCardinality(table.getParsedIndexColumns().get(0).getIndexTable(), bb("2"), bb("cf_age"), new ColumnVisibility());
        writer.incrementCardinality(table.getParsedIndexColumns().get(0).getIndexTable(), bb("3"), bb("cf_age"), new ColumnVisibility());
        writer.flush();

        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "a", "z", "cf_firstname", new Authorizations())), 6);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "0", "9", "cf_age", new Authorizations())), 6);

        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("abc"), bb("cf_firstname"), new ColumnVisibility("foo"));
        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("def"), bb("cf_firstname"), new ColumnVisibility("foo"));
        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("ghi"), bb("cf_firstname"), new ColumnVisibility("foo"));
        writer.incrementCardinality(table.getParsedIndexColumns().get(0).getIndexTable(), bb("1"), bb("cf_age"), new ColumnVisibility("foo"));
        writer.incrementCardinality(table.getParsedIndexColumns().get(0).getIndexTable(), bb("2"), bb("cf_age"), new ColumnVisibility("foo"));
        writer.incrementCardinality(table.getParsedIndexColumns().get(0).getIndexTable(), bb("3"), bb("cf_age"), new ColumnVisibility("foo"));
        writer.flush();

        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "a", "z", "cf_firstname", new Authorizations())), 6);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "0", "9", "cf_age", new Authorizations())), 6);

        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "a", "z", "cf_firstname", new Authorizations("foo"))), 9);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "0", "9", "cf_age", new Authorizations("foo"))), 9);

        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("abc"), bb("cf_firstname"), new ColumnVisibility("bar"));
        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("def"), bb("cf_firstname"), new ColumnVisibility("bar"));
        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("ghi"), bb("cf_firstname"), new ColumnVisibility("bar"));
        writer.incrementCardinality(table.getParsedIndexColumns().get(0).getIndexTable(), bb("1"), bb("cf_age"), new ColumnVisibility("bar"));
        writer.incrementCardinality(table.getParsedIndexColumns().get(0).getIndexTable(), bb("2"), bb("cf_age"), new ColumnVisibility("bar"));
        writer.incrementCardinality(table.getParsedIndexColumns().get(0).getIndexTable(), bb("3"), bb("cf_age"), new ColumnVisibility("bar"));
        writer.flush();

        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "a", "z", "cf_firstname", new Authorizations())), 6);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "0", "9", "cf_age", new Authorizations())), 6);

        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "a", "z", "cf_firstname", new Authorizations("foo"))), 9);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "0", "9", "cf_age", new Authorizations("foo"))), 9);

        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(2).getIndexTable(), "a", "z", "cf_firstname", new Authorizations("foo", "bar"))), 12);
        assertEquals(storage.newReader().getCardinality(mck(table.getParsedIndexColumns().get(0).getIndexTable(), "0", "9", "cf_age", new Authorizations("foo", "bar"))), 12);
    }

    @Test
    public void testGetCardinalities()
            throws Exception
    {
        storage.create(table);

        MetricsWriter writer = storage.newWriter(table);

        List<MetricCacheKey> keys = ImmutableList.of(
                mck(table.getParsedIndexColumns().get(2).getIndexTable(), "abc", "cf_firstname", new Authorizations()),
                mck(table.getParsedIndexColumns().get(2).getIndexTable(), "def", "cf_firstname", new Authorizations()),
                mck(table.getParsedIndexColumns().get(2).getIndexTable(), "ghi", "cf_firstname", new Authorizations()));

        List<MetricCacheKey> fooKeys = ImmutableList.of(
                mck(table.getParsedIndexColumns().get(2).getIndexTable(), "abc", "cf_firstname", new Authorizations("foo")),
                mck(table.getParsedIndexColumns().get(2).getIndexTable(), "def", "cf_firstname", new Authorizations("foo")),
                mck(table.getParsedIndexColumns().get(2).getIndexTable(), "ghi", "cf_firstname", new Authorizations("foo")));

        List<MetricCacheKey> barKeys = ImmutableList.of(
                mck(table.getParsedIndexColumns().get(2).getIndexTable(), "abc", "cf_firstname", new Authorizations("bar")),
                mck(table.getParsedIndexColumns().get(2).getIndexTable(), "def", "cf_firstname", new Authorizations("bar")),
                mck(table.getParsedIndexColumns().get(2).getIndexTable(), "ghi", "cf_firstname", new Authorizations("bar")));

        List<MetricCacheKey> fooBarKeys = ImmutableList.of(
                mck(table.getParsedIndexColumns().get(2).getIndexTable(), "abc", "cf_firstname", new Authorizations("foo", "bar")),
                mck(table.getParsedIndexColumns().get(2).getIndexTable(), "def", "cf_firstname", new Authorizations("foo", "bar")),
                mck(table.getParsedIndexColumns().get(2).getIndexTable(), "ghi", "cf_firstname", new Authorizations("foo", "bar")));

        Map<MetricCacheKey, Long> cardinalities = storage.newReader().getCardinalities(keys);
        for (MetricCacheKey key : keys) {
            assertNotNull(cardinalities.get(key));
            assertEquals(cardinalities.get(key).longValue(), 0);
        }

        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("abc"), bb("cf_firstname"), new ColumnVisibility());
        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("def"), bb("cf_firstname"), new ColumnVisibility());
        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("ghi"), bb("cf_firstname"), new ColumnVisibility());
        writer.flush();

        cardinalities = storage.newReader().getCardinalities(keys);
        for (MetricCacheKey key : keys) {
            assertNotNull(cardinalities.get(key));
            assertEquals(cardinalities.get(key).longValue(), 1);
        }

        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("abc"), bb("cf_firstname"), new ColumnVisibility());
        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("def"), bb("cf_firstname"), new ColumnVisibility());
        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("ghi"), bb("cf_firstname"), new ColumnVisibility());
        writer.flush();

        cardinalities = storage.newReader().getCardinalities(keys);
        for (MetricCacheKey key : keys) {
            assertNotNull(cardinalities.get(key));
            assertEquals(cardinalities.get(key).longValue(), 2);
        }

        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("abc"), bb("cf_firstname"), new ColumnVisibility("foo"));
        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("def"), bb("cf_firstname"), new ColumnVisibility("foo"));
        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("ghi"), bb("cf_firstname"), new ColumnVisibility("foo"));
        writer.flush();

        cardinalities = storage.newReader().getCardinalities(keys);
        for (MetricCacheKey key : keys) {
            assertNotNull(cardinalities.get(key));
            assertEquals(cardinalities.get(key).longValue(), 2);
        }

        cardinalities = storage.newReader().getCardinalities(fooKeys);
        for (MetricCacheKey key : fooKeys) {
            assertNotNull(cardinalities.get(key));
            assertEquals(cardinalities.get(key).longValue(), 3);
        }

        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("abc"), bb("cf_firstname"), new ColumnVisibility("bar"));
        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("def"), bb("cf_firstname"), new ColumnVisibility("bar"));
        writer.incrementCardinality(table.getParsedIndexColumns().get(2).getIndexTable(), bb("ghi"), bb("cf_firstname"), new ColumnVisibility("bar"));
        writer.flush();

        cardinalities = storage.newReader().getCardinalities(keys);
        for (MetricCacheKey key : keys) {
            assertNotNull(cardinalities.get(key));
            assertEquals(cardinalities.get(key).longValue(), 2);
        }

        cardinalities = storage.newReader().getCardinalities(fooKeys);
        for (MetricCacheKey key : fooKeys) {
            assertNotNull(cardinalities.get(key));
            assertEquals(cardinalities.get(key).longValue(), 3);
        }

        cardinalities = storage.newReader().getCardinalities(barKeys);
        for (MetricCacheKey key : barKeys) {
            assertNotNull(cardinalities.get(key));
            assertEquals(cardinalities.get(key).longValue(), 3);
        }

        cardinalities = storage.newReader().getCardinalities(fooBarKeys);
        for (MetricCacheKey key : fooBarKeys) {
            assertNotNull(cardinalities.get(key));
            assertEquals(cardinalities.get(key).longValue(), 4);
        }
    }

    protected static ByteBuffer bb(String v)
    {
        return wrap(b(v));
    }

    protected static byte[] b(String v)
    {
        return v.getBytes(UTF_8);
    }

    protected MetricCacheKey mck(String indexTable, String value, String family, Authorizations auths)
    {
        return new MetricCacheKey(indexTable, new Text(family), auths, new Range(new Text(value.getBytes(UTF_8))), storage);
    }

    protected MetricCacheKey mck(String indexTable, String begin, String end, String family, Authorizations auths)
    {
        return new MetricCacheKey(indexTable, new Text(family), auths, new Range(new Text(begin.getBytes(UTF_8)), new Text(end.getBytes(UTF_8))), storage);
    }
}
