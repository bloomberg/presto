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
import com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.model.IndexColumn;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.accumulo.index.metrics.AccumuloMetricsStorage.getMetricsTableName;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.DAY;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.HOUR;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.MINUTE;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.SECOND;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestAccumuloMetricStorage
        extends TestAbstractMetricStorage
{
    private static final Map<TimestampPrecision, byte[]> TIMESTAMP_CARDINALITY_FAMILIES = ImmutableMap.of(
            SECOND, "_tss".getBytes(UTF_8),
            MINUTE, "_tsm".getBytes(UTF_8),
            HOUR, "_tsh".getBytes(UTF_8),
            DAY, "_tsd".getBytes(UTF_8));

    private Connector connector;

    @Override
    public MetricsStorage getMetricsStorage(AccumuloConfig config)
    {
        return new AccumuloMetricsStorage(connector);
    }

    @BeforeClass
    @Override
    public void setupClass()
            throws Exception
    {
        config.setUsername("root");
        config.setPassword("secret");

        connector = AccumuloQueryRunner.getAccumuloConnector();
        connector.securityOperations().changeUserAuthorizations("root", new Authorizations("private", "moreprivate", "foo", "bar", "xyzzy"));

        super.setupClass();
    }

    @BeforeMethod
    public void setup()
            throws Exception
    {
        for (AccumuloTable accumuloTable : ImmutableList.of(table, table2)) {
            connector.tableOperations().create(accumuloTable.getFullTableName());
            for (IndexColumn indexColumn : accumuloTable.getParsedIndexColumns()) {
                if (!connector.tableOperations().exists(indexColumn.getIndexTable())) {
                    connector.tableOperations().create(indexColumn.getIndexTable());
                }
            }
        }
    }

    @AfterMethod
    public void cleanup()
            throws Exception
    {
        for (AccumuloTable accumuloTable : ImmutableList.of(table, table2)) {
            connector.tableOperations().delete(accumuloTable.getFullTableName());
            for (IndexColumn indexColumn : accumuloTable.getParsedIndexColumns()) {
                if (connector.tableOperations().exists(indexColumn.getIndexTable())) {
                    connector.tableOperations().delete(indexColumn.getIndexTable());
                }
            }
        }

        if (connector.tableOperations().exists(getMetricsTableName(table))) {
            connector.tableOperations().delete(getMetricsTableName(table));
        }

        if (connector.tableOperations().exists(getMetricsTableName(table2))) {
            connector.tableOperations().delete(getMetricsTableName(table2));
        }
    }

    @Test
    public void testCreateTable()
            throws Exception
    {
        AccumuloTable table = getTable("test_accumulo_metric_storage_create");
        try {
            createIndexTables(table);
            storage.create(table);
            assertTrue(connector.tableOperations().exists(getMetricsTableName(table)));
        }
        finally {
            destroyIndexTables(table);
        }
    }

    @Test
    public void testCreateTableAlreadyExists()
            throws Exception
    {
        AccumuloTable table = getTable("test_accumulo_metric_storage_create_already_exists");
        try {
            createIndexTables(table);
            connector.tableOperations().create(getMetricsTableName(table));
            storage.create(table);
            assertTrue(connector.tableOperations().exists(getMetricsTableName(table)));
        }
        finally {
            destroyIndexTables(table);
        }
    }

    @Test
    public void testDropTable()
            throws Exception
    {
        AccumuloTable table = getTable("test_accumulo_metric_storage_drop_table");
        try {
            createIndexTables(table);

            storage.create(table);
            assertTrue(connector.tableOperations().exists(getMetricsTableName(table)));
            storage.drop(table);
            assertFalse(connector.tableOperations().exists(getMetricsTableName(table)));
        }
        finally {
            destroyIndexTables(table);
        }
    }

    @Test
    public void testDropTableDoesNotExist()
            throws Exception
    {
        AccumuloTable table = getTable("test_accumulo_metric_storage_drop_table_does_not_exist");
        storage.drop(table);
        assertFalse(connector.tableOperations().exists(getMetricsTableName(table)));
    }

    @Override
    public void testDropExternalTable()
            throws Exception
    {
        AccumuloTable externalTable = getTable("test_accumulo_metric_storage_drop_external_table", true);
        try {
            createIndexTables(externalTable);
            storage.create(externalTable);
            assertTrue(connector.tableOperations().exists(getMetricsTableName(externalTable)));
            storage.drop(externalTable);
            assertTrue(connector.tableOperations().exists(getMetricsTableName(externalTable)));
        }
        finally {
            destroyIndexTables(externalTable);
        }
    }

    @Test
    public void testRenameTable()
            throws Exception
    {
        AccumuloTable table = getTable("test_accumulo_metric_storage_rename_table");
        AccumuloTable table2 = getTable("test_accumulo_metric_storage_rename_table2");
        try {
            createIndexTables(table);
            createIndexTables(table2);
            storage.create(table);
            storage.rename(table, table2);
            assertFalse(connector.tableOperations().exists(getMetricsTableName(table)));
            assertTrue(connector.tableOperations().exists(getMetricsTableName(table2)));
        }
        finally {
            destroyIndexTables(table);
            destroyIndexTables(table2);
        }
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testRenameTableDoesNotExist()
            throws Exception
    {
        AccumuloTable table = getTable("test_accumulo_metric_storage_rename_table_does_not_exist");
        AccumuloTable table2 = getTable("test_accumulo_metric_storage_rename_table_does_not_exist2");
        storage.rename(table, table2);
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testRenameTableNewTableExists()
            throws Exception
    {
        AccumuloTable table = getTable("test_accumulo_metric_storage_rename_new_table_exists");
        AccumuloTable table2 = getTable("test_accumulo_metric_storage_rename_new_table_exists2");
        try {
            createIndexTables(table);
            storage.create(table);
            connector.tableOperations().create(AccumuloMetricsStorage.getMetricsTableName(table2));
            storage.rename(table, table2);
        }
        finally {
            destroyIndexTables(table);
        }
    }

    @Test
    public void testExists()
            throws Exception
    {
        AccumuloTable table = getTable("test_accumulo_metric_storage_exists");
        try {
            createIndexTables(table);

            assertFalse(storage.exists(table.getSchemaTableName()));
            storage.create(table);
            assertTrue(storage.exists(table.getSchemaTableName()));
        }
        finally {
            destroyIndexTables(table);
        }
    }

    private AccumuloTable getTable(String tablename)
    {
        return getTable(tablename, false);
    }

    private AccumuloTable getTable(String tablename, boolean external)
    {
        return new AccumuloTable(table.getSchema(), tablename, table.getColumns(), table.getRowId(), external, table.getSerializerClassName(), table.getScanAuthorizations(), table.getMetricsStorageClass(), table.isTruncateTimestamps(), table.getIndexColumns());
    }

    private void createIndexTables(AccumuloTable table)
            throws TableExistsException, AccumuloSecurityException, AccumuloException
    {
        for (IndexColumn indexColumn : table.getParsedIndexColumns()) {
            if (!connector.tableOperations().exists(indexColumn.getIndexTable())) {
                connector.tableOperations().create(indexColumn.getIndexTable());
            }
        }
    }

    private void destroyIndexTables(AccumuloTable table)
            throws AccumuloSecurityException, AccumuloException, TableNotFoundException
    {
        for (IndexColumn indexColumn : table.getParsedIndexColumns()) {
            if (connector.tableOperations().exists(indexColumn.getIndexTable())) {
                connector.tableOperations().delete(indexColumn.getIndexTable());
            }
        }
    }
}
