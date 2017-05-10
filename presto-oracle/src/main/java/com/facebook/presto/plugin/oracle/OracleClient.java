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
package com.facebook.presto.plugin.oracle;

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import oracle.jdbc.OracleDriver;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

public class OracleClient extends BaseJdbcClient
{
    private static final Logger log = Logger.get(BaseJdbcClient.class);
    private com.google.common.cache.LoadingCache<JdbcTableHandle, List<JdbcColumnHandle>> tableColumnsCache =
          CacheBuilder.newBuilder().maximumSize(10000)
                  .expireAfterAccess(7, java.util.concurrent.TimeUnit.DAYS)
                  .build(new CacheLoader<JdbcTableHandle, List<JdbcColumnHandle>>(){
                      @Override
                      public List<JdbcColumnHandle> load(JdbcTableHandle tableHandle) throws Exception
                      {
                          return OracleClient.super.getColumns(tableHandle);
                      }
                  });

    @Inject
    public OracleClient(JdbcConnectorId connectorId, BaseJdbcConfig config,
                        OracleConfig oracleConfig) throws SQLException
    {
        super(connectorId, config, "", new OracleDriver());
        if (oracleConfig.isIncludeSynonyms()) {
            connectionProperties.setProperty("includeSynonyms", String.valueOf(oracleConfig.isIncludeSynonyms()));
        }
        connectionProperties.setProperty("defaultRowPrefetch", String.valueOf(oracleConfig.getDefaultRowPrefetch()));
    }

    @Override
    public Set<String> getSchemaNames()
    {
        try (Connection connection = driver.connect(connectionUrl,
                connectionProperties);
             ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString(1).toLowerCase();
                schemaNames.add(schemaName);
            }
            return schemaNames.build();
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    protected ResultSet getTables(Connection connection, String schemaName,
                                  String tableName) throws SQLException
    {
        // Here we put TABLE and SYNONYM when the table schema is another user schema
        return connection.getMetaData().getTables(connection.getCatalog(), schemaName, tableName,
                new String[] { "TABLE", "SYNONYM", "VIEW" });
    }

    @Override
    public List<JdbcColumnHandle> getColumns(JdbcTableHandle tableHandle)
    {
        try {
            return tableColumnsCache.get(tableHandle);
        }
        catch (java.util.concurrent.ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }
}
