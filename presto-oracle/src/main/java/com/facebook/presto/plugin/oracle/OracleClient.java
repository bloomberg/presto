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
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import io.airlift.log.Logger;
import oracle.jdbc.OracleDriver;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class OracleClient extends BaseJdbcClient
{
    private static final Logger log = Logger.get(BaseJdbcClient.class);

    @Inject
    public OracleClient(JdbcConnectorId connectorId, BaseJdbcConfig config,
                        OracleConfig oracleConfig) throws SQLException
    {
        super(connectorId, config, "", new OracleDriver());
    }

    protected ResultSet getTables(Connection connection, String schemaName,
                                  String tableName) throws SQLException
    {
        // Here we put TABLE and SYNONYM when the table schema is another user schema
        return connection.getMetaData().getTables(null, schemaName, tableName,
                new String[] { "TABLE", "SYNONYM" });
    }
}
