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
package com.facebook.presto.accumulo.io;

import com.facebook.presto.accumulo.index.IndexLookup;
import com.facebook.presto.accumulo.metadata.ZooKeeperMetadataManager;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.model.AccumuloSplit;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.log.Logger;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;

import javax.inject.Inject;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class AccumuloPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final Logger LOG = Logger.get(AccumuloPageSourceProvider.class);
    private static final Splitter COMMA_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

    private final Connector connector;
    private final ZooKeeperMetadataManager metadataManager;

    @Inject
    public AccumuloPageSourceProvider(
            Connector connector,
            ZooKeeperMetadataManager metadataManager)
    {
        this.connector = requireNonNull(connector, "connector is null");
        this.metadataManager = requireNonNull(metadataManager, "metadataManager is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns)
    {
        AccumuloSplit accumuloSplit = (AccumuloSplit) split;
        Authorizations auths = new Authorizations(Iterables.toArray(COMMA_SPLITTER.split(accumuloSplit.getScanAuthorizations()), String.class));
        LOG.debug("scan auths are %s", auths);

        List<Range> ranges;
        if (accumuloSplit.getIndexQueryParameters().isPresent()) {
            ranges = new IndexLookup().getIndexRanges(
                    connector,
                    session,
                    ImmutableList.of(accumuloSplit.getIndexQueryParameters().get()),
                    accumuloSplit.getRowIdRanges(),
                    auths);

            // No data to retrieve from Accumulo, return a null page
            if (ranges.isEmpty()) {
                return new NullPageSource();
            }
        }
        else {
            ranges = accumuloSplit.getRanges();
        }

        return new AccumuloPageSource(
                connector,
                auths,
                metadataManager.getTable(new SchemaTableName(accumuloSplit.getSchema(), accumuloSplit.getTable())),
                ranges,
                columns.stream().map(AccumuloColumnHandle.class::cast).collect(Collectors.toList()));
    }
}
