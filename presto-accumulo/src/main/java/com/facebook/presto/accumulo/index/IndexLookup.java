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
package com.facebook.presto.accumulo.index;

import com.facebook.presto.accumulo.model.AccumuloRange;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.log.Logger;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.htrace.Sampler;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.accumulo.AccumuloErrorCode.UNEXPECTED_ACCUMULO_ERROR;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.isTracingEnabled;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Class to assist the Presto connector, and maybe external applications,
 * leverage the secondary * index built by the {@link Indexer}.
 * Leverages {@link ColumnCardinalityCache} to assist in * retrieving row IDs.
 * Currently pretty bound to the Presto connector APIs.
 */
public class IndexLookup
{
    private static final Logger LOG = Logger.get(IndexLookup.class);
    private static final ExecutorService EXECUTOR;

    static {
        AtomicLong threadCount = new AtomicLong(0);
        EXECUTOR = MoreExecutors.getExitingExecutorService(
                new ThreadPoolExecutor(1, 4 * Runtime.getRuntime().availableProcessors(), 60L,
                        SECONDS, new SynchronousQueue<>(), runnable ->
                        new Thread(runnable, "index-range-scan-thread-" + threadCount.getAndIncrement())
                ));
    }

    private IndexLookup() {}

    public static List<Range> getIndexRanges(Connector connector, ConnectorSession session, String indexTable, List<IndexQueryParameters> indexParameters, Collection<AccumuloRange> rowIDRanges, Authorizations auths)
    {
        // For each column/constraint pair we submit a task to scan the index ranges
        Set<Range> finalRanges = new HashSet<>();
        List<Callable<Set<Range>>> tasks = new ArrayList<>();
        for (IndexQueryParameters queryParameters : indexParameters) {
            Callable<Set<Range>> task = () -> {
                Optional<TraceScope> indexTrace = Optional.empty();
                BatchScanner scanner = null;
                try {
                    if (isTracingEnabled(session)) {
                        String traceName = String.format("%s:%s_metrics:IndexLookup:%s", session.getQueryId(), indexTable, queryParameters.getIndexFamily());
                        indexTrace = Optional.of(Trace.startSpan(traceName, Sampler.ALWAYS));
                    }

                    long start = System.currentTimeMillis();
                    // Create a batch scanner against the index table, setting the ranges
                    scanner = connector.createBatchScanner(indexTable, auths, 10);
                    scanner.setRanges(queryParameters.getRanges());

                    // Fetch the column family for this specific column
                    scanner.fetchColumnFamily(queryParameters.getIndexFamily());

                    // For each entry in the scanner
                    Text tmpQualifier = new Text();
                    Set<Range> columnRanges = new HashSet<>();
                    for (Entry<Key, Value> entry : scanner) {
                        entry.getKey().getColumnQualifier(tmpQualifier);

                        // Add to our column ranges if it is in one of the row ID ranges
                        if (inRange(tmpQualifier, rowIDRanges)) {
                            columnRanges.add(new Range(tmpQualifier));
                        }
                    }

                    LOG.debug("Retrieved %d ranges for index column %s took %s ms", columnRanges.size(), queryParameters.getIndexColumn(), System.currentTimeMillis() - start);
                    return columnRanges;
                }
                finally {
                    if (scanner != null) {
                        scanner.close();
                    }

                    indexTrace.ifPresent(TraceScope::close);
                }
            };
            tasks.add(task);
        }

        try {
            EXECUTOR.invokeAll(tasks).forEach(future ->
            {
                try {
                    // If finalRanges is null, we have not yet added any column ranges
                    if (finalRanges.isEmpty()) {
                        finalRanges.addAll(future.get());
                    }
                    else {
                        // Retain only the row IDs for this column that have already been added
                        // This is your set intersection operation!
                        finalRanges.retainAll(future.get());
                    }
                }
                catch (ExecutionException | InterruptedException e) {
                    throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Exception when getting index ranges", e);
                }
            });
        }
        catch (InterruptedException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "InterruptedException when getting index ranges", e);
        }

        return ImmutableList.copyOf(finalRanges);
    }

    private static boolean inRange(Text text, Collection<AccumuloRange> ranges)
    {
        Key qualifier = new Key(text);
        return ranges.stream().map(AccumuloRange::getRange).anyMatch(r -> !r.beforeStartKey(qualifier) && !r.afterEndKey(qualifier));
    }
}
