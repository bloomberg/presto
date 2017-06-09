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

import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.conf.AccumuloSessionProperties;
import com.facebook.presto.accumulo.index.metrics.MetricsReader;
import com.facebook.presto.accumulo.index.metrics.MetricsStorage;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.model.AccumuloColumnConstraint;
import com.facebook.presto.accumulo.model.TabletSplitMetadata;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.facebook.presto.accumulo.AccumuloClient.getRangesFromDomain;
import static com.facebook.presto.accumulo.AccumuloErrorCode.UNEXPECTED_ACCUMULO_ERROR;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.getIndexSmallCardRowThreshold;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.getIndexSmallCardThreshold;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.getIndexThreshold;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.getMaxRowsPerSplit;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.getMinRowsPerSplit;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.getNumIndexRowsPerSplit;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.getSplitsPerWorker;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.isIndexMetricsEnabled;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.isOptimizeIndexEnabled;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.isOptimizeNumRowsPerSplitEnabled;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Class to assist the Presto connector, and maybe external applications,
 * leverage the secondary * index built by the {@link Indexer}.
 * Leverages {@link ColumnCardinalityCache} to assist in * retrieving row IDs.
 * Currently pretty bound to the Presto connector APIs.
 */
public class IndexLookup
{
    private static final Logger LOG = Logger.get(IndexLookup.class);
    private final ColumnCardinalityCache cardinalityCache;
    private final Connector connector;
    private final ExecutorService coreExecutor;
    private final BoundedExecutor executorService;
    private final NodeManager nodeManager;
    private final int maxIndexLookup;

    @Inject
    public IndexLookup(AccumuloConfig config, Connector connector, ColumnCardinalityCache cardinalityCache, NodeManager nodeManager)
    {
        this.connector = requireNonNull(connector, "connector is null");
        this.cardinalityCache = requireNonNull(cardinalityCache, "cardinalityCache is null");
        this.maxIndexLookup = requireNonNull(config, "config is null").getMaxIndexLookupCardinality();
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");

        // Create a bounded executor with a pool size at 4x number of processors
        this.coreExecutor = newCachedThreadPool(daemonThreadsNamed("cardinality-lookup-%s"));
        this.executorService = new BoundedExecutor(coreExecutor, 4 * Runtime.getRuntime().availableProcessors());
    }

    @PreDestroy
    public void shutdown()
    {
        coreExecutor.shutdownNow();
    }

    /**
     * Scans the index table, applying the index based on the given column constraints to return a set of tablet splits.
     * <p>
     * If this function returns true, the output parameter tabletSplits contains a list of TabletSplitMetadata objects.
     * These in turn contain a collection of Ranges containing the exact row IDs determined using the index.
     * <p>
     * If this function returns false, the secondary index should not be used. In this case,
     * either the accumulo session has disabled secondary indexing,
     * or the number of row IDs that would be used by the secondary index is greater than the configured threshold
     * (again retrieved from the session).
     *
     * @param session Current client session
     * @param table Table metadata
     * @param constraints All column constraints (this method will filter for if the column is indexed)
     * @param rowIdRanges Collection of Accumulo ranges based on any predicate against a record key
     * @param tabletSplits Output parameter containing the bundles of row IDs determined by the use of the index.
     * @param auths Scan authorizations
     * @return True if the tablet splits are valid and should be used, false otherwise
     * @throws Exception If something bad happens. What are the odds?
     */
    public boolean applyIndex(
            ConnectorSession session,
            AccumuloTable table,
            Collection<AccumuloColumnConstraint> constraints,
            Collection<Range> rowIdRanges,
            List<TabletSplitMetadata> tabletSplits,
            Authorizations auths)
            throws Exception
    {
        // Early out if index is disabled
        if (!isOptimizeIndexEnabled(session)) {
            LOG.debug("Secondary index is disabled");
            return false;
        }

        LOG.debug("Secondary index is enabled");

        // Collect Accumulo ranges for each indexed column constraint
        Multimap<AccumuloColumnConstraint, Range> constraintRanges = getIndexedConstraintRanges(table, constraints);

        // If there is no constraints on an index column, we again will bail out
        if (constraintRanges.isEmpty()) {
            LOG.debug("Query contains no constraints on indexed columns, skipping secondary index");
            return false;
        }

        // If metrics are not enabled
        if (!isIndexMetricsEnabled(session)) {
            LOG.debug("Use of index metrics is disabled");
            // Get the ranges via the index table
            List<Range> indexRanges = getIndexRanges(table.getIndexTableName(), constraintRanges, rowIdRanges, auths);
            if (!indexRanges.isEmpty()) {
                // Bin the ranges into TabletMetadataSplits and return true to use the tablet splits
                binRanges(optimizeNumRowsPerSplit(session, indexRanges.size(), nodeManager.getWorkerNodes().size()), indexRanges, tabletSplits);
                LOG.debug("Number of splits for %s is %d with %d ranges", table.getFullTableName(), tabletSplits.size(), indexRanges.size());
            }
            else {
                LOG.debug("Query would return no results, returning empty list of splits");
            }

            return true;
        }
        else {
            LOG.debug("Use of index metrics is enabled");
            // Get ranges using the metrics
            return getRangesWithMetrics(session, table, constraintRanges, rowIdRanges, tabletSplits, auths);
        }
    }

    private int optimizeNumRowsPerSplit(ConnectorSession session, int numRowIDs, int numWorkers)
    {
        if (isOptimizeNumRowsPerSplitEnabled(session)) {
            int min = getMinRowsPerSplit(session);

            if (numRowIDs <= min) {
                LOG.debug("RowsPerSplit " + numRowIDs);
                return numRowIDs;
            }

            int max = getMaxRowsPerSplit(session);
            int splitsPerWorker = getSplitsPerWorker(session);
            int rowsPerSplit = Math.max(Math.min((int) Math.ceil((float) numRowIDs / (float) splitsPerWorker / (float) numWorkers), max), min);
            LOG.debug("RowsPerSplit %s, Row IDs %s, Min %s, Max %s, Workers %s, SplitsPerWorker %s", rowsPerSplit, numRowIDs, min, max, numWorkers, splitsPerWorker);
            return rowsPerSplit;
        }
        else {
            return getNumIndexRowsPerSplit(session);
        }
    }

    private static Multimap<AccumuloColumnConstraint, Range> getIndexedConstraintRanges(AccumuloTable table, Collection<AccumuloColumnConstraint> constraints)
            throws AccumuloSecurityException, AccumuloException
    {
        AccumuloRowSerializer serializer = table.getSerializerInstance();
        ImmutableSetMultimap.Builder<AccumuloColumnConstraint, Range> builder = ImmutableSetMultimap.builder();
        // TODO
//        constraints.stream()
//                .filter(x -> {
//                    for (IndexColumn column : table.getParsedIndexColumns()) {
//                        AccumuloColumnHandle handle = table.getColumn(column.getColumns());
//                        if (handle.getFamily().isPresent() && handle.getQualifier().isPresent() &&
//                                handle.getFamily().get().equals(x.getFamily()) && handle.getQualifier().get().equals(x.getQualifier())) {
//                            return true;
//                        }
//                    }
//                    return false;
//                })
//                .forEach(constraint -> builder.putAll(constraint, getRangesFromDomain(constraint.getDomain(), serializer)));
        return builder.build();
    }

    private boolean getRangesWithMetrics(
            ConnectorSession session,
            AccumuloTable table,
            Multimap<AccumuloColumnConstraint, Range> constraintRanges,
            Collection<Range> rowIdRanges,
            List<TabletSplitMetadata> tabletSplits,
            Authorizations auths)
            throws Exception
    {
        MetricsStorage metricsStorage = table.getMetricsStorageInstance(connector);
        MetricsReader reader = metricsStorage.newReader();
        long numRows = reader.getNumRowsInTable(table.getSchema(), table.getTable());

        // Get the cardinalities from the metrics table
        Multimap<Long, AccumuloColumnConstraint> cardinalities;
        if (AccumuloSessionProperties.isIndexShortCircuitEnabled(session)) {
            cardinalities = cardinalityCache.getCardinalities(
                    table.getSchema(),
                    table.getTable(),
                    auths,
                    constraintRanges,
                    getSmallestCardinalityThreshold(session, numRows),
                    AccumuloSessionProperties.getIndexCardinalityCachePollingDuration(session),
                    metricsStorage,
                    table.isTruncateTimestamps());
        }
        else {
            // disable short circuit using 0
            cardinalities = cardinalityCache.getCardinalities(
                    table.getSchema(),
                    table.getTable(),
                    auths,
                    constraintRanges,
                    0,
                    new Duration(0, MILLISECONDS),
                    metricsStorage,
                    table.isTruncateTimestamps());
        }

        Optional<Entry<Long, AccumuloColumnConstraint>> entry = cardinalities.entries().stream().findFirst();
        if (!entry.isPresent()) {
            return false;
        }

        Entry<Long, AccumuloColumnConstraint> lowestCardinality = entry.get();
        String indexTable = table.getIndexTableName();
        double threshold = getIndexThreshold(session);
        List<Range> indexRanges;

        // If the smallest cardinality in our list is above the lowest cardinality threshold,
        // we should look at intersecting the row ID ranges to try and get under the threshold.
        if (smallestCardAboveThreshold(session, numRows, lowestCardinality.getKey())) {
            // If we only have one column, we can check the index threshold without doing the row intersection
            if (cardinalities.size() == 1) {
                long numEntries = lowestCardinality.getKey();
                double ratio = ((double) numEntries / (double) numRows);
                LOG.debug("Use of index would scan %d of %d rows, ratio %s. Threshold %2f, Using for index table? %b", numEntries, numRows, ratio, threshold, ratio < threshold);
                if (ratio >= threshold) {
                    return false;
                }
            }

            // Else, remove columns with a large number of rows
            ImmutableSetMultimap.Builder<AccumuloColumnConstraint, Range> builder = ImmutableSetMultimap.builder();
            cardinalities.entries().stream().filter(x -> x.getKey() < maxIndexLookup).map(Entry::getValue).forEach(constraint -> {
                LOG.debug(format("Cardinality of column %s is below the max index lookup threshold %s, added for intersection", constraint.getName(), maxIndexLookup));
                builder.putAll(constraint, constraintRanges.get(constraint));
            });
            Multimap<AccumuloColumnConstraint, Range> intersectionColumns = builder.build();

            // If there are columns to do row intersection, then do so
            if (intersectionColumns.size() > 0) {
                LOG.debug("%d indexed columns, intersecting ranges", intersectionColumns.size());
                indexRanges = getIndexRanges(indexTable, intersectionColumns, rowIdRanges, auths);
                LOG.debug("Intersection results in %d ranges from secondary index", indexRanges.size());
            }
            else {
                LOG.debug("No columns have few enough entries to allow intersection, doing a full table scan");
                return false;
            }
        }
        else {
            // Else, we don't need to intersect the columns and we can just use the column with the lowest cardinality,
            // so get all those row IDs in a set of ranges.
            LOG.debug("Not intersecting columns, using column with lowest cardinality");
            ImmutableMultimap.Builder<AccumuloColumnConstraint, Range> lcBldr = ImmutableMultimap.builder();
            lcBldr.putAll(lowestCardinality.getValue(), constraintRanges.get(lowestCardinality.getValue()));
            indexRanges = getIndexRanges(indexTable, lcBldr.build(), rowIdRanges, auths);
        }

        if (indexRanges.isEmpty()) {
            LOG.debug("Query would return no results, returning empty list of splits");
            return true;
        }

        // Okay, we now check how many rows we would scan by using the index vs. the overall number
        // of rows
        long numEntries = indexRanges.size();
        double ratio = (double) numEntries / (double) numRows;
        LOG.debug("Use of index would scan %d of %d rows, ratio %s. Threshold %2f, Using for table? %b", numEntries, numRows, ratio, threshold, ratio < threshold, table);

        // If the percentage of scanned rows, the ratio, less than the configured threshold
        if (ratio < threshold) {
            // Bin the ranges into TabletMetadataSplits and return true to use the tablet splits
            binRanges(optimizeNumRowsPerSplit(session, indexRanges.size(), nodeManager.getWorkerNodes().size()), indexRanges, tabletSplits);
            LOG.debug("Number of splits for %s is %d with %d ranges", table.getFullTableName(), tabletSplits.size(), indexRanges.size());
            return true;
        }
        else {
            // We are going to do too much work to use the secondary index, so return false
            return false;
        }
    }

    private static boolean smallestCardAboveThreshold(ConnectorSession session, long numRows, long smallestCardinality)
    {
        long threshold = getSmallestCardinalityThreshold(session, numRows);
        LOG.info("Smallest cardinality is %s, num rows is %s, threshold is %s", smallestCardinality, numRows, threshold);
        return smallestCardinality > threshold;
    }

    /**
     * Gets the smallest cardinality threshold, which is the number of rows to skip index intersection
     * if the cardinality is less than or equal to this value.
     * <p>
     * The threshold is the minimum of the percentage-based threshold and the row threshold
     *
     * @param session Current client session
     * @param numRows Number of rows in the table
     * @return Threshold
     */
    private static long getSmallestCardinalityThreshold(ConnectorSession session, long numRows)
    {
        return Math.min(
                (long) (numRows * getIndexSmallCardThreshold(session)),
                getIndexSmallCardRowThreshold(session));
    }

    private List<Range> getIndexRanges(String indexTable, Multimap<AccumuloColumnConstraint, Range> constraintRanges, Collection<Range> rowIDRanges, Authorizations auths)
            throws TableNotFoundException, InterruptedException
    {
        Set<Range> finalRanges = new HashSet<>();
        // For each column/constraint pair we submit a task to scan the index ranges
        List<Future<Set<Range>>> tasks = new ArrayList<>();
        CompletionService<Set<Range>> executor = new ExecutorCompletionService<>(executorService);
        for (Entry<AccumuloColumnConstraint, Collection<Range>> constraintEntry : constraintRanges.asMap().entrySet()) {
            tasks.add(executor.submit(() -> {
                BatchScanner scanner = null;
                try {
                    // Create a batch scanner against the index table, setting the ranges
                    scanner = connector.createBatchScanner(indexTable, auths, 10);
                    scanner.setRanges(constraintEntry.getValue());

                    // Fetch the column family for this specific column
                    Text cf = new Text(Indexer.getIndexColumnFamily(constraintEntry.getKey().getFamily().getBytes(), constraintEntry.getKey().getQualifier().getBytes()));
                    scanner.fetchColumnFamily(cf);

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

                    LOG.info("Retrieved %d ranges for index column %s", columnRanges.size(), constraintEntry.getKey().getName());
                    scanner.close();
                    return columnRanges;
                }
                finally {
                    if (scanner != null) {
                        scanner.close();
                    }
                }
            }));
        }

        tasks.forEach(future ->
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
        return ImmutableList.copyOf(finalRanges);
    }

    private static void binRanges(int numRangesPerBin, List<Range> splitRanges, List<TabletSplitMetadata> prestoSplits)
    {
        checkArgument(numRangesPerBin > 0, "number of ranges per bin must positivebe greater than zero");
        int toAdd = splitRanges.size();
        int fromIndex = 0;
        int toIndex = Math.min(toAdd, numRangesPerBin);
        do {
            // Add the sublist of range handles
            // Use an empty location because we are binning multiple Ranges spread across many tablet servers
            prestoSplits.add(new TabletSplitMetadata(Optional.empty(), splitRanges.subList(fromIndex, toIndex)));
            toAdd -= toIndex - fromIndex;
            fromIndex = toIndex;
            toIndex += Math.min(toAdd, numRangesPerBin);
        }
        while (toAdd > 0);
    }

    /**
     * Gets a Boolean value indicating if the given value is in one of the Ranges in the given collection
     *
     * @param text Text object to check against the Range collection
     * @param ranges Ranges to look into
     * @return True if the text object is in one of the ranges, false otherwise
     */
    private static boolean inRange(Text text, Collection<Range> ranges)
    {
        Key qualifier = new Key(text);
        return ranges.stream().anyMatch(r -> !r.beforeStartKey(qualifier) && !r.afterEndKey(qualifier));
    }
}
