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
import com.facebook.presto.accumulo.model.AccumuloRange;
import com.facebook.presto.accumulo.model.IndexColumn;
import com.facebook.presto.accumulo.model.TabletSplitMetadata;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
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

import javax.inject.Inject;

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

import static com.facebook.presto.accumulo.AccumuloClient.getRangesFromDomain;
import static com.facebook.presto.accumulo.AccumuloErrorCode.UNEXPECTED_ACCUMULO_ERROR;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.getIndexSmallCardRowThreshold;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.getIndexSmallCardThreshold;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.getMaxRowsPerSplit;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.getMinRowsPerSplit;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.getNumIndexRowsPerSplit;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.getSplitsPerWorker;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.isOptimizeNumRowsPerSplitEnabled;
import static com.facebook.presto.accumulo.index.Indexer.getIndexColumnFamily;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
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
    private final Connector connector;
    private final ColumnCardinalityCache cardinalityCache;
    private final ExecutorService executor;
    private final NodeManager nodeManager;
    private final int maxIndexLookup;

    @Inject
    public IndexLookup(Connector connector, AccumuloConfig config, NodeManager nodeManager)
    {
        this.connector = requireNonNull(connector, "connector is null");
        this.cardinalityCache = new ColumnCardinalityCache(config.getCardinalityCacheSize(), config.getCardinalityCacheExpiration());
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");

        AtomicLong threadCount = new AtomicLong(0);
        this.executor = MoreExecutors.getExitingExecutorService(
                new ThreadPoolExecutor(1, 4 * Runtime.getRuntime().availableProcessors(), 60L,
                        SECONDS, new SynchronousQueue<>(), runnable ->
                        new Thread(runnable, "index-range-scan-thread-" + threadCount.getAndIncrement())
                ));

        this.maxIndexLookup = config.getMaxIndexLookupCardinality();
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
            Collection<AccumuloRange> rowIdRanges,
            List<TabletSplitMetadata> tabletSplits,
            Authorizations auths)
            throws Exception
    {
        // Early out if index is disabled
        if (!AccumuloSessionProperties.isOptimizeIndexEnabled(session)) {
            LOG.debug("Secondary index is disabled");
            return false;
        }

        LOG.debug("Secondary index is enabled");

        // Collect Accumulo ranges for each indexed column constraint
        List<IndexQueryParameters> indexParameters = getIndexQueryParameters(table, constraints);

        // If there is no constraints on an index column, we again will bail out
        if (indexParameters.isEmpty()) {
            LOG.debug("Query contains no constraints on indexed columns, skipping secondary index");
            return false;
        }

        // If metrics are not enabled
        if (!AccumuloSessionProperties.isIndexMetricsEnabled(session)) {
            LOG.debug("Use of index metrics is disabled");
            // Get the ranges via the index table
            List<Range> indexRanges = getIndexRanges(table.getIndexTableName(), indexParameters, rowIdRanges, auths);
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
            return getRangesWithMetrics(session, table, indexParameters, rowIdRanges, tabletSplits, auths);
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

    private boolean getRangesWithMetrics(
            ConnectorSession session,
            AccumuloTable table,
            List<IndexQueryParameters> indexQueryParameters,
            Collection<AccumuloRange> rowIdRanges,
            List<TabletSplitMetadata> tabletSplits,
            Authorizations auths)
            throws Exception
    {
        MetricsStorage metricsStorage = table.getMetricsStorageInstance(connector);
        MetricsReader reader = metricsStorage.newReader();
        long numRows = reader.getNumRowsInTable(table.getSchema(), table.getTable());

        // Get the cardinalities from the metrics table
        Multimap<Long, IndexQueryParameters> cardinalities;
        if (AccumuloSessionProperties.isIndexShortCircuitEnabled(session)) {
            cardinalities = cardinalityCache.getCardinalities(
                    table.getSchema(),
                    table.getTable(),
                    indexQueryParameters,
                    auths,
                    getSmallestCardinalityThreshold(session, numRows),
                    AccumuloSessionProperties.getIndexCardinalityCachePollingDuration(session),
                    metricsStorage);
        }
        else {
            // disable short circuit using 0
            cardinalities = cardinalityCache.getCardinalities(
                    table.getSchema(),
                    table.getTable(),
                    indexQueryParameters,
                    auths,
                    0,
                    new Duration(0, MILLISECONDS),
                    metricsStorage);
        }

        Optional<Entry<Long, IndexQueryParameters>> entry = cardinalities.entries().stream().findFirst();
        if (!entry.isPresent()) {
            return false;
        }

        Entry<Long, IndexQueryParameters> lowestCardinality = entry.get();
        String indexTable = table.getIndexTableName();
        double threshold = AccumuloSessionProperties.getIndexThreshold(session);
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
            ImmutableList.Builder<IndexQueryParameters> builder = ImmutableList.builder();
            cardinalities.entries().stream().filter(x -> x.getKey() < maxIndexLookup).map(Entry::getValue).forEach(queryParameter -> {
                LOG.debug(format("Cardinality of column %s is below the max index lookup threshold %s, added for intersection", queryParameter.getIndexColumn(), maxIndexLookup));
                builder.add(queryParameter);
            });
            List<IndexQueryParameters> intersectionColumns = builder.build();

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
            // Else, we don't need to intersect the columns and we can just use the column with the lowest cardinality
            LOG.debug("Not intersecting columns, using column with lowest cardinality: " + lowestCardinality.getValue().getIndexColumn());
            indexRanges = getIndexRanges(indexTable, ImmutableList.of(lowestCardinality.getValue()), rowIdRanges, auths);
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

    private static List<IndexQueryParameters> getIndexQueryParameters(AccumuloTable table, Collection<AccumuloColumnConstraint> constraints)
            throws AccumuloSecurityException, AccumuloException
    {
        if (table.getParsedIndexColumns().size() == 0) {
            return ImmutableList.of();
        }

        AccumuloRowSerializer serializer = table.getSerializerInstance();

        // initialize list of index query parameters
        List<IndexQueryParameters> queryParameters = new ArrayList<>(table.getParsedIndexColumns().size());

        // For each index column
        NEXT_INDEX_COLUMN:
        for (IndexColumn indexColumn : table.getParsedIndexColumns()) {
            // create index query parameters
            IndexQueryParameters parameters = new IndexQueryParameters(indexColumn);

            // for each column in the index column
            for (String column : indexColumn.getColumns()) {
                // iterate through the constraints to find the matching column
                Optional<AccumuloColumnConstraint> optionalIndexedConstraint = constraints.stream().filter(constraint -> constraint.getName().equals(column)).findAny();
                if (!optionalIndexedConstraint.isPresent()) {
                    // We can skip this index column since we don't have a constraint on it
                    continue NEXT_INDEX_COLUMN;
                }

                AccumuloColumnConstraint indexedConstraint = optionalIndexedConstraint.get();

                // if found, convert domain to list of ranges and append to our parameters list
                parameters.appendColumn(
                        getIndexColumnFamily(indexedConstraint.getFamily().getBytes(UTF_8), indexedConstraint.getQualifier().getBytes(UTF_8)),
                        getRangesFromDomain(indexedConstraint.getDomain(), serializer),
                        table.isTruncateTimestamps() && indexedConstraint.getType().equals(TIMESTAMP));
            }

            queryParameters.add(parameters);
        }

        // Sweep through index columns to prune subsets
        ImmutableList.Builder<IndexQueryParameters> prunedQueryParameters = ImmutableList.builder();
        queryParameters.forEach(queryParameter -> {
            Optional<IndexQueryParameters> add = queryParameters.stream().filter(x -> !x.equals(queryParameter)).filter(that -> {
                // To test if we are going to keep this queryParameter, intersect it with 'that' query parameter
                int numInCommon = Sets.intersection(Sets.newHashSet(queryParameter.getIndexColumn().getColumns()), Sets.newHashSet(that.getIndexColumn().getColumns())).size();

                // If the number of columns this queryParameter has with that query parameter is the same,
                // then 'that' queryParameter subsumes this one
                // We return true here to signify that we should *not* add this parameter
                return numInCommon == queryParameter.getIndexColumn().getNumColumns();
            }).findAny();

            if (!add.isPresent()) {
                prunedQueryParameters.add(queryParameter);
            }
        });

        // return list of index query parameters
        return prunedQueryParameters.build();
    }

    private static boolean smallestCardAboveThreshold(ConnectorSession session, long numRows, long smallestCardinality)
    {
        long threshold = getSmallestCardinalityThreshold(session, numRows);
        LOG.info("Smallest cardinality is %d, num rows is %d, threshold is %d", smallestCardinality, numRows, threshold);
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

    private List<Range> getIndexRanges(String indexTable, List<IndexQueryParameters> indexParameters, Collection<AccumuloRange> rowIDRanges, Authorizations auths)
            throws TableNotFoundException, InterruptedException
    {
        Set<Range> finalRanges = new HashSet<>();
        // For each column/constraint pair we submit a task to scan the index ranges
        List<Callable<Set<Range>>> tasks = new ArrayList<>();
        for (IndexQueryParameters queryParameters : indexParameters) {
            Callable<Set<Range>> task = () -> {
                BatchScanner scanner = null;
                try {
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

                    LOG.info("Retrieved %d ranges for index column %s took %s ms", columnRanges.size(), queryParameters.getIndexColumn(), System.currentTimeMillis() - start);
                    return columnRanges;
                }
                finally {
                    if (scanner != null) {
                        scanner.close();
                    }
                }
            };
            tasks.add(task);
        }

        executor.invokeAll(tasks).forEach(future ->
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
    private static boolean inRange(Text text, Collection<AccumuloRange> ranges)
    {
        Key qualifier = new Key(text);
        return ranges.stream().map(AccumuloRange::getRange).anyMatch(r -> !r.beforeStartKey(qualifier) && !r.afterEndKey(qualifier));
    }
}
