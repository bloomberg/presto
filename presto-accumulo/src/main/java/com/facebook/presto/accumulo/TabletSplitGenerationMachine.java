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
package com.facebook.presto.accumulo;

import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.index.ColumnCardinalityCache;
import com.facebook.presto.accumulo.index.IndexLookup;
import com.facebook.presto.accumulo.index.IndexQueryParameters;
import com.facebook.presto.accumulo.index.Indexer;
import com.facebook.presto.accumulo.index.metrics.MetricsReader;
import com.facebook.presto.accumulo.index.metrics.MetricsStorage;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.model.AccumuloColumnConstraint;
import com.facebook.presto.accumulo.model.AccumuloRange;
import com.facebook.presto.accumulo.model.IndexColumn;
import com.facebook.presto.accumulo.model.TabletSplitMetadata;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.airlift.log.Logger;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.accumulo.AccumuloErrorCode.EXCEEDED_INDEX_THRESHOLD;
import static com.facebook.presto.accumulo.AccumuloErrorCode.UNEXPECTED_ACCUMULO_ERROR;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.getIndexDistributionThreshold;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.getIndexMaximumThreshold;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.getIndexSmallCardRowThreshold;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.getIndexSmallCardThreshold;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.getIndexThreshold;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.getMaxRowsPerSplit;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.getMinRowsPerSplit;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.getNumIndexRowsPerSplit;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.getScanUsername;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.getSplitsPerWorker;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.isIndexMetricsEnabled;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.isOptimizeIndexEnabled;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.isOptimizeNumRowsPerSplitEnabled;
import static com.facebook.presto.accumulo.index.Indexer.getIndexColumnFamily;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class TabletSplitGenerationMachine
{
    private static final Logger LOG = Logger.get(TabletSplitGenerationMachine.class);
    private static final Splitter COMMA_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

    private enum State
    {
        NONE,
        METRICS,
        INDEX,
        DISTRIBUTE,
        FULL,
        DONE
    }

    private final ColumnCardinalityCache cardinalityCache;
    private final Connector connector;
    private final int maxIndexLookup;

    @Inject
    public TabletSplitGenerationMachine(AccumuloConfig config, Connector connector, ColumnCardinalityCache cardinalityCache)
    {
        this.connector = requireNonNull(connector, "connector is null");
        this.cardinalityCache = requireNonNull(cardinalityCache, "cardinalityCache is null");
        this.maxIndexLookup = config.getMaxIndexLookupCardinality();
    }

    public List<TabletSplitMetadata> getTabletSplits(
            ConnectorSession session,
            Authorizations auths,
            AccumuloTable table,
            Optional<Domain> rowIdDomain,
            List<AccumuloColumnConstraint> constraints,
            int numWorkers)
    {
        return new TabletSplitGenerationTask(session, auths, table, rowIdDomain, constraints, maxIndexLookup, numWorkers).run();
    }

    private class TabletSplitGenerationTask
    {
        private final AccumuloTable table;
        private final Authorizations auths;
        private final ConnectorSession session;
        private final MetricsReader metricsReader;
        private final MetricsStorage metricsStorage;
        private final List<AccumuloColumnConstraint> constraints;
        private final Optional<Domain> rowIdDomain;
        private final String tableName;
        private final int maxIndexLookup;
        private final int numWorkers;

        private Collection<AccumuloRange> rowIdRanges;
        private List<IndexQueryParameters> indexQueryParameters;
        private List<TabletSplitMetadata> tabletSplits = null;
        private State state = State.NONE;
        private double threshold;
        private Long numRows = null;

        public TabletSplitGenerationTask(ConnectorSession session, Authorizations auths, AccumuloTable table, Optional<Domain> rowIdDomain, List<AccumuloColumnConstraint> constraints, int maxIndexLookup, int numWorkers)
        {
            this.session = requireNonNull(session, "session is null");
            this.auths = requireNonNull(auths, "auths is null");
            this.table = requireNonNull(table, "table is null");
            this.rowIdDomain = requireNonNull(rowIdDomain, "rowIdDomain is null");
            this.constraints = requireNonNull(constraints, "constraints is null");
            this.maxIndexLookup = maxIndexLookup;
            this.numWorkers = numWorkers;
            this.metricsStorage = table.getMetricsStorageInstance(connector);
            this.metricsReader = metricsStorage.newReader();

            this.threshold = getIndexThreshold(session);
            this.tableName = table.getFullTableName();
        }

        public List<TabletSplitMetadata> run()
        {
            LOG.debug("Getting tablet splits for table %s", tableName);

            try {
                this.rowIdRanges = getRangesFromDomain(rowIdDomain, table.getSerializerInstance());

                numRows = table.isIndexed() ? metricsReader.getNumRowsInTable(table.getSchema(), table.getTable()) : -1;

                setInitialState();

                while (state != State.DONE) {
                    switch (state) {
                        case METRICS:
                            if (ensureQueryParameters()) {
                                break;
                            }

                            handleMetricsState();
                            break;
                        case DISTRIBUTE:
                            handleDistributeState();
                            break;
                        case INDEX:
                            if (ensureQueryParameters()) {
                                break;
                            }

                            handleIndexState();
                            break;
                        case FULL:
                            handleFullState();
                            break;
                        case DONE:
                            throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, "TabletSplitGenerationMachine is in an unexpected DONE state");
                        case NONE:
                            throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, "TabletSplitGenerationMachine is in an unexpected NONE state");
                    }
                }
            }
            catch (Exception e) {
                // Re-throw any instances of a PrestoException rather than wrapping it again
                if (e instanceof PrestoException) {
                    throw (PrestoException) e;
                }

                throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Failed to get splits from Accumulo", e);
            }

            if (tabletSplits == null) {
                throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, "TabletSplitGenerationMachine finished with null tablet splits");
            }

            return ImmutableList.copyOf(tabletSplits);
        }

        private void setInitialState()
        {
            if (table.getParsedIndexColumns().isEmpty()) {
                state = State.FULL;
            }
            else {
                if (isOptimizeIndexEnabled(session)) {
                    LOG.debug("Index is enabled");
                    if (isIndexMetricsEnabled(session)) {
                        LOG.debug("Metrics are enabled");
                        state = State.METRICS;
                    }
                    else {
                        LOG.debug("Metrics are disabled");
                        state = State.INDEX;
                    }
                }
                else {
                    LOG.debug("Index is disabled");
                    state = State.FULL;
                }
            }
        }

        /**
         * Parses the query parameters to make sure they are set if not already
         *
         * @return true if the state changed, false otherwise
         */
        private boolean ensureQueryParameters()
                throws Exception
        {
            if (indexQueryParameters != null) {
                return false;
            }

            // Collect Accumulo ranges for each indexed column constraint
            indexQueryParameters = getIndexQueryParameters(table, constraints);

            // If there is no constraints on an index column, we will bail out to a full table scan
            if (indexQueryParameters.isEmpty()) {
                LOG.debug("Query contains no constraints on indexed columns, skipping secondary index");
                state = State.FULL;
                return true;
            }

            return false;
        }

        private void handleMetricsState()
                throws Exception
        {
            checkState(state == State.METRICS, "State machine is not set to METRICS");
            requireNonNull(indexQueryParameters, "Index query parameters are null");
            requireNonNull(numRows, "Number of rows is null");

            // Get the cardinalities from the metrics table
            Multimap<Long, IndexQueryParameters> cardinalities = cardinalityCache.getCardinalities(
                    session,
                    table.getSchema(),
                    table.getTable(),
                    getScanAuthorizations(session, table),
                    indexQueryParameters,
                    getSmallestCardinalityThreshold(session, numRows),
                    metricsStorage);

            Optional<Map.Entry<Long, IndexQueryParameters>> entry = cardinalities.entries().stream().findFirst();
            if (!entry.isPresent()) {
                // If we have no metrics, fall back to the index
                state = State.INDEX;
                return;
            }

            Map.Entry<Long, IndexQueryParameters> lowestCardinality = entry.get();

            // If the smallest cardinality in our list is above the lowest cardinality threshold,
            // we should look at intersecting the row ID ranges to try and get under the threshold.
            if (smallestCardAboveThreshold(session, numRows, lowestCardinality.getKey())) {
                // If we only have one column, we can check the index threshold without doing the row intersection
                if (cardinalities.size() == 1) {
                    long numEntries = lowestCardinality.getKey();
                    double ratio = ((double) numEntries / (double) numRows);
                    LOG.debug("Use of index would scan %d of %d rows, ratio %s. Threshold %2f, Using for index table? %b", numEntries, numRows, ratio, threshold, ratio < threshold);
                    if (ratio >= threshold) {
                        state = State.FULL;
                        return;
                    }
                }

                // Else, we are going to use the index, now we just need to figure out if we are going to distribute it

                // If there is only one intersection column and it is greater than some threshold to distribute it to the workers
                if (canDistributeIndexLookup(indexQueryParameters)) {
                    state = State.DISTRIBUTE;
                    return;
                }

                // Else, we will try row intersection

                // Remove columns with a large number of rows that would otherwise blow up the coordinator JVM
                ImmutableList.Builder<IndexQueryParameters> builder = ImmutableList.builder();
                cardinalities.entries().stream().filter(x -> x.getKey() < maxIndexLookup).map(Map.Entry::getValue).forEach(queryParameter -> {
                    LOG.debug(format("Cardinality of column %s is below the max index lookup threshold %s, added for intersection", queryParameter.getIndexColumn(), maxIndexLookup));
                    builder.add(queryParameter);
                });
                List<IndexQueryParameters> intersectionColumns = builder.build();

                // If there are columns to do row intersection, then do so
                if (intersectionColumns.size() > 0) {
                    LOG.debug("%d indexed columns, intersecting ranges", intersectionColumns.size());
                    indexQueryParameters = intersectionColumns;
                    state = State.INDEX;
                }
                else {
                    LOG.debug("No columns have few enough entries to allow intersection, doing a full table scan");
                    state = State.FULL;
                }
            }
            else if (canDistributeIndexLookup(indexQueryParameters)) {
                state = State.DISTRIBUTE;
            }
            else {
                // Else, we don't need to intersect the columns and we can just use the column with the lowest cardinality
                LOG.debug("Not intersecting columns, using column with lowest cardinality: " + lowestCardinality.getValue().getIndexColumn());
                indexQueryParameters = ImmutableList.of(lowestCardinality.getValue());
                state = State.INDEX;
            }
        }

        private boolean canDistributeIndexLookup(List<IndexQueryParameters> queryParameters)
        {
            long threshold = getIndexDistributionThreshold(session);
            if (threshold == 0) {
                LOG.debug("Distribution of index is disabled by session property");
                return false;
            }
            else if (queryParameters.size() != 1) {
                LOG.debug("Distribution of index is disabled, too many query parameters to distribute index.  Expected 1, actual " + queryParameters.size());
                return false;
            }
            else if (queryParameters.get(0).getCardinality() < threshold) {
                LOG.debug("Distribution of index is disabled, cardinality of query column is too large.  Needs to be greater than or equal to " + threshold);
                return false;
            }
            else if (queryParameters.get(0).getRanges().stream().anyMatch(Indexer::isExact)) {
                LOG.debug("Distribution of index is disabled, query contains an exact key lookup.  Distribution is only supported for range scans");
                return false;
            }

            LOG.debug("Distribution of index is a go!");
            return true;
        }

        private void handleDistributeState()
                throws Exception
        {
            checkState(state == State.DISTRIBUTE, "State machine is not set to DISTRIBUTE");
            requireNonNull(indexQueryParameters, "Index query parameters are null");
            checkState(indexQueryParameters.size() == 1, "Size of index query parameters is not one but " + indexQueryParameters.size());
            checkState(indexQueryParameters.get(0).hasCardinality(), "Index query parameters does not have a cardinality set");
            requireNonNull(numRows, "Number of rows is null");

            IndexQueryParameters params = indexQueryParameters.get(0);
            int numSplits = (int) params.getCardinality() / optimizeNumRowsPerSplit(session, params.getCardinality(), numWorkers);

            if (numSplits > 0) {
                ImmutableList.Builder<TabletSplitMetadata> builder = ImmutableList.builder();
                for (IndexQueryParameters splitParams : params.split(numSplits)) {
                    builder.add(new TabletSplitMetadata(ImmutableList.of(), rowIdRanges, Optional.of(splitParams)));
                }
                tabletSplits = builder.build();
            }
            else {
                tabletSplits = new ArrayList<>();
                tabletSplits.add(new TabletSplitMetadata(ImmutableList.of(), rowIdRanges, Optional.of(params)));
            }

            LOG.info("Distributing %s tablet splits to worker nodes for index retrieval", tabletSplits.size());
            state = State.DONE;
        }

        private void handleIndexState()
                throws Exception
        {
            checkState(state == State.INDEX, "State machine is not set to INDEX");
            requireNonNull(indexQueryParameters, "Index query parameters are null");
            requireNonNull(numRows, "Number of rows is null");

            // Get the ranges via the index table
            List<Range> indexRanges = new IndexLookup().getIndexRanges(connector, session, indexQueryParameters, rowIdRanges, getScanAuthorizations(session, table));
            if (!indexRanges.isEmpty()) {
                // Okay, we now check how many rows we would scan by using the index vs. the overall number of rows
                long numEntries = indexRanges.size();
                double ratio = (double) numEntries / (double) numRows;
                LOG.debug("Use of index would scan %d of %d rows, ratio %s. Threshold %2f, Using for table? %b", numEntries, numRows, ratio, threshold, ratio < threshold, table);

                // If the percentage of scanned rows, the ratio, less than the configured threshold
                if (ratio < threshold) {
                    // Bin the ranges into TabletMetadataSplits and return true to use the tablet splits
                    tabletSplits = new ArrayList<>();
                    binRanges(optimizeNumRowsPerSplit(session, indexRanges.size(), numWorkers), indexRanges, rowIdRanges, tabletSplits);
                    LOG.debug("Number of splits for %s is %d with %d ranges", table.getFullTableName(), tabletSplits.size(), indexRanges.size());
                    state = State.DONE;
                }
                else {
                    state = State.FULL;
                }
            }
            else {
                LOG.debug("Query would return no results, returning empty list of splits");
                tabletSplits = ImmutableList.of();
                state = State.DONE;
            }
        }

        private void handleFullState()
                throws AccumuloSecurityException, TableNotFoundException, AccumuloException
        {
            checkState(state == State.FULL, "State machine is not set to FULL");

            long threshold = getIndexMaximumThreshold(session);
            if (numRows > threshold) {
                throw new PrestoException(EXCEEDED_INDEX_THRESHOLD, format("Refusing to execute this query: Index lookup cannot be distributed to workers and the number of rows in the table (%s) is greater than index_maximum_threshold (%s)", numRows, threshold));
            }
            else if (numRows < 0) {
                LOG.debug("Executing full table scan against a non-indexed table.  Unable to determine if this is a bad idea");
            }

            tabletSplits = new ArrayList<>();

            // Split the ranges on tablet boundaries
            Collection<Range> splitRanges = splitByTabletBoundaries(tableName, rowIdRanges.stream().map(AccumuloRange::getRange).collect(Collectors.toList()));

            // Create TabletSplitMetadata objects for each range
            for (Range range : splitRanges) {
                // else, just use the default location
                tabletSplits.add(new TabletSplitMetadata(ImmutableList.of(range), rowIdRanges, Optional.empty()));
            }

            // Log some fun stuff and return the tablet splits
            LOG.debug("Number of splits for table %s is %d with %d ranges", tableName, tabletSplits.size(), splitRanges.size());
            state = State.DONE;
        }

        /**
         * Gets the scan authorizations to use for scanning tables.
         * <p>
         * In order of priority: session username authorizations, then table property, then the default connector auths.
         *
         * @param session Current session
         * @param table Table metadata
         * @return Scan authorizations
         * @throws AccumuloException If a generic Accumulo error occurs
         * @throws AccumuloSecurityException If a security exception occurs
         */
        private Authorizations getScanAuthorizations(ConnectorSession session, AccumuloTable table)
                throws AccumuloException, AccumuloSecurityException
        {
            String sessionScanUser = getScanUsername(session);
            if (sessionScanUser != null) {
                Authorizations scanAuths = connector.securityOperations().getUserAuthorizations(sessionScanUser);
                LOG.debug("Using session scan auths for user %s: %s", sessionScanUser, scanAuths);
                return scanAuths;
            }

            Optional<String> strAuths = table.getScanAuthorizations();
            if (strAuths.isPresent()) {
                Authorizations scanAuths = new Authorizations(Iterables.toArray(COMMA_SPLITTER.split(strAuths.get()), String.class));
                LOG.debug("scan_auths table property set, using: %s", scanAuths);
                return scanAuths;
            }

            LOG.debug("scan_auths table property not set, using connector auths: %s", this.auths);
            return this.auths;
        }

        private Collection<Range> splitByTabletBoundaries(String tableName, Collection<Range> ranges)
                throws org.apache.accumulo.core.client.TableNotFoundException, AccumuloException, AccumuloSecurityException
        {
            ImmutableSet.Builder<Range> rangeBuilder = ImmutableSet.builder();
            for (Range range : ranges) {
                // if start and end key are equivalent, no need to split the range
                if (range.getStartKey() != null && range.getEndKey() != null && range.getStartKey().equals(range.getEndKey())) {
                    rangeBuilder.add(range);
                }
                else {
                    // Call out to Accumulo to split the range on tablets
                    rangeBuilder.addAll(connector.tableOperations().splitRangeByTablets(tableName, range, Integer.MAX_VALUE));
                }
            }
            return rangeBuilder.build();
        }

        private int optimizeNumRowsPerSplit(ConnectorSession session, long numRowIDs, int numWorkers)
        {
            if (isOptimizeNumRowsPerSplitEnabled(session)) {
                int min = getMinRowsPerSplit(session);

                if (numRowIDs <= min) {
                    LOG.debug("RowsPerSplit " + numRowIDs);
                    return (int) numRowIDs;
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
    }

    /**
     * Gets a collection of Accumulo Range objects from the given Presto domain.
     * This maps the column constraints of the given Domain to an Accumulo Range scan.
     *
     * @param domain Domain, can be null (returns (-inf, +inf) Range)
     * @param serializer Instance of an {@link AccumuloRowSerializer}
     * @return A collection of Accumulo Range objects
     */
    public static Collection<AccumuloRange> getRangesFromDomain(Optional<Domain> domain, AccumuloRowSerializer serializer)
    {
        // if we have no predicate pushdown, use the full range
        if (!domain.isPresent()) {
            return ImmutableSet.of(new AccumuloRange());
        }

        ImmutableSet.Builder<AccumuloRange> rangeBuilder = ImmutableSet.builder();
        for (com.facebook.presto.spi.predicate.Range range : domain.get().getValues().getRanges().getOrderedRanges()) {
            rangeBuilder.add(getRangeFromPrestoRange(range, serializer));
        }

        return rangeBuilder.build();
    }

    public static AccumuloRange getRangeFromPrestoRange(com.facebook.presto.spi.predicate.Range prestoRange, AccumuloRowSerializer serializer)
    {
        AccumuloRange accumuloRange;
        if (prestoRange.isAll()) {
            accumuloRange = new AccumuloRange();
        }
        else if (prestoRange.isSingleValue()) {
            accumuloRange = new AccumuloRange(serializer.encode(prestoRange.getType(), prestoRange.getSingleValue()));
        }
        else {
            if (prestoRange.getLow().isLowerUnbounded()) {
                // If low is unbounded, then create a range from (-inf, value), checking inclusivity
                boolean inclusive = prestoRange.getHigh().getBound() == Marker.Bound.EXACTLY;
                byte[] split = serializer.encode(prestoRange.getType(), prestoRange.getHigh().getValue());
                accumuloRange = new AccumuloRange(null, false, split, inclusive);
            }
            else if (prestoRange.getHigh().isUpperUnbounded()) {
                // If high is unbounded, then create a range from (value, +inf), checking inclusivity
                boolean inclusive = prestoRange.getLow().getBound() == Marker.Bound.EXACTLY;
                byte[] split = serializer.encode(prestoRange.getType(), prestoRange.getLow().getValue());
                accumuloRange = new AccumuloRange(split, inclusive, null, false);
            }
            else {
                // If high is unbounded, then create a range from low to high, checking inclusivity
                boolean startKeyInclusive = prestoRange.getLow().getBound() == Marker.Bound.EXACTLY;
                byte[] startSplit = serializer.encode(prestoRange.getType(), prestoRange.getLow().getValue());

                boolean endKeyInclusive = prestoRange.getHigh().getBound() == Marker.Bound.EXACTLY;
                byte[] endSplit = serializer.encode(prestoRange.getType(), prestoRange.getHigh().getValue());
                accumuloRange = new AccumuloRange(startSplit, startKeyInclusive, endSplit, endKeyInclusive);
            }
        }

        return accumuloRange;
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

    private static void binRanges(int numRangesPerBin, List<Range> splitRanges, Collection<AccumuloRange> rowIdRanges, List<TabletSplitMetadata> prestoSplits)
    {
        checkArgument(numRangesPerBin > 0, "number of ranges per bin must be greater than zero");
        int toAdd = splitRanges.size();
        int fromIndex = 0;
        int toIndex = Math.min(toAdd, numRangesPerBin);
        do {
            // Add the sublist of range handles
            // Use an empty location because we are binning multiple Ranges spread across many tablet servers
            prestoSplits.add(new TabletSplitMetadata(splitRanges.subList(fromIndex, toIndex), rowIdRanges, Optional.empty()));
            toAdd -= toIndex - fromIndex;
            fromIndex = toIndex;
            toIndex += Math.min(toAdd, numRangesPerBin);
        }
        while (toAdd > 0);
    }

    private static boolean smallestCardAboveThreshold(ConnectorSession session, long numRows, long smallestCardinality)
    {
        long threshold = getSmallestCardinalityThreshold(session, numRows);
        LOG.debug("Smallest cardinality is %d, num rows is %d, threshold is %d", smallestCardinality, numRows, threshold);
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
}
