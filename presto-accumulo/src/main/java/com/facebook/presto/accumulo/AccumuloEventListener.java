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
import com.facebook.presto.accumulo.index.metrics.AccumuloMetricsStorage;
import com.facebook.presto.accumulo.index.metrics.MetricsStorage;
import com.facebook.presto.accumulo.io.PrestoBatchWriter;
import com.facebook.presto.accumulo.iterators.ListCombiner;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.metadata.ZooKeeperMetadataManager;
import com.facebook.presto.accumulo.model.IndexColumn;
import com.facebook.presto.accumulo.model.RowSchema;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.AbstractArrayBlock;
import com.facebook.presto.spi.block.ArrayBlockBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.QueryInputMetadata;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.AbstractType;
import com.facebook.presto.spi.type.ParametricType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.facebook.presto.accumulo.AccumuloErrorCode.UNEXPECTED_ACCUMULO_ERROR;
import static com.facebook.presto.accumulo.Types.getElementType;
import static com.facebook.presto.accumulo.serializers.AccumuloRowSerializer.getBlockFromArray;
import static com.facebook.presto.accumulo.serializers.AccumuloRowSerializer.getBlockFromMap;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.ROW;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class AccumuloEventListener
        implements EventListener
{
    public static final String SPLIT = "split";
    public static final Type SPLIT_TYPE;

    private static final AccumuloTable TABLE;
    private static final Logger LOG = Logger.get(AccumuloEventListener.class);
    private static final String FAILURE = "failure";
    private static final String INDEX_COLUMNS = "create_time,start_time,end_time";
    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String ROW_ID = "id";
    private static final String SCHEMA = "presto";
    private static final String TABLE_NAME = "query_archive";
    private static final String QUERY = "query";
    private static final Type FAILURE_TYPE;
    private static final Type INPUT_TYPE;
    private static final Type SESSION_PROPERTIES_TYPE = new MapType(VARCHAR, VARCHAR);

    private final AccumuloRowSerializer serializer = new LexicoderRowSerializer();

    static {
        RowSchema schema = new RowSchema();
        schema.addRowId("id", VARCHAR);
        schema.addColumn("source", Optional.of(QUERY), Optional.of("source"), VARCHAR);
        schema.addColumn("user", Optional.of(QUERY), Optional.of("user"), VARCHAR);
        schema.addColumn("user_agent", Optional.of(QUERY), Optional.of("user_agent"), VARCHAR);

        schema.addColumn("sql", Optional.of(QUERY), Optional.of("sql"), VARCHAR);
        schema.addColumn("session_properties", Optional.of(QUERY), Optional.of("session_properties"), SESSION_PROPERTIES_TYPE);
        schema.addColumn("schema", Optional.of(QUERY), Optional.of("schema"), VARCHAR);
        schema.addColumn("transaction_id", Optional.of(QUERY), Optional.of("transaction_id"), VARCHAR);
        schema.addColumn("state", Optional.of(QUERY), Optional.of("state"), VARCHAR);
        schema.addColumn("is_complete", Optional.of(QUERY), Optional.of("is_complete"), BOOLEAN);
        schema.addColumn("uri", Optional.of(QUERY), Optional.of("uri"), VARCHAR);

        schema.addColumn("create_time", Optional.of(QUERY), Optional.of("create_time"), TIMESTAMP);
        schema.addColumn("start_time", Optional.of(QUERY), Optional.of("start_time"), TIMESTAMP);
        schema.addColumn("end_time", Optional.of(QUERY), Optional.of("end_time"), TIMESTAMP);
        schema.addColumn("cpu_time", Optional.of(QUERY), Optional.of("cpu_time"), BIGINT);
        schema.addColumn("analysis_time", Optional.of(QUERY), Optional.of("analysis_time"), BIGINT);
        schema.addColumn("planning_time", Optional.of(QUERY), Optional.of("planning_time"), BIGINT);
        schema.addColumn("queued_time", Optional.of(QUERY), Optional.of("queued_time"), BIGINT);
        schema.addColumn("wall_time", Optional.of(QUERY), Optional.of("wall_time"), BIGINT);

        schema.addColumn("completed_splits", Optional.of(QUERY), Optional.of("completed_splits"), INTEGER);
        schema.addColumn("total_bytes", Optional.of(QUERY), Optional.of("total_bytes"), BIGINT);
        schema.addColumn("total_rows", Optional.of(QUERY), Optional.of("total_rows"), BIGINT);
        schema.addColumn("peak_mem_bytes", Optional.of(QUERY), Optional.of("peak_mem_bytes"), BIGINT);

        schema.addColumn("environment", Optional.of(QUERY), Optional.of("environment"), VARCHAR);
        schema.addColumn("remote_client_address", Optional.of(QUERY), Optional.of("remote_client_address"), VARCHAR);
        schema.addColumn("server_address", Optional.of(QUERY), Optional.of("server_address"), VARCHAR);
        schema.addColumn("server_version", Optional.of(QUERY), Optional.of("server_version"), VARCHAR);

        INPUT_TYPE = new ArrayType(new RowType(ImmutableList.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR), Optional.of(ImmutableList.of("connector_id", "table", "schema", "columns", "connector_info"))));
        schema.addColumn("inputs", Optional.of(INPUT), Optional.of(INPUT), INPUT_TYPE);

        schema.addColumn("output_connector_id", Optional.of(OUTPUT), Optional.of("connector_id"), VARCHAR);
        schema.addColumn("output_schema", Optional.of(OUTPUT), Optional.of("schema"), VARCHAR);
        schema.addColumn("output_table", Optional.of(OUTPUT), Optional.of("table"), VARCHAR);

        FAILURE_TYPE = new RowType(ImmutableList.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR), Optional.of(ImmutableList.of("code", "type", "msg", "task", "host", "json")));
        schema.addColumn("failure", Optional.of(FAILURE), Optional.of("failure"), FAILURE_TYPE);

        SPLIT_TYPE = new ArrayType(new RowType(
                ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT, TIMESTAMP, TIMESTAMP, VARCHAR, VARCHAR, BIGINT, VARCHAR, TIMESTAMP, VARCHAR, BIGINT, BIGINT, BIGINT, BIGINT),
                Optional.of(ImmutableList.of("completed_data_size_bytes", "completed_positions", "completed_read_time", "cpu_time", "create_time", "end_time", "failure_msg", "failure_type", "queued_time", "stage_id", "start_time", "task_id", "time_to_first_byte", "time_to_last_byte", "user_time", "wall_time"))));
        schema.addColumn("splits", Optional.of(SPLIT), Optional.of(SPLIT), SPLIT_TYPE);

        TABLE = new AccumuloTable(
                SCHEMA,
                TABLE_NAME,
                schema.getColumns(),
                ROW_ID,
                true,
                LexicoderRowSerializer.class.getCanonicalName(),
                Optional.empty(),
                Optional.of(AccumuloMetricsStorage.class.getCanonicalName()),
                true,
                Optional.of(INDEX_COLUMNS)
        );
    }

    private final AccumuloConfig config;
    private final BatchWriterConfig batchWriterConfig;
    private final Duration timeout;
    private final Duration latency;
    private final String user;
    private final BlockingQueue<MetadataUpdate> mutationQueue = new LinkedBlockingQueue<>();

    private Connector connector;
    private PrestoBatchWriter writer;

    public AccumuloEventListener(String instance, String zooKeepers, String user, String password, Duration timeout, Duration latency)
    {
        requireNonNull(instance, "instance is null");
        requireNonNull(zooKeepers, "zookeepers is null");
        this.user = requireNonNull(user, "user is null");
        requireNonNull(password, "password is null");
        this.timeout = requireNonNull(timeout, "timeout is null");
        this.latency = requireNonNull(latency, "latency is null");

        this.config = new AccumuloConfig().setInstance(instance).setZooKeepers(zooKeepers).setUsername(user).setPassword(password);
        this.batchWriterConfig = new BatchWriterConfig().setTimeout(timeout.toMillis(), MILLISECONDS).setMaxLatency(latency.toMillis(), MILLISECONDS);

        Thread writerThread = new Thread(new MutationWriter(instance, zooKeepers, password));
        writerThread.setName("event-listener-writer");
        writerThread.setDaemon(true);
        writerThread.start();
    }

    @Override
    public void queryCreated(QueryCreatedEvent event)
    {
        Mutation mutation = new Mutation(event.getMetadata().getQueryId());
        mutation.put(QUERY, "user", event.getContext().getUser());
        event.getContext().getPrincipal().ifPresent(principal -> mutation.put(QUERY, "principal", principal));
        event.getContext().getRemoteClientAddress().ifPresent(address -> mutation.put(QUERY, "remote_client_address", address));
        event.getContext().getUserAgent().ifPresent(agent -> mutation.put(QUERY, "user_agent", agent));
        event.getContext().getSource().ifPresent(source -> mutation.put(QUERY, "source", source));
        event.getContext().getSchema().ifPresent(schema -> mutation.put(QUERY, "schema", schema));

        if (!event.getContext().getSessionProperties().isEmpty()) {
            mutation.put(QUERY, "session_properties", new Value(serializer.encode(SESSION_PROPERTIES_TYPE, getBlockFromMap(SESSION_PROPERTIES_TYPE, event.getContext().getSessionProperties()))));
        }

        mutation.put(QUERY, "server_address", event.getContext().getServerAddress());
        mutation.put(QUERY, "server_version", event.getContext().getServerVersion());
        mutation.put(QUERY, "environment", event.getContext().getEnvironment());

        mutation.put(QUERY, "create_time", new Value(serializer.encode(TIMESTAMP, event.getCreateTime().getEpochSecond() * 1000L)));

        mutation.put(QUERY, "id", event.getMetadata().getQueryId());
        event.getMetadata().getTransactionId().ifPresent(transaction -> mutation.put(QUERY, "transaction_id", transaction));
        mutation.put(QUERY, "sql", event.getMetadata().getQuery());
        mutation.put(QUERY, "state", event.getMetadata().getQueryState());
        mutation.put(QUERY, "uri", event.getMetadata().getUri().toString());

        if (!mutationQueue.offer(new MetadataUpdate(mutation, true, true))) {
            LOG.warn("Failed to offer mutation to queue, skipping");
        }
    }

    @Override
    public void queryCompleted(QueryCompletedEvent event)
    {
        Mutation mutation = new Mutation(event.getMetadata().getQueryId());
        mutation.put(QUERY, "cpu_time", new Value(serializer.encode(BIGINT, event.getStatistics().getCpuTime().toMillis())));
        mutation.put(QUERY, "wall_time", new Value(serializer.encode(BIGINT, event.getStatistics().getWallTime().toMillis())));
        mutation.put(QUERY, "queued_time", new Value(serializer.encode(BIGINT, event.getStatistics().getQueuedTime().toMillis())));
        event.getStatistics().getAnalysisTime().ifPresent(time -> mutation.put(QUERY, "analysis_time", new Value(serializer.encode(BIGINT, time.toMillis()))));
        event.getStatistics().getDistributedPlanningTime().ifPresent(time -> mutation.put(QUERY, "planning_time", new Value(serializer.encode(BIGINT, time.toMillis()))));
        mutation.put(QUERY, "peak_mem_bytes", new Value(serializer.encode(BIGINT, event.getStatistics().getPeakMemoryBytes())));
        mutation.put(QUERY, "total_bytes", new Value(serializer.encode(BIGINT, event.getStatistics().getTotalBytes())));
        mutation.put(QUERY, "total_rows", new Value(serializer.encode(BIGINT, event.getStatistics().getTotalRows())));
        mutation.put(QUERY, "completed_splits", new Value(serializer.encode(INTEGER, event.getStatistics().getCompletedSplits())));

        mutation.put(QUERY, "is_complete", new Value(serializer.encode(BOOLEAN, event.getStatistics().isComplete())));
        mutation.put(QUERY, "cpu_time", new Value(serializer.encode(BIGINT, event.getStatistics().getCpuTime().toMillis())));

        mutation.put(QUERY, "state", event.getMetadata().getQueryState());

        if (event.getIoMetadata().getInputs().size() > 0) {
            appendInputs(mutation, event.getIoMetadata().getInputs());
        }

        event.getIoMetadata().getOutput().ifPresent(output -> {
            mutation.put(OUTPUT, "connector_id", output.getConnectorId());
            mutation.put(OUTPUT, "schema", output.getSchema());
            mutation.put(OUTPUT, "table", output.getTable());
        });

        if (event.getFailureInfo().isPresent()) {
            mutation.put(FAILURE, "code", event.getFailureInfo().get().getErrorCode().toString());
            event.getFailureInfo().get().getFailureType().ifPresent(type -> mutation.put(FAILURE, "type", type));
            event.getFailureInfo().get().getFailureMessage().ifPresent(message -> mutation.put(FAILURE, "msg", message));
            event.getFailureInfo().get().getFailureTask().ifPresent(task -> mutation.put(FAILURE, "task", task));
            event.getFailureInfo().get().getFailureHost().ifPresent(host -> mutation.put(FAILURE, "host", host));
            mutation.put(FAILURE, "json", event.getFailureInfo().get().getFailuresJson());
        }

        mutation.put(QUERY, "start_time", new Value(serializer.encode(TIMESTAMP, event.getExecutionStartTime().getEpochSecond() * 1000L)));
        mutation.put(QUERY, "end_time", new Value(serializer.encode(TIMESTAMP, event.getEndTime().getEpochSecond() * 1000L)));

        if (!mutationQueue.offer(new MetadataUpdate(mutation, false, true))) {
            LOG.warn("Failed to offer mutation to queue, skipping");
        }
    }

    @Override
    public void splitCompleted(SplitCompletedEvent event)
    {
        Mutation mutation = new Mutation(event.getQueryId());

        // "completed_data_size_bytes", "completed_positions", "completed_read_time", "cpu_time", "create_time", "end_time"
        // We use an ArrayList here to allow null elements
        List<Object> row = new ArrayList<>();
        row.add(event.getStatistics().getCompletedDataSizeBytes());
        row.add(event.getStatistics().getCompletedPositions());
        row.add(event.getStatistics().getCompletedReadTime().toMillis());
        row.add(event.getStatistics().getCpuTime().toMillis());
        row.add(event.getCreateTime().getEpochSecond() * 1000L);
        row.add(event.getEndTime().isPresent() ? event.getEndTime().get().getEpochSecond() * 1000L : null);

        // "failure_msg", "failure_type"
        if (event.getFailureInfo().isPresent()) {
            row.add(event.getFailureInfo().get().getFailureMessage());
            row.add(event.getFailureInfo().get().getFailureType());
        }
        else {
            row.add(null);
            row.add(null);
        }

        // "queued_time", "stage_id", "start_time", "task_id", "time_to_first_byte", "time_to_last_byte", "user_time", "wall_time"
        row.add(event.getStatistics().getQueuedTime().toMillis());
        row.add(event.getStageId());
        row.add(event.getStartTime().isPresent() ? event.getStartTime().get().getEpochSecond() * 1000L : null);
        row.add(event.getTaskId());
        row.add(event.getStatistics().getTimeToFirstByte().isPresent() ? event.getStatistics().getTimeToFirstByte().get().toMillis() : null);
        row.add(event.getStatistics().getTimeToLastByte().isPresent() ? event.getStatistics().getTimeToLastByte().get().toMillis() : null);
        row.add(event.getStatistics().getUserTime().toMillis());
        row.add(event.getStatistics().getWallTime().toMillis());

        mutation.put(SPLIT, SPLIT, new Value(serializer.encode(SPLIT_TYPE, getBlockFromArray(getElementType(SPLIT_TYPE), ImmutableList.of(row)))));

        if (!mutationQueue.offer(new MetadataUpdate(mutation, false, false))) {
            LOG.warn("Failed to offer mutation to queue, skipping");
        }
    }

    private void appendInputs(Mutation mutation, List<QueryInputMetadata> inputs)
    {
        // "connector_id", "table", "schema", "columns", "connector_info"
        ImmutableList.Builder<List<Object>> inputBuilder = ImmutableList.builder();
        inputs.forEach(input -> {
            // We use an ArrayList here to allow null elements
            List<Object> row = new ArrayList<>();
            row.add(input.getConnectorId());
            row.add(input.getTable());
            row.add(input.getSchema());
            row.add(input.getColumns().toString());
            row.add(input.getConnectorInfo().isPresent() ? input.getConnectorInfo().get().toString() : null);
            inputBuilder.add(row);
        });

        mutation.put(INPUT, INPUT, new Value(serializer.encode(INPUT_TYPE, getBlockFromArray(getElementType(INPUT_TYPE), inputBuilder.build()))));
    }

    private void createBatchWriter()
    {
        if (writer != null) {
            try {
                writer.close();
            }
            catch (MutationsRejectedException e) {
                LOG.error("Failed to close writer, sleeping for 10s", e);
            }
        }

        writer = null;
        do {
            try {
                createTableIfNotExists();
                writer = new PrestoBatchWriter(connector, connector.securityOperations().getUserAuthorizations(user), TABLE, batchWriterConfig);
                LOG.info("Created BatchWriter against table %s with timeout %s and latency %s", TABLE.getFullTableName(), timeout, latency);
            }
            catch (TableNotFoundException e) {
                LOG.warn("Table %s not found. Recreating...", TABLE.getFullTableName());
                createTableIfNotExists();
            }
            catch (AccumuloSecurityException e) {
                LOG.error("Security exception getting user authorizations: " + e.getMessage());
                break;
            }
            catch (AccumuloException e) {
                LOG.error("Unexpected Accumulo exception getting user authorizations: " + e.getMessage());
                break;
            }
            catch (Exception e) {
                LOG.error("Unexpected exception trying to create batch writer: " + e.getMessage());
                break;
            }
        }
        while (writer == null);
    }

    private void createTableIfNotExists()
    {
        ZooKeeperMetadataManager metadataManager = new ZooKeeperMetadataManager(config, new DummyTypeManager() {});
        if (!metadataManager.exists(TABLE.getSchemaTableName())) {
            metadataManager.createTableMetadata(TABLE);
            LOG.info("Created metadata for table " + TABLE.getSchemaTableName());
        }

        AccumuloTableManager tableManager = new AccumuloTableManager(connector);
        tableManager.ensureNamespace(TABLE.getSchema());
        if (!tableManager.exists(TABLE.getFullTableName())) {
            tableManager.createAccumuloTable(TABLE.getFullTableName());

            try {
                IteratorSetting setting = new IteratorSetting(19, ListCombiner.class);
                ListCombiner.setCombineAllColumns(setting, false);
                ListCombiner.setColumns(setting, ImmutableList.of(new Column(SPLIT, SPLIT)));
                connector.tableOperations().attachIterator(TABLE.getFullTableName(), setting);
                LOG.info("Created Accumulo table %s", TABLE.getFullTableName());
            }
            catch (AccumuloSecurityException | AccumuloException e) {
                throw new PrestoException(AccumuloErrorCode.UNEXPECTED_ACCUMULO_ERROR, "Failed to set iterator", e);
            }
            catch (TableNotFoundException e) {
                LOG.warn("Failed to set iterator, table %s does not exist", TABLE.getFullTableName());
            }
        }

        for (IndexColumn indexColumn : TABLE.getParsedIndexColumns()) {
            if (!tableManager.exists(indexColumn.getIndexTable())) {
                tableManager.createAccumuloTable(indexColumn.getIndexTable());
                LOG.info("Created Accumulo table %s", indexColumn.getIndexTable());
            }
        }

        MetricsStorage storage = TABLE.getMetricsStorageInstance(connector);
        if (!storage.exists(TABLE.getSchemaTableName())) {
            storage.create(TABLE);
            LOG.info("Created metrics storage for %s", TABLE.getFullTableName());
        }
    }

    private static class MetadataUpdate
    {
        private Mutation mutation;
        private boolean incrementNumRows;
        private boolean flush;

        public MetadataUpdate(Mutation mutation, boolean incrementNumRows, boolean flush)
        {
            this.mutation = mutation;
            this.incrementNumRows = incrementNumRows;
            this.flush = flush;
        }

        public Mutation getMutation()
        {
            return mutation;
        }

        public boolean isIncrementNumRows()
        {
            return incrementNumRows;
        }

        public boolean isFlush()
        {
            return flush;
        }
    }

    private class MutationWriter
            implements Runnable
    {
        private String instance;
        private String zooKeepers;
        private String password;

        public MutationWriter(String instance, String zooKeepers, String password)
        {
            this.instance = instance;
            this.zooKeepers = zooKeepers;
            this.password = password;
        }

        @Override
        public void run()
        {
            try {
                connector = new ZooKeeperInstance(instance, zooKeepers).getConnector(user, new PasswordToken(password));
            }
            catch (AccumuloException | AccumuloSecurityException e) {
                throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Failed to create connector", e);
            }

            //noinspection InfiniteLoopStatement
            while (true) {
                MetadataUpdate update;
                try {
                    update = mutationQueue.take();
                }
                catch (InterruptedException e) {
                    LOG.error("InterruptedException polling for mutation, sleeping for 10s", e);
                    sleep();
                    continue;
                }

                if (writer == null) {
                    createBatchWriter();

                    if (writer == null) {
                        LOG.warn("Writer is still null, skipping this mutation, sleeping for 10s");
                        sleep();
                        continue;
                    }
                }

                boolean written = false;
                do {
                    try {
                        writer.addMutation(update.getMutation(), update.isIncrementNumRows());
                        written = true;
                    }
                    catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
                        LOG.error("Failed to write mutation, sleeping for 10s", e);
                        sleep();
                        createBatchWriter();
                    }
                }
                while (!written);

                while (update.isFlush()) {
                    try {
                        writer.flush();
                        break;
                    }
                    catch (MutationsRejectedException e) {
                        LOG.error("Failed to flush, sleeping for 10s", e);
                        sleep();
                        createBatchWriter();
                    }
                }
            }
        }

        private void sleep()
        {
            try {
                Thread.sleep(10000);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.error("InterruptedException on sleep", e);
            }
        }
    }

    private static class ArrayType
            extends AbstractType
    {
        private final Type elementType;
        public static final String ARRAY_NULL_ELEMENT_MSG = "ARRAY comparison not supported for arrays with null elements";

        public ArrayType(Type elementType)
        {
            super(new TypeSignature(ARRAY, TypeSignatureParameter.of(elementType.getTypeSignature())), Block.class);
            this.elementType = requireNonNull(elementType, "elementType is null");
        }

        public Type getElementType()
        {
            return elementType;
        }

        @Override
        public boolean isComparable()
        {
            return elementType.isComparable();
        }

        @Override
        public boolean isOrderable()
        {
            return elementType.isOrderable();
        }

        @Override
        public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
        {
            Block leftArray = leftBlock.getObject(leftPosition, Block.class);
            Block rightArray = rightBlock.getObject(rightPosition, Block.class);

            if (leftArray.getPositionCount() != rightArray.getPositionCount()) {
                return false;
            }

            for (int i = 0; i < leftArray.getPositionCount(); i++) {
                checkElementNotNull(leftArray.isNull(i), ARRAY_NULL_ELEMENT_MSG);
                checkElementNotNull(rightArray.isNull(i), ARRAY_NULL_ELEMENT_MSG);
                if (!elementType.equalTo(leftArray, i, rightArray, i)) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public long hash(Block block, int position)
        {
            Block array = getObject(block, position);
            long hash = 0;
            for (int i = 0; i < array.getPositionCount(); i++) {
                checkElementNotNull(array.isNull(i), ARRAY_NULL_ELEMENT_MSG);
                hash = getHash(hash, elementType.hash(array, i));
            }
            return hash;
        }

        private static long getHash(long previousHashValue, long value)
        {
            return (31 * previousHashValue + value);
        }

        @Override
        public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
        {
            if (!elementType.isOrderable()) {
                throw new UnsupportedOperationException(getTypeSignature() + " type is not orderable");
            }

            Block leftArray = leftBlock.getObject(leftPosition, Block.class);
            Block rightArray = rightBlock.getObject(rightPosition, Block.class);

            int len = Math.min(leftArray.getPositionCount(), rightArray.getPositionCount());
            int index = 0;
            while (index < len) {
                checkElementNotNull(leftArray.isNull(index), ARRAY_NULL_ELEMENT_MSG);
                checkElementNotNull(rightArray.isNull(index), ARRAY_NULL_ELEMENT_MSG);
                int comparison = elementType.compareTo(leftArray, index, rightArray, index);
                if (comparison != 0) {
                    return comparison;
                }
                index++;
            }

            if (index == len) {
                return leftArray.getPositionCount() - rightArray.getPositionCount();
            }

            return 0;
        }

        @Override
        public Object getObjectValue(ConnectorSession session, Block block, int position)
        {
            if (block.isNull(position)) {
                return null;
            }

            if (block instanceof AbstractArrayBlock) {
                return ((AbstractArrayBlock) block).apply((valuesBlock, start, length) -> arrayBlockToObjectValues(session, valuesBlock, start, length), position);
            }
            else {
                Block arrayBlock = block.getObject(position, Block.class);
                return arrayBlockToObjectValues(session, arrayBlock, 0, arrayBlock.getPositionCount());
            }
        }

        private List<Object> arrayBlockToObjectValues(ConnectorSession session, Block block, int start, int length)
        {
            List<Object> values = new ArrayList<>(length);

            for (int i = 0; i < length; i++) {
                values.add(elementType.getObjectValue(session, block, i + start));
            }

            return Collections.unmodifiableList(values);
        }

        @Override
        public void appendTo(Block block, int position, BlockBuilder blockBuilder)
        {
            if (block.isNull(position)) {
                blockBuilder.appendNull();
            }
            else {
                block.writePositionTo(position, blockBuilder);
                blockBuilder.closeEntry();
            }
        }

        @Override
        public Slice getSlice(Block block, int position)
        {
            return block.getSlice(position, 0, block.getSliceLength(position));
        }

        @Override
        public void writeSlice(BlockBuilder blockBuilder, Slice value)
        {
            writeSlice(blockBuilder, value, 0, value.length());
        }

        @Override
        public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
        {
            blockBuilder.writeBytes(value, offset, length).closeEntry();
        }

        @Override
        public Block getObject(Block block, int position)
        {
            return block.getObject(position, Block.class);
        }

        @Override
        public void writeObject(BlockBuilder blockBuilder, Object value)
        {
            blockBuilder.writeObject(value).closeEntry();
        }

        @Override
        public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
        {
            return new ArrayBlockBuilder(elementType, blockBuilderStatus, expectedEntries, expectedBytesPerEntry);
        }

        @Override
        public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
        {
            return createBlockBuilder(blockBuilderStatus, expectedEntries, 100);
        }

        @Override
        public List<Type> getTypeParameters()
        {
            return ImmutableList.of(getElementType());
        }

        @Override
        public String getDisplayName()
        {
            return ARRAY + "(" + elementType.getDisplayName() + ")";
        }
    }

    private static class MapType
            extends AbstractType
    {
        private final Type keyType;
        private final Type valueType;
        private static final String MAP_NULL_ELEMENT_MSG = "MAP comparison not supported for null value elements";
        private static final int EXPECTED_BYTES_PER_ENTRY = 32;

        public MapType(Type keyType, Type valueType)
        {
            super(new TypeSignature(StandardTypes.MAP,
                            TypeSignatureParameter.of(keyType.getTypeSignature()),
                            TypeSignatureParameter.of(valueType.getTypeSignature())),
                    Block.class);
            checkArgument(keyType.isComparable(), "key type must be comparable");
            this.keyType = keyType;
            this.valueType = valueType;
        }

        @Override
        public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
        {
            return new ArrayBlockBuilder(
                    new InterleavedBlockBuilder(getTypeParameters(), blockBuilderStatus, expectedEntries * 2, expectedBytesPerEntry),
                    blockBuilderStatus,
                    expectedEntries);
        }

        @Override
        public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
        {
            return createBlockBuilder(blockBuilderStatus, expectedEntries, EXPECTED_BYTES_PER_ENTRY);
        }

        public Type getKeyType()
        {
            return keyType;
        }

        public Type getValueType()
        {
            return valueType;
        }

        @Override
        public boolean isComparable()
        {
            return valueType.isComparable();
        }

        @Override
        public long hash(Block block, int position)
        {
            Block mapBlock = getObject(block, position);
            long result = 0;

            for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
                result += hashPosition(keyType, mapBlock, i);
                result += hashPosition(valueType, mapBlock, i + 1);
            }
            return result;
        }

        @Override
        public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
        {
            Block leftMapBlock = leftBlock.getObject(leftPosition, Block.class);
            Block rightMapBlock = rightBlock.getObject(rightPosition, Block.class);

            if (leftMapBlock.getPositionCount() != rightMapBlock.getPositionCount()) {
                return false;
            }

            Map<KeyWrapper, Integer> wrappedLeftMap = new HashMap<>();
            for (int position = 0; position < leftMapBlock.getPositionCount(); position += 2) {
                wrappedLeftMap.put(new KeyWrapper(keyType, leftMapBlock, position), position + 1);
            }

            for (int position = 0; position < rightMapBlock.getPositionCount(); position += 2) {
                KeyWrapper key = new KeyWrapper(keyType, rightMapBlock, position);
                Integer leftValuePosition = wrappedLeftMap.get(key);
                if (leftValuePosition == null) {
                    return false;
                }
                int rightValuePosition = position + 1;
                checkElementNotNull(leftMapBlock.isNull(leftValuePosition), MAP_NULL_ELEMENT_MSG);
                checkElementNotNull(rightMapBlock.isNull(rightValuePosition), MAP_NULL_ELEMENT_MSG);

                if (!valueType.equalTo(leftMapBlock, leftValuePosition, rightMapBlock, rightValuePosition)) {
                    return false;
                }
            }
            return true;
        }

        private static final class KeyWrapper
        {
            private final Type type;
            private final Block block;
            private final int position;

            public KeyWrapper(Type type, Block block, int position)
            {
                this.type = type;
                this.block = block;
                this.position = position;
            }

            public Block getBlock()
            {
                return this.block;
            }

            public int getPosition()
            {
                return this.position;
            }

            @Override
            public int hashCode()
            {
                return Long.hashCode(type.hash(block, position));
            }

            @Override
            public boolean equals(Object obj)
            {
                if (obj == null || !getClass().equals(obj.getClass())) {
                    return false;
                }
                KeyWrapper other = (KeyWrapper) obj;
                return type.equalTo(this.block, this.position, other.getBlock(), other.getPosition());
            }
        }

        @Override
        public Object getObjectValue(ConnectorSession session, Block block, int position)
        {
            if (block.isNull(position)) {
                return null;
            }

            Block mapBlock = block.getObject(position, Block.class);
            Map<Object, Object> map = new HashMap<>();
            for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
                map.put(keyType.getObjectValue(session, mapBlock, i), valueType.getObjectValue(session, mapBlock, i + 1));
            }

            return Collections.unmodifiableMap(map);
        }

        @Override
        public void appendTo(Block block, int position, BlockBuilder blockBuilder)
        {
            if (block.isNull(position)) {
                blockBuilder.appendNull();
            }
            else {
                block.writePositionTo(position, blockBuilder);
                blockBuilder.closeEntry();
            }
        }

        @Override
        public Block getObject(Block block, int position)
        {
            return block.getObject(position, Block.class);
        }

        @Override
        public void writeObject(BlockBuilder blockBuilder, Object value)
        {
            blockBuilder.writeObject(value).closeEntry();
        }

        @Override
        public List<Type> getTypeParameters()
        {
            return ImmutableList.of(getKeyType(), getValueType());
        }

        @Override
        public String getDisplayName()
        {
            return "map(" + keyType.getDisplayName() + ", " + valueType.getDisplayName() + ")";
        }
    }

    private static class RowType
            extends AbstractType
    {
        private final List<RowField> fields;
        private final List<Type> fieldTypes;

        public RowType(List<Type> fieldTypes, Optional<List<String>> fieldNames)
        {
            super(new TypeSignature(
                            ROW,
                            Lists.transform(fieldTypes, Type::getTypeSignature),
                            ImmutableList.copyOf(new ArrayList<>(fieldNames.orElse(ImmutableList.of())))),
                    Block.class);

            ImmutableList.Builder<RowField> builder = ImmutableList.builder();
            for (int i = 0; i < fieldTypes.size(); i++) {
                int index = i;
                builder.add(new RowField(fieldTypes.get(i), fieldNames.map((names) -> names.get(index))));
            }
            fields = builder.build();
            this.fieldTypes = ImmutableList.copyOf(fieldTypes);
        }

        @Override
        public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
        {
            return new ArrayBlockBuilder(
                    new InterleavedBlockBuilder(getTypeParameters(), blockBuilderStatus, expectedEntries * getTypeParameters().size(), expectedBytesPerEntry),
                    blockBuilderStatus,
                    expectedEntries);
        }

        @Override
        public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
        {
            return new ArrayBlockBuilder(
                    new InterleavedBlockBuilder(getTypeParameters(), blockBuilderStatus, expectedEntries * getTypeParameters().size()),
                    blockBuilderStatus,
                    expectedEntries);
        }

        @Override
        public String getDisplayName()
        {
            // Convert to standard sql name
            List<String> fieldDisplayNames = new ArrayList<>();
            for (RowField field : fields) {
                String typeDisplayName = field.getType().getDisplayName();
                if (field.getName().isPresent()) {
                    fieldDisplayNames.add(field.getName().get() + " " + typeDisplayName);
                }
                else {
                    fieldDisplayNames.add(typeDisplayName);
                }
            }
            return ROW + "(" + Joiner.on(", ").join(fieldDisplayNames) + ")";
        }

        @Override
        public Object getObjectValue(ConnectorSession session, Block block, int position)
        {
            if (block.isNull(position)) {
                return null;
            }

            Block arrayBlock = getObject(block, position);
            List<Object> values = new ArrayList<>(arrayBlock.getPositionCount());

            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                values.add(fields.get(i).getType().getObjectValue(session, arrayBlock, i));
            }

            return Collections.unmodifiableList(values);
        }

        @Override
        public void appendTo(Block block, int position, BlockBuilder blockBuilder)
        {
            if (block.isNull(position)) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeObject(block.getObject(position, Block.class));
                blockBuilder.closeEntry();
            }
        }

        @Override
        public Block getObject(Block block, int position)
        {
            return block.getObject(position, Block.class);
        }

        @Override
        public void writeObject(BlockBuilder blockBuilder, Object value)
        {
            blockBuilder.writeObject(value).closeEntry();
        }

        @Override
        public List<Type> getTypeParameters()
        {
            return fieldTypes;
        }

        public static class RowField
        {
            private final Type type;
            private final Optional<String> name;

            public RowField(Type type, Optional<String> name)
            {
                this.type = requireNonNull(type, "type is null");
                this.name = requireNonNull(name, "name is null");
            }

            public Type getType()
            {
                return type;
            }

            public Optional<String> getName()
            {
                return name;
            }
        }

        @Override
        public boolean isComparable()
        {
            return fields.stream().allMatch(field -> field.getType().isComparable());
        }

        @Override
        public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
        {
            Block leftRow = leftBlock.getObject(leftPosition, Block.class);
            Block rightRow = rightBlock.getObject(rightPosition, Block.class);

            for (int i = 0; i < leftRow.getPositionCount(); i++) {
                checkElementNotNull(leftRow.isNull(i));
                checkElementNotNull(rightRow.isNull(i));
                Type fieldType = fields.get(i).getType();
                if (!fieldType.equalTo(leftRow, i, rightRow, i)) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public long hash(Block block, int position)
        {
            Block arrayBlock = block.getObject(position, Block.class);
            long result = 1;
            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                checkElementNotNull(arrayBlock.isNull(i));
                Type elementType = fields.get(i).getType();
                result = 31 * result + elementType.hash(arrayBlock, i);
            }
            return result;
        }

        private static void checkElementNotNull(boolean isNull)
        {
            if (isNull) {
                throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "ROW comparison not supported for fields with null elements");
            }
        }
    }

    private static class DummyTypeManager
            implements TypeManager
    {
        @Override
        public Type getType(TypeSignature signature)
        {
            return null;
        }

        @Override
        public Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters)
        {
            return null;
        }

        @Override
        public List<Type> getTypes()
        {
            return null;
        }

        @Override
        public Collection<ParametricType> getParametricTypes()
        {
            return null;
        }

        @Override
        public Optional<Type> getCommonSuperType(Type firstType, Type secondType)
        {
            return null;
        }

        @Override
        public boolean isTypeOnlyCoercion(Type actualType, Type expectedType)
        {
            return false;
        }

        @Override
        public Optional<Type> coerceTypeBase(Type sourceType, String resultTypeBase)
        {
            return null;
        }

        @Override
        public MethodHandle resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
        {
            return null;
        }
    }

    private static long hashPosition(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return 0;
        }

        return type.hash(block, position);
    }

    private static void checkElementNotNull(boolean isNull, String errorMsg)
    {
        if (isNull) {
            throw new PrestoException(NOT_SUPPORTED, errorMsg);
        }
    }
}
