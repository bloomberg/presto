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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.MethodHandleUtil;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.QueryInputMetadata;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.ParametricType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;
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
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.facebook.presto.accumulo.AccumuloErrorCode.UNEXPECTED_ACCUMULO_ERROR;
import static com.facebook.presto.accumulo.Types.getElementType;
import static com.facebook.presto.accumulo.serializers.AccumuloRowSerializer.getBlockFromArray;
import static com.facebook.presto.accumulo.serializers.AccumuloRowSerializer.getBlockFromMap;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class AccumuloEventListener
        implements EventListener
{
    public static final String SPLIT = "split";
    public static final Type SPLIT_TYPE;

    private static final Logger LOG = Logger.get(AccumuloEventListener.class);
    private static final String FAILURE = "failure";
    private static final String INDEX_COLUMNS = "create_time,start_time,end_time";
    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String ROW_ID = "id";
    private static final String SCHEMA = "presto";
    private static final String QUERY = "query";
    private static final Type FAILURE_TYPE;
    private static final Type INPUT_TYPE;
    private static final Type SESSION_PROPERTIES_TYPE = new MapType(
            VARCHAR,
            VARCHAR,
            MethodHandleUtil.methodHandle(Slice.class, "equals", Object.class).asType(MethodType.methodType(boolean.class, Slice.class, Slice.class)),
            MethodHandleUtil.methodHandle(Slice.class, "hashCode").asType(MethodType.methodType(long.class, Slice.class)),
            MethodHandleUtil.methodHandle(AccumuloEventListener.class, "blockVarcharHashCode", Block.class, int.class));
    private static final RowSchema ROW_SCHEMA = new RowSchema();

    private final AccumuloRowSerializer serializer = new LexicoderRowSerializer();

    static {
        ROW_SCHEMA.addRowId("id", VARCHAR);
        ROW_SCHEMA.addColumn("source", Optional.of(QUERY), Optional.of("source"), VARCHAR);
        ROW_SCHEMA.addColumn("user", Optional.of(QUERY), Optional.of("user"), VARCHAR);
        ROW_SCHEMA.addColumn("user_agent", Optional.of(QUERY), Optional.of("user_agent"), VARCHAR);

        ROW_SCHEMA.addColumn("sql", Optional.of(QUERY), Optional.of("sql"), VARCHAR);
        ROW_SCHEMA.addColumn("session_properties", Optional.of(QUERY), Optional.of("session_properties"), SESSION_PROPERTIES_TYPE);
        ROW_SCHEMA.addColumn("schema", Optional.of(QUERY), Optional.of("schema"), VARCHAR);
        ROW_SCHEMA.addColumn("transaction_id", Optional.of(QUERY), Optional.of("transaction_id"), VARCHAR);
        ROW_SCHEMA.addColumn("state", Optional.of(QUERY), Optional.of("state"), VARCHAR);
        ROW_SCHEMA.addColumn("is_complete", Optional.of(QUERY), Optional.of("is_complete"), BOOLEAN);
        ROW_SCHEMA.addColumn("uri", Optional.of(QUERY), Optional.of("uri"), VARCHAR);

        ROW_SCHEMA.addColumn("create_time", Optional.of(QUERY), Optional.of("create_time"), TIMESTAMP);
        ROW_SCHEMA.addColumn("start_time", Optional.of(QUERY), Optional.of("start_time"), TIMESTAMP);
        ROW_SCHEMA.addColumn("end_time", Optional.of(QUERY), Optional.of("end_time"), TIMESTAMP);
        ROW_SCHEMA.addColumn("cpu_time", Optional.of(QUERY), Optional.of("cpu_time"), BIGINT);
        ROW_SCHEMA.addColumn("analysis_time", Optional.of(QUERY), Optional.of("analysis_time"), BIGINT);
        ROW_SCHEMA.addColumn("planning_time", Optional.of(QUERY), Optional.of("planning_time"), BIGINT);
        ROW_SCHEMA.addColumn("queued_time", Optional.of(QUERY), Optional.of("queued_time"), BIGINT);
        ROW_SCHEMA.addColumn("wall_time", Optional.of(QUERY), Optional.of("wall_time"), BIGINT);

        ROW_SCHEMA.addColumn("completed_splits", Optional.of(QUERY), Optional.of("completed_splits"), INTEGER);
        ROW_SCHEMA.addColumn("total_bytes", Optional.of(QUERY), Optional.of("total_bytes"), BIGINT);
        ROW_SCHEMA.addColumn("total_rows", Optional.of(QUERY), Optional.of("total_rows"), BIGINT);
        ROW_SCHEMA.addColumn("peak_mem_bytes", Optional.of(QUERY), Optional.of("peak_mem_bytes"), BIGINT);

        ROW_SCHEMA.addColumn("environment", Optional.of(QUERY), Optional.of("environment"), VARCHAR);
        ROW_SCHEMA.addColumn("remote_client_address", Optional.of(QUERY), Optional.of("remote_client_address"), VARCHAR);
        ROW_SCHEMA.addColumn("server_address", Optional.of(QUERY), Optional.of("server_address"), VARCHAR);
        ROW_SCHEMA.addColumn("server_version", Optional.of(QUERY), Optional.of("server_version"), VARCHAR);

        INPUT_TYPE = new ArrayType(new RowType(ImmutableList.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR), Optional.of(ImmutableList.of("connector_id", "table", "schema", "columns", "connector_info"))));
        ROW_SCHEMA.addColumn("inputs", Optional.of(INPUT), Optional.of(INPUT), INPUT_TYPE);

        ROW_SCHEMA.addColumn("output_connector_id", Optional.of(OUTPUT), Optional.of("connector_id"), VARCHAR);
        ROW_SCHEMA.addColumn("output_schema", Optional.of(OUTPUT), Optional.of("schema"), VARCHAR);
        ROW_SCHEMA.addColumn("output_table", Optional.of(OUTPUT), Optional.of("table"), VARCHAR);

        FAILURE_TYPE = new RowType(ImmutableList.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR), Optional.of(ImmutableList.of("code", "type", "msg", "task", "host", "json")));
        ROW_SCHEMA.addColumn("failure", Optional.of(FAILURE), Optional.of("failure"), FAILURE_TYPE);

        SPLIT_TYPE = new ArrayType(new RowType(
                ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT, TIMESTAMP, TIMESTAMP, VARCHAR, VARCHAR, BIGINT, VARCHAR, TIMESTAMP, VARCHAR, BIGINT, BIGINT, BIGINT, BIGINT),
                Optional.of(ImmutableList.of("completed_data_size_bytes", "completed_positions", "completed_read_time", "cpu_time", "create_time", "end_time", "failure_msg", "failure_type", "queued_time", "stage_id", "start_time", "task_id", "time_to_first_byte", "time_to_last_byte", "user_time", "wall_time"))));

        ROW_SCHEMA.addColumn("splits", Optional.of(SPLIT), Optional.of(SPLIT), SPLIT_TYPE);
    }

    @SuppressWarnings("unused")
    public static long blockVarcharHashCode(Block block, int position)
    {
        return block.hash(position, 0, block.getSliceLength(position));
    }

    private final AccumuloConfig config;
    private final AccumuloTable table;
    private final BatchWriterConfig batchWriterConfig;
    private final Duration timeout;
    private final Duration latency;
    private final String user;
    private final BlockingQueue<MetadataUpdate> mutationQueue = new LinkedBlockingQueue<>();

    private Connector connector;
    private PrestoBatchWriter writer;

    public AccumuloEventListener(
            String instance,
            String zooKeepers,
            String user,
            String password,
            Duration timeout,
            Duration latency,
            String tableName)
    {
        requireNonNull(instance, "instance is null");
        requireNonNull(zooKeepers, "zookeepers is null");
        this.user = requireNonNull(user, "user is null");
        requireNonNull(password, "password is null");
        this.timeout = requireNonNull(timeout, "timeout is null");
        this.latency = requireNonNull(latency, "latency is null");
        requireNonNull(tableName, "tableName is null");

        this.config = new AccumuloConfig().setInstance(instance).setZooKeepers(zooKeepers).setUsername(user).setPassword(password);
        this.batchWriterConfig = new BatchWriterConfig().setTimeout(timeout.toMillis(), MILLISECONDS).setMaxLatency(latency.toMillis(), MILLISECONDS);

        table = new AccumuloTable(
                SCHEMA,
                tableName,
                ROW_SCHEMA.getColumns(),
                ROW_ID,
                true,
                LexicoderRowSerializer.class.getCanonicalName(),
                Optional.empty(),
                Optional.of(AccumuloMetricsStorage.class.getCanonicalName()),
                true,
                Optional.of(INDEX_COLUMNS));

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
                writer = new PrestoBatchWriter(connector, connector.securityOperations().getUserAuthorizations(user), table, batchWriterConfig);
                LOG.info("Created BatchWriter against table %s with timeout %s and latency %s", table.getFullTableName(), timeout, latency);
            }
            catch (TableNotFoundException e) {
                LOG.warn("Table %s not found. Recreating...", table.getFullTableName());
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
        ZooKeeperMetadataManager metadataManager = new ZooKeeperMetadataManager(config, new DummyTypeRegistry() {});
        if (!metadataManager.exists(table.getSchemaTableName())) {
            metadataManager.createTableMetadata(table);
            LOG.info("Created metadata for table " + table.getSchemaTableName());
        }

        AccumuloTableManager tableManager = new AccumuloTableManager(connector);
        tableManager.ensureNamespace(table.getSchema());
        if (!tableManager.exists(table.getFullTableName())) {
            tableManager.createAccumuloTable(table.getFullTableName());

            try {
                IteratorSetting setting = new IteratorSetting(19, ListCombiner.class);
                ListCombiner.setCombineAllColumns(setting, false);
                ListCombiner.setColumns(setting, ImmutableList.of(new Column(SPLIT, SPLIT)));
                connector.tableOperations().attachIterator(table.getFullTableName(), setting);
                LOG.info("Created Accumulo table %s", table.getFullTableName());
            }
            catch (AccumuloSecurityException | AccumuloException e) {
                throw new PrestoException(AccumuloErrorCode.UNEXPECTED_ACCUMULO_ERROR, "Failed to set iterator", e);
            }
            catch (TableNotFoundException e) {
                LOG.warn("Failed to set iterator, table %s does not exist", table.getFullTableName());
            }
        }

        for (IndexColumn indexColumn : table.getParsedIndexColumns()) {
            if (!tableManager.exists(indexColumn.getIndexTable())) {
                tableManager.createAccumuloTable(indexColumn.getIndexTable());
                LOG.info("Created Accumulo table %s", indexColumn.getIndexTable());
            }
        }

        MetricsStorage storage = table.getMetricsStorageInstance(connector);
        if (!storage.exists(table.getSchemaTableName())) {
            storage.create(table);
            LOG.info("Created metrics storage for %s", table.getFullTableName());
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

    private class DummyTypeRegistry
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
}
