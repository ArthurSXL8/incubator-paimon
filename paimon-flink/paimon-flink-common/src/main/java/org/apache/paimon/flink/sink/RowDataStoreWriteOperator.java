/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.sink;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.log.LogWriteCallback;
import org.apache.paimon.flink.lookup.FixedBucketFromPkExtractor;
import org.apache.paimon.flink.lookup.FullCacheLookupTable;
import org.apache.paimon.lookup.LocalCache;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyTableUtils;
import org.apache.paimon.table.sink.FixedBucketRowKeyExtractor;
import org.apache.paimon.table.sink.SinkRecord;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.functions.StreamingFunctionUtils;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.apache.paimon.lookup.RocksDBOptions.LOOKUP_CACHE_ROWS;

/** A {@link PrepareCommitOperator} to write {@link InternalRow}. Record schema is fixed. */
public class RowDataStoreWriteOperator extends TableWriteOperator<InternalRow> {

    private static final long serialVersionUID = 3L;

    @Nullable private final LogSinkFunction logSinkFunction;
    private transient SimpleContext sinkContext;
    @Nullable private transient LogWriteCallback logCallback;
    private transient LocalCache localCache;
    // private transient
    private transient FullCacheLookupTable lookupTable;
    private transient FixedBucketFromPkExtractor fixedBucketExtractor;
    private MergeFunction<KeyValue> mf;

    private transient long lastRefreshMillis;
    /** We listen to this ourselves because we don't have an {@link InternalTimerService}. */
    private long currentWatermark = Long.MIN_VALUE;

    public RowDataStoreWriteOperator(
            FileStoreTable table,
            @Nullable LogSinkFunction logSinkFunction,
            StoreSinkWrite.Provider storeSinkWriteProvider,
            String initialCommitUser) {
        super(table, storeSinkWriteProvider, initialCommitUser);
        this.logSinkFunction = logSinkFunction;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<Committable>> output) {
        super.setup(containingTask, config, output);
        if (logSinkFunction != null) {
            FunctionUtils.setFunctionRuntimeContext(logSinkFunction, getRuntimeContext());
        }
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        if (logSinkFunction != null) {
            StreamingFunctionUtils.restoreFunctionState(context, logSinkFunction);
        }
    }

    @Override
    protected boolean containLogSystem() {
        return logSinkFunction != null;
    }

    @Override
    public void open() throws Exception {
        super.open();

        this.sinkContext = new SimpleContext(getProcessingTimeService());
        if (logSinkFunction != null) {
            // to stay compatible with Flink 1.18-
            if (logSinkFunction instanceof RichFunction) {
                RichFunction richFunction = (RichFunction) logSinkFunction;
                richFunction.open(new Configuration());
            }

            logCallback = new LogWriteCallback();
            logSinkFunction.setWriteCallback(logCallback);

            this.lookupTable = newLookupTable();
            lookupTable.open();

            this.fixedBucketExtractor = new FixedBucketFromPkExtractor(table.schema());

            this.localCache = newLocalCache();
            localCache.open();
            lastRefreshMillis = System.currentTimeMillis();

            KeyValueFieldsExtractor extractor =
                    PrimaryKeyTableUtils.PrimaryKeyFieldsExtractor.EXTRACTOR;
            MergeFunctionFactory<KeyValue> mfFactory =
                    PrimaryKeyTableUtils.createMergeFunctionFactory(table.schema(), extractor);
            mf = mfFactory.create(null);
        }
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);

        this.currentWatermark = mark.getTimestamp();
        if (logSinkFunction != null) {
            logSinkFunction.writeWatermark(
                    new org.apache.flink.api.common.eventtime.Watermark(mark.getTimestamp()));
        }
    }

    @Override
    public void processElement(StreamRecord<InternalRow> element) throws Exception {
        sinkContext.timestamp = element.hasTimestamp() ? element.getTimestamp() : null;

        InternalRow row = element.getValue();
        SinkRecord record;
        try {
            record = write.write(element.getValue());
        } catch (Exception e) {
            throw new IOException(e);
        }

        if (record != null && logSinkFunction != null) {
            refreshLookupTableIfNeeded();
            KeyValue mergedKeyValue = mergeKeyValue(row);
            InternalRow newValue = mergedKeyValue.value();
            localCache.put(row, newValue);

            record = toSinkRecord(newValue);

            // write to log store, need to preserve original pk (which includes partition fields)
            SinkRecord logRecord = write.toLogRecord(record);
            LOG.info("logRecord {}", logRecord);
            logSinkFunction.invoke(logRecord, sinkContext);
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        if (logSinkFunction != null) {
            StreamingFunctionUtils.snapshotFunctionState(
                    context, getOperatorStateBackend(), logSinkFunction);
        }
    }

    @Override
    public void finish() throws Exception {
        super.finish();

        if (logSinkFunction != null) {
            logSinkFunction.finish();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();

        if (logSinkFunction != null) {
            FunctionUtils.closeFunction(logSinkFunction);
        }

        if (lookupTable != null) {
            lookupTable.close();
        }

        if (localCache != null) {
            localCache.close();
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);

        if (logSinkFunction instanceof CheckpointListener) {
            ((CheckpointListener) logSinkFunction).notifyCheckpointComplete(checkpointId);
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        super.notifyCheckpointAborted(checkpointId);

        if (logSinkFunction instanceof CheckpointListener) {
            ((CheckpointListener) logSinkFunction).notifyCheckpointAborted(checkpointId);
        }
    }

    @Override
    protected List<Committable> prepareCommit(boolean waitCompaction, long checkpointId)
            throws IOException {
        List<Committable> committables = super.prepareCommit(waitCompaction, checkpointId);

        if (logCallback != null) {
            try {
                Objects.requireNonNull(logSinkFunction).flush();
            } catch (Exception e) {
                throw new IOException(e);
            }
            logCallback
                    .offsets()
                    .forEach(
                            (k, v) ->
                                    committables.add(
                                            new Committable(
                                                    checkpointId,
                                                    Committable.Kind.LOG_OFFSET,
                                                    new LogOffsetCommittable(k, v))));
        }

        return committables;
    }

    private class SimpleContext implements SinkFunction.Context {

        @Nullable private Long timestamp;

        private final ProcessingTimeService processingTimeService;

        public SimpleContext(ProcessingTimeService processingTimeService) {
            this.processingTimeService = processingTimeService;
        }

        @Override
        public long currentProcessingTime() {
            return processingTimeService.getCurrentProcessingTime();
        }

        @Override
        public long currentWatermark() {
            return currentWatermark;
        }

        @Override
        public Long timestamp() {
            return timestamp;
        }
    }

    private LocalCache newLocalCache() throws IOException {
        String[] tmpDirs = getRuntimeContext().getTaskManagerRuntimeInfo().getTmpDirectories();
        String rocksDBDir = tmpDirs[ThreadLocalRandom.current().nextInt(tmpDirs.length)];
        // The TTL
        return new LocalCache(table, rocksDBDir, 1024, Duration.ofMinutes(10));
    }

    private KeyValue newKeyValue(InternalRow row) {
        return new KeyValue().replace(row, KeyValue.UNKNOWN_SEQUENCE, row.getRowKind(), row);
    }

    private SinkRecord toSinkRecord(InternalRow row) {
        FixedBucketRowKeyExtractor extractor = new FixedBucketRowKeyExtractor(table.schema());
        extractor.setRecord(row);
        return new SinkRecord(
                extractor.partition(), extractor.bucket(), extractor.trimmedPrimaryKey(), row);
    }

    private FullCacheLookupTable newLookupTable() {
        String[] tmpDirs = getRuntimeContext().getTaskManagerRuntimeInfo().getTmpDirectories();
        String rocksDBDir = tmpDirs[ThreadLocalRandom.current().nextInt(tmpDirs.length)];
        File path = new File(rocksDBDir, "rocksdb-lookup-" + UUID.randomUUID());

        int[] projection = IntStream.range(0, table.rowType().getFieldCount()).toArray();
        FullCacheLookupTable.Context context =
                new FullCacheLookupTable.Context(
                        table, projection, null, null, path, table.primaryKeys(), null);
        return FullCacheLookupTable.create(
                context, Long.parseLong(table.options().get(LOOKUP_CACHE_ROWS)));
    }

    private void refreshLookupTableIfNeeded() throws Exception {
        long current = System.currentTimeMillis();
        if (current - lastRefreshMillis >= TimeUnit.MINUTES.toMillis(5)) {
            lookupTable.refresh();
            lastRefreshMillis = current;
        }
    }

    private KeyValue mergeKeyValue(InternalRow row) throws IOException {
        List<InternalRow> rows = new ArrayList<>();
        InternalRow oldRowValue = localCache.get(row);
        if (oldRowValue == null) {
            rows = lookupTable.get(row);
        } else {
            rows.add(oldRowValue);
        }

        rows.add(row);

        for (InternalRow r : rows) {
            mf.add(newKeyValue(r));
        }
        return mf.getResult();
    }
}
