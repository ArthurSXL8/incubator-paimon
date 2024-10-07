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

package org.apache.paimon.lookup;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TypeUtils;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.paimon.lookup.RocksDBOptions.BLOCK_CACHE_SIZE;

/** The {@link LocalCache} is a read cache for log store, which maintains the fresh value. */
public class LocalCache {
    private FileStoreTable table;
    private String tmpDir;
    private long lruCacheSize;
    private Duration ttl;

    private RocksDBStateFactory stateFactory;
    private RocksDBValueState<InternalRow, InternalRow> tableState;
    protected RowType projectedType;

    public LocalCache(FileStoreTable table, String tmpDir, long lruCacheSize, Duration ttl) {
        this.table = table;
        this.tmpDir = tmpDir;
        this.lruCacheSize = lruCacheSize;
        this.ttl = ttl;
    }

    public void open() throws IOException {
        File path = new File(tmpDir, "rocksdb-localcache-" + UUID.randomUUID());

        CoreOptions coreOptions = table.coreOptions();
        Options options = coreOptions.toConfiguration();

        Options rocksdbOptions = Options.fromMap(new HashMap<>(options.toMap()));
        // we should avoid too small memory
        long blockCache = rocksdbOptions.get(BLOCK_CACHE_SIZE).getBytes();
        rocksdbOptions.set(BLOCK_CACHE_SIZE, new MemorySize(blockCache));

        this.stateFactory = new RocksDBStateFactory(path.toString(), rocksdbOptions, ttl);

        List<String> fieldNames = table.rowType().getFieldNames();

        int[] projection = IntStream.range(0, table.rowType().getFieldCount()).toArray();
        List<String> projectFields =
                Arrays.stream(projection)
                        .mapToObj(i -> table.rowType().getFieldNames().get(i))
                        .collect(Collectors.toList());

        // add primary keys
        for (String field : table.primaryKeys()) {
            if (!projectFields.contains(field)) {
                projectFields.add(field);
            }
        }

        projectedType = TypeUtils.project(table.rowType(), projection);

        int[] primaryKeyMapping =
                table.primaryKeys().stream().mapToInt(fieldNames::indexOf).toArray();

        this.tableState =
                stateFactory.valueState(
                        "localCache",
                        InternalSerializers.create(
                                TypeUtils.project(projectedType, primaryKeyMapping)),
                        InternalSerializers.create(projectedType),
                        lruCacheSize);
    }

    public InternalRow get(InternalRow key) throws IOException {
        return tableState.get(key);
    }

    public void put(InternalRow key, InternalRow value) throws IOException {
        tableState.put(key, value);
    }

    public void close() throws IOException {
        if (tableState != null) {
            tableState.db.close();
        }
        if (stateFactory != null) {
            stateFactory.close();
        }
    }
}
