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

package org.apache.paimon.append;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.append.AppendDeletionFileMaintainer;
import org.apache.paimon.deletionvectors.append.UnawareAppendDeletionFileMaintainer;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.IndexIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.operation.AppendOnlyFileStoreWrite;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.paimon.table.BucketMode.UNAWARE_BUCKET;

/** Compaction task generated by {@link UnawareAppendTableCompactionCoordinator}. */
public class UnawareAppendCompactionTask {

    private final BinaryRow partition;
    private final List<DataFileMeta> compactBefore;
    private final List<DataFileMeta> compactAfter;

    public UnawareAppendCompactionTask(BinaryRow partition, List<DataFileMeta> files) {
        Preconditions.checkArgument(files != null);
        this.partition = partition;
        compactBefore = new ArrayList<>(files);
        compactAfter = new ArrayList<>();
    }

    public BinaryRow partition() {
        return partition;
    }

    public List<DataFileMeta> compactBefore() {
        return compactBefore;
    }

    public List<DataFileMeta> compactAfter() {
        return compactAfter;
    }

    public CommitMessage doCompact(FileStoreTable table, AppendOnlyFileStoreWrite write)
            throws Exception {
        boolean dvEnabled = table.coreOptions().deletionVectorsEnabled();
        Preconditions.checkArgument(
                dvEnabled || compactBefore.size() > 1,
                "AppendOnlyCompactionTask need more than one file input.");
        IndexIncrement indexIncrement;
        if (dvEnabled) {
            UnawareAppendDeletionFileMaintainer dvIndexFileMaintainer =
                    AppendDeletionFileMaintainer.forUnawareAppend(
                            table.store().newIndexFileHandler(),
                            table.snapshotManager().latestSnapshotId(),
                            partition);
            compactAfter.addAll(
                    write.compactRewrite(
                            partition,
                            UNAWARE_BUCKET,
                            dvIndexFileMaintainer::getDeletionVector,
                            compactBefore));

            compactBefore.forEach(
                    f -> dvIndexFileMaintainer.notifyRemovedDeletionVector(f.fileName()));
            List<IndexManifestEntry> indexEntries = dvIndexFileMaintainer.persist();
            Preconditions.checkArgument(
                    indexEntries.stream().noneMatch(i -> i.kind() == FileKind.ADD));
            List<IndexFileMeta> removed =
                    indexEntries.stream()
                            .map(IndexManifestEntry::indexFile)
                            .collect(Collectors.toList());
            indexIncrement = new IndexIncrement(Collections.emptyList(), removed);
        } else {
            compactAfter.addAll(
                    write.compactRewrite(partition, UNAWARE_BUCKET, null, compactBefore));
            indexIncrement = new IndexIncrement(Collections.emptyList());
        }

        CompactIncrement compactIncrement =
                new CompactIncrement(compactBefore, compactAfter, Collections.emptyList());
        return new CommitMessageImpl(
                partition,
                0, // bucket 0 is bucket for unaware-bucket table for compatibility with the old
                // design
                DataIncrement.emptyIncrement(),
                compactIncrement,
                indexIncrement);
    }

    public int hashCode() {
        return Objects.hash(partition, compactBefore, compactAfter);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UnawareAppendCompactionTask that = (UnawareAppendCompactionTask) o;
        return Objects.equals(partition, that.partition)
                && Objects.equals(compactBefore, that.compactBefore)
                && Objects.equals(compactAfter, that.compactAfter);
    }

    @Override
    public String toString() {
        return String.format(
                "CompactionTask {"
                        + "partition = %s, "
                        + "compactBefore = %s, "
                        + "compactAfter = %s}",
                partition, compactBefore, compactAfter);
    }
}