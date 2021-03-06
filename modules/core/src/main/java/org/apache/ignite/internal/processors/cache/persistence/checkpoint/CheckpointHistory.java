/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.CacheState;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.Checkpoint;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE;

/**
 * Checkpoint history. Holds chronological ordered map with {@link CheckpointEntry CheckpointEntries}.
 * Data is loaded from corresponding checkpoint directory.
 * This directory holds files for checkpoint start and end.
 */
public class CheckpointHistory {
    /** Logger. */
    private final IgniteLogger log;

    /** Cache shared context. */
    private final GridCacheSharedContext<?, ?> cctx;

    /**
     * Maps checkpoint's timestamp (from CP file name) to CP entry.
     * Using TS provides historical order of CP entries in map ( first is oldest )
     */
    private final NavigableMap<Long, CheckpointEntry> histMap = new ConcurrentSkipListMap<>();

    /** The maximal number of checkpoints hold in memory. */
    private final int maxCpHistMemSize;

    /** Should WAL be truncated */
    private final boolean isWalTruncationEnabled;

    /** Maximum size of WAL archive (in bytes) */
    private final long maxWalArchiveSize;

    /** Map stores the earliest checkpoint for each partition from particular group. */
    private final Map<GroupPartitionId, CheckpointEntry> earliestCp = new HashMap<>();

    /**
     * Constructor.
     *
     * @param ctx Context.
     */
    public CheckpointHistory(GridKernalContext ctx) {
        cctx = ctx.cache().context();
        log = ctx.log(getClass());

        DataStorageConfiguration dsCfg = ctx.config().getDataStorageConfiguration();

        maxWalArchiveSize = dsCfg.getMaxWalArchiveSize();

        isWalTruncationEnabled = maxWalArchiveSize != DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE;

        maxCpHistMemSize = IgniteSystemProperties.getInteger(IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE, 100);
    }

    /**
     * @param checkpoints Checkpoints.
     */
    public void initialize(List<CheckpointEntry> checkpoints) {
        for (CheckpointEntry e : checkpoints)
            histMap.put(e.timestamp(), e);

        for (Long timestamp : checkpoints(false)) {
            try {
                updateEarliestCpMap(entry(timestamp));
            }
            catch (IgniteCheckedException e) {
                U.warn(log, "Failed to process checkpoint, happened at " + U.format(timestamp) + '.', e);
            }
        }
    }

    /**
     * @param cpTs Checkpoint timestamp.
     * @return Initialized entry.
     * @throws IgniteCheckedException If failed to initialize entry.
     */
    private CheckpointEntry entry(Long cpTs) throws IgniteCheckedException {
        CheckpointEntry entry = histMap.get(cpTs);

        if (entry == null)
            throw new IgniteCheckedException("Checkpoint entry was removed: " + cpTs);

        return entry;
    }

    /**
     * @return First checkpoint entry if exists. Otherwise {@code null}.
     */
    public CheckpointEntry firstCheckpoint() {
        Map.Entry<Long,CheckpointEntry> entry = histMap.firstEntry();

        return entry != null ? entry.getValue() : null;
    }

    /**
     * @return Last checkpoint entry if exists. Otherwise {@code null}.
     */
    public CheckpointEntry lastCheckpoint() {
        Map.Entry<Long,CheckpointEntry> entry = histMap.lastEntry();

        return entry != null ? entry.getValue() : null;
    }

    /**
     * @return First checkpoint WAL pointer if exists. Otherwise {@code null}.
     */
    public WALPointer firstCheckpointPointer() {
        CheckpointEntry entry = firstCheckpoint();

        return entry != null ? entry.checkpointMark() : null;
    }

    /**
     * @return Collection of checkpoint timestamps.
     */
    public Collection<Long> checkpoints(boolean descending) {
        if (descending)
            return histMap.descendingKeySet();

        return histMap.keySet();
    }

    /**
     *
     */
    public Collection<Long> checkpoints() {
        return checkpoints(false);
    }

    /**
     * Adds checkpoint entry after the corresponding WAL record has been written to WAL. The checkpoint itself
     * is not finished yet.
     *
     * @param entry Entry to add.
     * @param cacheStates Cache states map.
     */
    public void addCheckpoint(CheckpointEntry entry, Map<Integer, CacheState> cacheStates) {
        addCpCacheStatesToEarliestCpMap(entry, cacheStates);

        histMap.put(entry.timestamp(), entry);
    }

    /**
     * Update map which stored the earliest checkpoint each partitions for groups.
     *
     * @param entry Checkpoint entry.
     */
    private void updateEarliestCpMap(CheckpointEntry entry) {
        try {
            Map<Integer, CheckpointEntry.GroupState> states = entry.groupState(cctx);

            Iterator<Map.Entry<GroupPartitionId, CheckpointEntry>> iter = earliestCp.entrySet().iterator();

            while (iter.hasNext()) {
                Map.Entry<GroupPartitionId, CheckpointEntry> grpPartCp = iter.next();

                int grpId = grpPartCp.getKey().getGroupId();

                if (!isCheckpointApplicableForGroup(grpId, entry)) {
                    iter.remove();

                    continue;
                }

                int part = grpPartCp.getKey().getPartitionId();

                int pIdx = states.get(grpId).indexByPartition(part);

                if (pIdx < 0)
                    iter.remove();
            }

            addCpGroupStatesToEarliestCpMap(entry, states);
        }
        catch (IgniteCheckedException ex) {
            U.warn(log, "Failed to process checkpoint: " + (entry != null ? entry : "none"), ex);

            earliestCp.clear();
        }
    }

    /**
     * Prepare last checkpoint in history that will marked as inapplicable.
     *
     * @param grpId Group id.
     * @return Checkpoint witch it'd be marked as inapplicable.
     */
    public CheckpointEntry lastCheckpointMarkingAsInapplicable(Integer grpId) {
        synchronized (earliestCp) {
            CheckpointEntry lastCp = lastCheckpoint();

            earliestCp.keySet().removeIf(grpPart -> grpId.equals(grpPart.getGroupId()));

            return lastCp;
        }
    }

    /**
     * Add last checkpoint to map of the earliest checkpoints.
     *
     * @param entry Checkpoint entry.
     * @param cacheStates Cache states map.
     */
    private void addCpCacheStatesToEarliestCpMap(CheckpointEntry entry, Map<Integer, CacheState> cacheStates) {
        for (Integer grpId : cacheStates.keySet()) {
            CacheState cacheState = cacheStates.get(grpId);

            for (int pIdx = 0; pIdx < cacheState.size(); pIdx++) {
                int part = cacheState.partitionByIndex(pIdx);

                GroupPartitionId grpPartKey = new GroupPartitionId(grpId, part);

                addPartitionToEarliestCheckpoints(grpPartKey, entry);
            }
        }
    }

    /**
     * Add last checkpoint to map of the earliest checkpoints.
     *
     * @param entry Checkpoint entry.
     * @param cacheGrpStates Group states map.
     */
    private void addCpGroupStatesToEarliestCpMap(
        CheckpointEntry entry,
        Map<Integer, CheckpointEntry.GroupState> cacheGrpStates
    ) {
        for (Integer grpId : cacheGrpStates.keySet()) {
            CheckpointEntry.GroupState grpState = cacheGrpStates.get(grpId);

            for (int pIdx = 0; pIdx < grpState.size(); pIdx++) {
                int part = grpState.getPartitionByIndex(pIdx);

                GroupPartitionId grpPartKey = new GroupPartitionId(grpId, part);

                addPartitionToEarliestCheckpoints(grpPartKey, entry);
            }
        }
    }

    /**
     * Add entry to earliest checkpoint map. Ignore is such key is already present.
     *
     * @param grpPartKey Key that consists of cache group id and partition index.
     * @param entry Checkpoint entry.
     */
    private void addPartitionToEarliestCheckpoints(GroupPartitionId grpPartKey, CheckpointEntry entry) {
        if (!earliestCp.containsKey(grpPartKey))
            earliestCp.put(grpPartKey, entry);
    }

    /**
     * Clears checkpoint history after WAL truncation.
     *
     * @return List of checkpoint entries removed from history.
     */
    public List<CheckpointEntry> onWalTruncated(WALPointer ptr) {
        List<CheckpointEntry> removed = new ArrayList<>();

        FileWALPointer highBound = (FileWALPointer)ptr;

        for (CheckpointEntry cpEntry : histMap.values()) {
            FileWALPointer cpPnt = (FileWALPointer)cpEntry.checkpointMark();

            if (highBound.compareTo(cpPnt) <= 0)
                break;

            if (!removeCheckpoint(cpEntry))
                break;

            removed.add(cpEntry);
        }

        return removed;
    }

    /**
     * Removes checkpoints from history.
     *
     * @return List of checkpoint entries removed from history.
     */
    public List<CheckpointEntry> removeCheckpoints(int countToRemove) {
        if (countToRemove == 0)
            return Collections.emptyList();

        List<CheckpointEntry> removed = new ArrayList<>();

        for (Iterator<Map.Entry<Long, CheckpointEntry>> iterator = histMap.entrySet().iterator();
            iterator.hasNext() && removed.size() < countToRemove; ) {
            Map.Entry<Long, CheckpointEntry> entry = iterator.next();

            CheckpointEntry checkpoint = entry.getValue();

            if (!removeCheckpoint(checkpoint))
                break;

            removed.add(checkpoint);
        }

        return removed;
    }

    /**
     * Remove checkpoint from history
     * @param checkpoint Checkpoint to be removed
     * @return Whether checkpoint was removed from history
     */
    private boolean removeCheckpoint(CheckpointEntry checkpoint) {
        if (cctx.wal().reserved(checkpoint.checkpointMark())) {
            U.warn(log, "Could not clear historyMap due to WAL reservation on cp: " + checkpoint +
                ", history map size is " + histMap.size());

            return false;
        }

        synchronized (earliestCp) {
            CheckpointEntry deletedCpEntry = histMap.remove(checkpoint.timestamp());

            CheckpointEntry oldestCpInHistory = firstCheckpoint();

            Iterator<Map.Entry<GroupPartitionId, CheckpointEntry>> iter = earliestCp.entrySet().iterator();

            while (iter.hasNext()) {
                Map.Entry<GroupPartitionId, CheckpointEntry> grpPartPerCp = iter.next();

                if (grpPartPerCp.getValue() == deletedCpEntry)
                    grpPartPerCp.setValue(oldestCpInHistory);
            }
        }

        return true;
    }

    /**
     * Logs and clears checkpoint history after checkpoint finish.
     *
     * @return List of checkpoints removed from history.
     */
    public List<CheckpointEntry> onCheckpointFinished(Checkpoint chp) {
        chp.walSegsCoveredRange(calculateWalSegmentsCovered());

        int removeCount = isWalTruncationEnabled
            ? checkpointCountUntilDeleteByArchiveSize()
            : (histMap.size() - maxCpHistMemSize);

        if (removeCount <= 0)
            return Collections.emptyList();

        List<CheckpointEntry> deletedCheckpoints = removeCheckpoints(removeCount);

        if (isWalTruncationEnabled) {
            int deleted = cctx.wal().truncate(null, firstCheckpointPointer());

            chp.walFilesDeleted(deleted);
        }

        return deletedCheckpoints;
    }

    /**
     * Calculate count of checkpoints to delete by maximum allowed archive size.
     *
     * @return Checkpoint count to be deleted.
     */
    private int checkpointCountUntilDeleteByArchiveSize() {
        long absFileIdxToDel = cctx.wal().maxArchivedSegmentToDelete();

        if (absFileIdxToDel < 0)
            return 0;

        long fileUntilDel = absFileIdxToDel + 1;

        long checkpointFileIdx = absFileIdx(lastCheckpoint());

        int countToRemove = 0;

        for (CheckpointEntry cpEntry : histMap.values()) {
            long currFileIdx = absFileIdx(cpEntry);

            if (checkpointFileIdx <= currFileIdx || fileUntilDel <= currFileIdx)
                return countToRemove;

            countToRemove++;
        }

        return histMap.size() - 1;
    }

    /**
     * Retrieve absolute file index by checkpoint entry.
     *
     * @param pointer checkpoint entry for which need to calculate absolute file index.
     * @return absolute file index for given checkpoint entry.
     */
    private long absFileIdx(CheckpointEntry pointer) {
        return ((FileWALPointer)pointer.checkpointMark()).index();
    }

    /**
     * Calculates indexes of WAL segments covered by last checkpoint.
     *
     * @return list of indexes or empty list if there are no checkpoints.
     */
    private IgniteBiTuple<Long, Long> calculateWalSegmentsCovered() {
        IgniteBiTuple<Long, Long> tup = new IgniteBiTuple<>(-1L, -1L);

        Map.Entry<Long, CheckpointEntry> lastEntry = histMap.lastEntry();

        if (lastEntry == null)
            return tup;

        Map.Entry<Long, CheckpointEntry> previousEntry = histMap.lowerEntry(lastEntry.getKey());

        WALPointer lastWALPointer = lastEntry.getValue().checkpointMark();

        long lastIdx = 0;

        long prevIdx = 0;

        if (lastWALPointer instanceof FileWALPointer) {
            lastIdx = ((FileWALPointer)lastWALPointer).index();

            if (previousEntry != null)
                prevIdx = ((FileWALPointer)previousEntry.getValue().checkpointMark()).index();
        }

        tup.set1(prevIdx);
        tup.set2(lastIdx - 1);

        return tup;
    }

    /**
     * Search the earliest WAL pointer for particular group, matching by counter for partitions.
     *
     * @param grpId Group id.
     * @param partsCounter Partition mapped to update counter.
     * @return Earliest WAL pointer for group specified.
     */
    @Nullable public WALPointer searchEarliestWalPointer(
        int grpId,
        Map<Integer, Long> partsCounter,
        long margin
    ) throws IgniteCheckedException {
        if (F.isEmpty(partsCounter))
            return null;

        Map<Integer, Long> modifiedPartsCounter = new HashMap<>(partsCounter);

        FileWALPointer minPtr = null;

        LinkedList<WalPointerCandidate> historyPointerCandidate = new LinkedList<>();

        for (Long cpTs : checkpoints(true)) {
            CheckpointEntry cpEntry = entry(cpTs);

            while (!F.isEmpty(historyPointerCandidate)) {
                FileWALPointer ptr = historyPointerCandidate.poll()
                    .choose(cpEntry, margin, partsCounter);

                if (minPtr == null || ptr.compareTo(minPtr) < 0)
                    minPtr = ptr;
            }

            Iterator<Map.Entry<Integer, Long>> iter = modifiedPartsCounter.entrySet().iterator();

            while (iter.hasNext()) {
                Map.Entry<Integer, Long> entry = iter.next();

                Long foundCntr = cpEntry.partitionCounter(cctx, grpId, entry.getKey());

                if (foundCntr != null && foundCntr <= entry.getValue()) {
                    iter.remove();

                    FileWALPointer ptr = (FileWALPointer)cpEntry.checkpointMark();

                    if (ptr == null) {
                        throw new IgniteCheckedException("Could not find start pointer for partition [part="
                            + entry.getKey() + ", partCntrSince=" + entry.getValue() + "]");
                    }

                    if (foundCntr + margin > entry.getValue()) {
                        historyPointerCandidate.add(new WalPointerCandidate(grpId, entry.getKey(), entry.getValue(), ptr,
                            foundCntr));

                        continue;
                    }

                    partsCounter.put(entry.getKey(), entry.getValue() - margin);

                    if (minPtr == null || ptr.compareTo(minPtr) < 0)
                        minPtr = ptr;
                }
            }

            if (F.isEmpty(modifiedPartsCounter) && F.isEmpty(historyPointerCandidate))
                return minPtr;
        }

        if (!F.isEmpty(modifiedPartsCounter)) {
            Map.Entry<Integer, Long> entry = modifiedPartsCounter.entrySet().iterator().next();

            throw new IgniteCheckedException("Could not find start pointer for partition [part="
                + entry.getKey() + ", partCntrSince=" + entry.getValue() + "]");
        }

        while (!F.isEmpty(historyPointerCandidate)) {
            FileWALPointer ptr = historyPointerCandidate.poll()
                .choose(null, margin, partsCounter);

            if (minPtr == null || ptr.compareTo(minPtr) < 0)
                minPtr = ptr;
        }

        return minPtr;
    }

    /**
     * The class is used for get a pointer with a specific margin.
     * This stores the nearest pointer which covering a partition counter.
     * It is able to choose between other pointer and this.
     */
    private class WalPointerCandidate {
        /** Group id. */
        private int grpId;

        /** Partition id. */
        private int part;

        /** Partition counter. */
        private long partContr;

        /** WAL pointer. */
        private FileWALPointer walPntr;

        /** Partition counter at the moment of WAL pointer. */
        private long walPntrCntr;

        /**
         * @param grpId Group id.
         * @param part Partition id.
         * @param partContr Partition counter.
         * @param walPntr WAL pointer.
         * @param walPntrCntr Counter of WAL pointer.
         */
        public WalPointerCandidate(int grpId, int part, long partContr, FileWALPointer walPntr, long walPntrCntr) {
            this.grpId = grpId;
            this.part = part;
            this.partContr = partContr;
            this.walPntr = walPntr;
            this.walPntrCntr = walPntrCntr;
        }

        /**
         * Make a choice between stored WAL pointer and other, getting from checkpoint, with a specific margin.
         * Updates counter in collection from parameters.
         *
         * @param cpEntry Checkpoint entry.
         * @param margin Margin.
         * @param partCntsForUpdate Collection of partition id by counter.
         * @return Chosen WAL pointer.
         */
        public FileWALPointer choose(
            CheckpointEntry cpEntry,
            long margin,
            Map<Integer, Long> partCntsForUpdate
        ) {
            Long foundCntr = cpEntry == null ? null : cpEntry.partitionCounter(cctx, grpId, part);

            if (foundCntr == null || foundCntr == walPntrCntr) {
                partCntsForUpdate.put(part, walPntrCntr);

                return walPntr;
            }

            partCntsForUpdate.put(part, Math.max(foundCntr, partContr - margin));

            return (FileWALPointer)cpEntry.checkpointMark();
        }
    }

    /**
     * Tries to search for a WAL pointer for the given partition counter start.
     *
     * @param searchCntrMap Search map contains (Group Id, partition, counter).
     * @return Map of group-partition on checkpoint entry or empty map if nothing found.
     */
    @Nullable public Map<GroupPartitionId, CheckpointEntry> searchCheckpointEntry(Map<T2<Integer, Integer>, Long> searchCntrMap) {
        if (F.isEmpty(searchCntrMap))
            return Collections.emptyMap();

        Map<T2<Integer, Integer>, Long> modifiedSearchMap = new HashMap<>(searchCntrMap);

        Map<GroupPartitionId, CheckpointEntry> res = new HashMap<>();

        for (Long cpTs : checkpoints(true)) {
            try {
                CheckpointEntry cpEntry = entry(cpTs);

                Iterator<Map.Entry<T2<Integer, Integer>, Long>> iter = modifiedSearchMap.entrySet().iterator();

                while (iter.hasNext()) {
                    Map.Entry<T2<Integer, Integer>, Long> entry = iter.next();

                    Long foundCntr = cpEntry.partitionCounter(cctx, entry.getKey().get1(), entry.getKey().get2());

                    if (foundCntr != null && foundCntr <= entry.getValue()) {
                        iter.remove();

                        res.put(new GroupPartitionId(entry.getKey().get1(), entry.getKey().get2()), cpEntry);
                    }
                }

                if (F.isEmpty(modifiedSearchMap))
                    return res;
            }
            catch (IgniteCheckedException ignore) {
                break;
            }
        }

        if (!F.isEmpty(modifiedSearchMap))
            return Collections.emptyMap();

        return res;
    }

    /**
     * Tries to search for a WAL pointer for the given partition counter start.
     *
     * @param grpId Cache group ID.
     * @param part Partition ID.
     * @param partCntrSince Partition counter or {@code null} to search for minimal counter.
     * @return Checkpoint entry or {@code null} if failed to search.
     */
    @Nullable public CheckpointEntry searchCheckpointEntry(int grpId, int part, long partCntrSince) {
        for (Long cpTs : checkpoints(true)) {
            try {
                CheckpointEntry entry = entry(cpTs);

                Long foundCntr = entry.partitionCounter(cctx, grpId, part);

                if (foundCntr != null && foundCntr <= partCntrSince)
                    return entry;
            }
            catch (IgniteCheckedException ignore) {
                break;
            }
        }

        return null;
    }

    /**
     * Finds and reserves earliest valid checkpoint for each of given groups and partitions.
     *
     * @param groupsAndPartitions Groups and partitions to find and reserve earliest valid checkpoint.
     *
     * @return Checkpoint history reult: Map (groupId, Reason (the reason why reservation cannot be made deeper): Map
     * (partitionId, earliest valid checkpoint to history search)) and reserved checkpoint.
     */
    public CheckpointHistoryResult searchAndReserveCheckpoints(
        final Map<Integer, Set<Integer>> groupsAndPartitions
    ) {
        if (F.isEmpty(groupsAndPartitions) ||
            cctx.kernalContext().config().getDataStorageConfiguration().getWalMode() == WALMode.NONE)
            return new CheckpointHistoryResult(Collections.emptyMap(), null);

        final Map<Integer, T2<ReservationReason, Map<Integer, CheckpointEntry>>> res = new HashMap<>();

        CheckpointEntry oldestCpForReservation = null;

        synchronized (earliestCp) {
            CheckpointEntry oldestHistCpEntry = firstCheckpoint();

            for (Integer grpId : groupsAndPartitions.keySet()) {
                CheckpointEntry oldestGrpCpEntry = null;

                for (Integer part : groupsAndPartitions.get(grpId)) {
                    CheckpointEntry cpEntry = earliestCp.get(new GroupPartitionId(grpId, part));

                    if (cpEntry == null)
                        continue;

                    if (oldestCpForReservation == null || oldestCpForReservation.timestamp() > cpEntry.timestamp())
                        oldestCpForReservation = cpEntry;

                    if (oldestGrpCpEntry == null || oldestGrpCpEntry.timestamp() > cpEntry.timestamp())
                        oldestGrpCpEntry = cpEntry;

                    res.computeIfAbsent(grpId, partCpMap ->
                        new T2<>(ReservationReason.NO_MORE_HISTORY, new HashMap<>()))
                        .get2().put(part, cpEntry);
                }

                if (oldestGrpCpEntry == null || oldestGrpCpEntry != oldestHistCpEntry)
                    res.computeIfAbsent(grpId, (partCpMap) ->
                        new T2<>(ReservationReason.CHECKPOINT_NOT_APPLICABLE, null))
                        .set1(ReservationReason.CHECKPOINT_NOT_APPLICABLE);
            }
        }

        if (oldestCpForReservation != null) {
            if (!cctx.wal().reserve(oldestCpForReservation.checkpointMark())) {
                log.warning("Could not reserve cp " + oldestCpForReservation.checkpointMark());

                for (Map.Entry<Integer, T2<ReservationReason, Map<Integer, CheckpointEntry>>> entry : res.entrySet())
                    entry.setValue(new T2<>(ReservationReason.WAL_RESERVATION_ERROR, null));

                oldestCpForReservation = null;
            }
        }

        return new CheckpointHistoryResult(res, oldestCpForReservation);
    }

    /**
     * Checkpoint is not applicable when:
     * 1) WAL was disabled somewhere after given checkpoint.
     * 2) Checkpoint doesn't contain specified {@code grpId}.
     *
     * @param grpId Group ID.
     * @param cp Checkpoint.
     */
    public boolean isCheckpointApplicableForGroup(int grpId, CheckpointEntry cp) throws IgniteCheckedException {
        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager) cctx.database();

        if (dbMgr.isCheckpointInapplicableForWalRebalance(cp.timestamp(), grpId))
            return false;

        if (!cp.groupState(cctx).containsKey(grpId))
            return false;

        return true;
    }
}
