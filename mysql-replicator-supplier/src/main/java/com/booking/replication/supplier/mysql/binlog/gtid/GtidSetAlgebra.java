package com.booking.replication.supplier.mysql.binlog.gtid;

import com.booking.replication.commons.checkpoint.Checkpoint;

import java.util.*;

public class GtidSetAlgebra {

    private final Map<String, Set<String>> serverTransactionRanges;

    private final Map<String, Set<String>> serverTransactionPrefixIntervals;
    private final Map<String, Set<Long>> serverLastTransactionIntervalUpperLimits;
    private final Map<String, Map<Long, String>> serverTransactionUpperLimitToRange;
    private final Map<String, Checkpoint> gtidSetToCheckpoint;

    public GtidSetAlgebra() {
        this.serverTransactionRanges = new TreeMap<>();
        this.serverLastTransactionIntervalUpperLimits = new TreeMap<>();
        this.serverTransactionUpperLimitToRange = new TreeMap<>();
        this.gtidSetToCheckpoint = new TreeMap<>();
        this.serverTransactionPrefixIntervals = new TreeMap<>();
    }

    public synchronized Checkpoint getSafeCheckpoint(List<Checkpoint> checkpointsSeenWithGtidSet) {

        Checkpoint safeCheckpoint;

        for (Checkpoint checkpoint : checkpointsSeenWithGtidSet) {
            String seenGTIDSet = sortGTIDSet(checkpoint.getGtidSet());
            gtidSetToCheckpoint.put(seenGTIDSet, checkpoint);
            addGTIDSetToServersTransactionRangeMap(seenGTIDSet);
        }

        Map<String, Set<Long>> filteredGTIDSets = getSafeGTIDSetForApplierCommitedTransactions();

        Map<String, String> last = extractFinalRanges(filteredGTIDSets);

        String safeGTIDSet = sortGTIDSet(getSafeGTIDSet(last));

        safeCheckpoint = new Checkpoint(
                gtidSetToCheckpoint.get(safeGTIDSet).getTimestamp(),
                gtidSetToCheckpoint.get(safeGTIDSet).getServerId(),
                gtidSetToCheckpoint.get(safeGTIDSet).getGtid(),
                gtidSetToCheckpoint.get(safeGTIDSet).getBinlog(),
                gtidSetToCheckpoint.get(safeGTIDSet).getGtidSet()
        );

        serverTransactionUpperLimitToRange.clear();
        serverLastTransactionIntervalUpperLimits.clear();
        serverTransactionRanges.clear();
        gtidSetToCheckpoint.clear();
        serverTransactionPrefixIntervals.clear();

        return safeCheckpoint;
    }

    private String getSafeGTIDSet(Map<String, String> last) {
        String safeGtidSet = "";
        StringJoiner sj = new StringJoiner(",");
        for (String serverId: last.keySet()) {
            safeGtidSet += serverId;
            safeGtidSet += ":";
            safeGtidSet += last.get(serverId);
            sj.add(safeGtidSet);
            safeGtidSet = "";
        }
        return  sj.toString();
    }

    private String sortGTIDSet(String gtidSet) {
        String[] items = gtidSet.split(",");
        Arrays.sort(items);
        StringJoiner sj = new StringJoiner(",");
        for (String item: items) {
            sj.add(item);
        }
        return  sj.toString();
    }

    private  Map<String, String> extractFinalRanges(Map<String, Set<Long>> filteredGTIDSets) {

        Map<String, String> last = new HashMap<>();
        for (String serverId : filteredGTIDSets.keySet() ) {
            TreeSet<Long> upperLimits = (TreeSet<Long>) filteredGTIDSets.get(serverId);
            String lastRange =  "";
            for (String x : serverTransactionPrefixIntervals.get(serverId)) {
                if (!lastRange.equals("")) {
                    lastRange += ":" + x;
                } else {
                    lastRange = x;
                }
            }
            if (!lastRange.equals("")) {
                lastRange += ":" + serverTransactionUpperLimitToRange.get(serverId).get(upperLimits.last());
            } else {
                lastRange = serverTransactionUpperLimitToRange.get(serverId).get(upperLimits.last());
                serverTransactionUpperLimitToRange.get(serverId).get(upperLimits.last());
            }
            last.put(
                    serverId,
                    lastRange
            );
        }
        return last;
    }

    private Long getRangeUpperLimit(String range) {
        return  Long.parseLong(range.split("-")[1]);
    }

    public void addGTIDSetToServersTransactionRangeMap(String gtidSet) {

        String [] serverRanges = gtidSet.split(",");

        for (String serverRange: serverRanges) {

            String[] slices = serverRange.split(":");
            String serverUUID = slices[0];

            if (serverTransactionPrefixIntervals.get(serverUUID) == null) {
                serverTransactionPrefixIntervals.put(serverUUID, new HashSet<>());
            }
            if (serverTransactionRanges.get(serverUUID) == null) {
                serverTransactionRanges.put(serverUUID, new HashSet<>());
            }
            if (serverLastTransactionIntervalUpperLimits.get(serverUUID) == null) {
                serverLastTransactionIntervalUpperLimits.put(serverUUID, new TreeSet<>());
            }
            if (serverTransactionUpperLimitToRange.get(serverUUID) == null) {
                serverTransactionUpperLimitToRange.put(serverUUID, new HashMap<>());
            }

            for (int i = 1; i < slices.length - 1; i++ ) {
                serverTransactionPrefixIntervals.get(serverUUID).add(slices[i]);
            }

            // add last slice
            int lastSliceIndex = slices.length - 1;

            serverTransactionRanges.get(serverUUID).add(slices[lastSliceIndex]);

            serverLastTransactionIntervalUpperLimits.get(serverUUID).add(getRangeUpperLimit(slices[lastSliceIndex]));

            serverTransactionUpperLimitToRange.get(serverUUID).put(
                    getRangeUpperLimit(slices[lastSliceIndex]),
                    slices[lastSliceIndex]
            );

        }
    }

    // remove the gaps and all transactions higher than the gap beginning
    private Map<String, Set<Long>> getSafeGTIDSetForApplierCommitedTransactions() {
        Map<String, Set<Long>> uninteruptedGTIDRanges = new HashMap<>();
        for (String server : serverTransactionRanges.keySet()) {
            uninteruptedGTIDRanges.put(
                    server,
                    getMaxUninteruptedRangeStartingFromMinimalTransaction(server)
            );
        }

        return  uninteruptedGTIDRanges;
    }


    private TreeSet<Long> getMaxUninteruptedRangeStartingFromMinimalTransaction (String serverId) {

        TreeSet<Long> range = (TreeSet) serverLastTransactionIntervalUpperLimits.get(serverId);
        TreeSet<Long> uninteruptedRange = new TreeSet<>();

        Long position = range.first();

        for (Long item : range) {
            if ((item >= position)) {
                if (item - position == 1) {
                    position = item;
                } else {
                    // gap
                }
            } else {
                throw new RuntimeException("Error in logic");
            }
        }

        Long item = range.pollFirst();
        while (item != null && item <= position) {
            uninteruptedRange.add(item);
            item = range.pollFirst();
        }

        return uninteruptedRange;
    }

}
