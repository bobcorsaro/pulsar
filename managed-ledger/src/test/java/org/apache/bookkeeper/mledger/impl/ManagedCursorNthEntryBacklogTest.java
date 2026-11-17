/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.mledger.impl;

import static org.apache.bookkeeper.mledger.util.ManagedLedgerTestUtil.defaultConfig;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursor.IndividualDeletedEntries;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.annotations.Test;

/**
 * Checks {@link ManagedCursor#getNthEntry} against a real managed ledger when the backlog contains individually
 * acknowledged entries, which is what a per-subscription backlog age quantile lookup relies on.
 */
public class ManagedCursorNthEntryBacklogTest extends MockedBookKeeperTestCase {

    private static String payload(int i) {
        return "msg" + i;
    }

    @Test(timeOut = 60000)
    void nthEntryMatchesUnackedEntriesWithRandomIndividualAcks() throws Exception {
        ManagedLedger ledger = factory.open("nthEntryRandomAcks", initManagedLedgerConfig(defaultConfig()));
        ManagedCursor cursor = ledger.openCursor("c1");

        int total = 500;
        List<Position> positions = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            positions.add(ledger.addEntry(payload(i).getBytes(StandardCharsets.UTF_8)));
        }

        // Leave entry 0 unacked so the mark-delete position stays put and every ack below is an individual one.
        Random random = new Random(42);
        Set<Integer> acked = new HashSet<>();
        while (acked.size() < 200) {
            int i = 1 + random.nextInt(total - 1);
            if (acked.add(i)) {
                cursor.delete(positions.get(i));
            }
        }

        List<Integer> unacked = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            if (!acked.contains(i)) {
                unacked.add(i);
            }
        }
        assertEquals(cursor.getNumberOfEntriesInBacklog(true), unacked.size());

        List<Integer> ranks = new ArrayList<>(List.of(1, 2, unacked.size() / 2, unacked.size() - 1, unacked.size()));
        for (int k = 0; k < 50; k++) {
            ranks.add(1 + random.nextInt(unacked.size()));
        }
        for (int n : ranks) {
            Entry entry = cursor.getNthEntry(n, IndividualDeletedEntries.Exclude);
            assertNotNull(entry, "rank " + n);
            assertEquals(new String(entry.getDataAndRelease(), StandardCharsets.UTF_8), payload(unacked.get(n - 1)),
                    "rank " + n);
        }

        assertNull(cursor.getNthEntry(unacked.size() + 1, IndividualDeletedEntries.Exclude));
    }

    @Test(timeOut = 60000)
    void nthEntryIsCorrectAfterMarkDeleteAdvancesOverAckedRanges() throws Exception {
        ManagedLedger ledger = factory.open("nthEntryMarkDelete", initManagedLedgerConfig(defaultConfig()));
        ManagedCursor cursor = ledger.openCursor("c1");

        List<Position> positions = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            positions.add(ledger.addEntry(payload(i).getBytes(StandardCharsets.UTF_8)));
        }

        // Ack 0..4 contiguously (mark-delete moves), then 7, 8, 12 individually.
        for (int i = 0; i <= 4; i++) {
            cursor.delete(positions.get(i));
        }
        for (int i : new int[] {7, 8, 12}) {
            cursor.delete(positions.get(i));
        }

        List<Integer> unacked = new ArrayList<>();
        for (int i = 5; i < 20; i++) {
            if (i != 7 && i != 8 && i != 12) {
                unacked.add(i);
            }
        }
        assertEquals(cursor.getNumberOfEntriesInBacklog(true), unacked.size());
        for (int n = 1; n <= unacked.size(); n++) {
            Entry entry = cursor.getNthEntry(n, IndividualDeletedEntries.Exclude);
            assertNotNull(entry, "rank " + n);
            assertEquals(new String(entry.getDataAndRelease(), StandardCharsets.UTF_8), payload(unacked.get(n - 1)),
                    "rank " + n);
        }
    }

    /**
     * Measures the cost of the lookup with many individually acknowledged ranges. This prints timings rather than
     * asserting on them, since the numbers depend on the machine.
     */
    @Test(timeOut = 300000)
    void measureNthEntryCostWithManyIndividualAckRanges() throws Exception {
        ManagedLedger ledger = factory.open("nthEntryCost", initManagedLedgerConfig(defaultConfig()));
        ManagedCursor cursor = ledger.openCursor("c1");

        int total = 20_000;
        Position[] added = new Position[total];
        CountDownLatch latch = new CountDownLatch(total);
        for (int i = 0; i < total; i++) {
            final int idx = i;
            ledger.asyncAddEntry(new byte[16], new AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    added[idx] = position;
                    latch.countDown();
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    latch.countDown();
                }
            }, null);
        }
        assertTrue(latch.await(120, TimeUnit.SECONDS));
        List<Position> positions = Arrays.asList(added);

        // Ack every other entry after the first, giving ~50k separate ack ranges.
        for (int i = 2; i < total; i += 2) {
            cursor.delete(positions.get(i));
        }
        long backlog = cursor.getNumberOfEntriesInBacklog(true);
        System.out.println("QUANTILE-BENCH backlog=" + backlog + " ackRanges=" + (total / 2 - 1));

        double[] quantiles = {0.99, 0.9, 0.5, 0.0};
        for (double q : quantiles) {
            long n = Math.max(1, (long) Math.ceil(backlog * (1 - q) - 1e-9));
            // Warm up once, then time a few reads.
            release(cursor.getNthEntry((int) n, IndividualDeletedEntries.Exclude));
            int reps = 5;
            long start = System.nanoTime();
            for (int r = 0; r < reps; r++) {
                release(cursor.getNthEntry((int) n, IndividualDeletedEntries.Exclude));
            }
            double ms = (System.nanoTime() - start) / 1e6 / reps;
            System.out.printf("QUANTILE-BENCH q=%.2f rank=%d avgMs=%.3f%n", q, n, ms);
        }

        // Also measure the walk by itself to separate it from the entry read.
        ManagedCursorImpl impl = (ManagedCursorImpl) cursor;
        Position md = impl.getMarkDeletedPosition();
        for (double q : quantiles) {
            long n = Math.max(1, (long) Math.ceil(backlog * (1 - q) - 1e-9));
            long start = System.nanoTime();
            long skipped = impl.getNumIndividualDeletedEntriesToSkip(md, n);
            double ms = (System.nanoTime() - start) / 1e6;
            System.out.printf("QUANTILE-BENCH walk q=%.2f rank=%d skipped=%d ms=%.3f%n", q, n, skipped, ms);
        }
    }

    /**
     * Measures how long individual acks wait while another thread repeatedly runs the worst-case lookup (rank equal
     * to the backlog size) on a large backlog with many ack ranges. Prints timings rather than asserting on them.
     */
    @Test(timeOut = 900000)
    void measureAckLatencyWhileNthEntryLookupRuns() throws Exception {
        // Persisting the ack state truncates at this many ranges. Keeping it small stops every ack from serializing a
        // huge range list, which would otherwise exhaust the test heap. The in-memory ack ranges, which the lookup
        // walks, are not affected.
        ManagedLedgerConfig config = initManagedLedgerConfig(defaultConfig());
        config.setMaxUnackedRangesToPersist(100);
        config.setMaxUnackedRangesToPersistInMetadataStore(50);
        ManagedLedger ledger = factory.open("prop/ns/persistent/nthEntryAckContention", config);
        ManagedCursor cursor = ledger.openCursor("c1");

        int total = 50_000;
        Position[] added = new Position[total];
        Semaphore inFlight = new Semaphore(1000);
        CountDownLatch latch = new CountDownLatch(total);
        long addStart = System.nanoTime();
        for (int i = 0; i < total; i++) {
            final int idx = i;
            inFlight.acquire();
            ledger.asyncAddEntry(new byte[16], new AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    added[idx] = position;
                    inFlight.release();
                    latch.countDown();
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    inFlight.release();
                    latch.countDown();
                }
            }, null);
            if (i % 10_000 == 0) {
                System.out.printf("ACK-BENCH added %d entries so far, %d ms%n", i,
                        (System.nanoTime() - addStart) / 1_000_000);
            }
        }
        assertTrue(latch.await(600, TimeUnit.SECONDS));
        System.out.printf("ACK-BENCH added %d entries in %d ms%n", total, (System.nanoTime() - addStart) / 1_000_000);

        // Ack the even entries after the first, leaving ~250k ack ranges. The odd entries stay as the backlog.
        for (int i = 2; i < total; i += 2) {
            cursor.delete(added[i]);
        }
        long backlog = cursor.getNumberOfEntriesInBacklog(true);
        System.out.println("ACK-BENCH backlog=" + backlog + " ackRanges=" + (total / 2 - 1));

        ManagedCursorImpl impl = (ManagedCursorImpl) cursor;
        long walkStart = System.nanoTime();
        long skipped = impl.getNumIndividualDeletedEntriesToSkip(impl.getMarkDeletedPosition(), backlog);
        System.out.printf("ACK-BENCH single worst-case walk skipped=%d ms=%.2f%n", skipped,
                (System.nanoTime() - walkStart) / 1e6);

        int acks = 1000;
        // Baseline: acks with no lookup running. Acks the odd entries from the top, which are in the backlog.
        long[] baseline = new long[acks];
        int next = total - 1;
        for (int a = 0; a < acks; a++, next -= 2) {
            long t = System.nanoTime();
            cursor.delete(added[next]);
            baseline[a] = System.nanoTime() - t;
        }
        printStats("ACK-BENCH baseline ack", baseline);

        // Contended: same acks while a thread loops the worst-case lookup.
        AtomicBoolean stop = new AtomicBoolean();
        AtomicLong lookups = new AtomicLong();
        Thread walker = new Thread(() -> {
            while (!stop.get()) {
                try {
                    release(cursor.getNthEntry((int) cursor.getNumberOfEntriesInBacklog(true),
                            IndividualDeletedEntries.Exclude));
                    lookups.incrementAndGet();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        walker.start();
        Thread.sleep(500);
        long[] contended = new long[acks];
        for (int a = 0; a < acks; a++, next -= 2) {
            long t = System.nanoTime();
            cursor.delete(added[next]);
            contended[a] = System.nanoTime() - t;
        }
        stop.set(true);
        walker.join();
        System.out.println("ACK-BENCH lookups completed during contended acks=" + lookups.get());
        printStats("ACK-BENCH contended ack", contended);
    }

    /**
     * Compares the bitmap-based lookup with the walk over the deleted ranges, for every rank, on backlogs that span
     * several ledgers and have random individual acks, with and without the mark-delete position moving.
     */
    @Test(timeOut = 120000)
    void bitmapLookupMatchesTheRangeWalkAcrossLedgers() throws Exception {
        for (long seed = 1; seed <= 6; seed++) {
            ManagedLedgerConfig config = initManagedLedgerConfig(defaultConfig());
            config.setMaxEntriesPerLedger(37);
            ManagedLedger ledger = factory.open("prop/ns/persistent/bitmapLookup" + seed, config);
            ManagedCursor cursor = ledger.openCursor("c1");
            ManagedCursorImpl impl = (ManagedCursorImpl) cursor;

            int total = 400;
            List<Position> positions = new ArrayList<>();
            for (int i = 0; i < total; i++) {
                positions.add(ledger.addEntry(payload(i).getBytes(StandardCharsets.UTF_8)));
            }
            assertTrue(((ManagedLedgerImpl) ledger).getLedgersInfo().size() > 5);

            Random random = new Random(seed);
            // Even seeds also ack a contiguous prefix, so the mark-delete position moves into the middle.
            int firstUnacked = seed % 2 == 0 ? 1 + random.nextInt(120) : 0;
            for (int i = 0; i < firstUnacked; i++) {
                cursor.delete(positions.get(i));
            }
            double ackFraction = 0.2 + 0.7 * random.nextDouble();
            Set<Integer> acked = new HashSet<>();
            for (int i = firstUnacked + 1; i < total; i++) {
                if (random.nextDouble() < ackFraction) {
                    acked.add(i);
                    cursor.delete(positions.get(i));
                }
            }

            long backlog = cursor.getNumberOfEntriesInBacklog(true);
            assertTrue(backlog > 0);
            impl.lock.readLock().lock();
            try {
                Position markDelete = impl.getMarkDeletedPosition();
                for (long n = 1; n <= backlog + 1; n++) {
                    Position bitmap = impl.getNthNotDeletedPositionAfterMarkDelete(markDelete, n);
                    if (n > backlog) {
                        assertNull(bitmap, "seed " + seed + " rank " + n);
                        continue;
                    }
                    Position walk = impl.getPositionAfterMarkDelete(markDelete, n, IndividualDeletedEntries.Exclude);
                    assertEquals(bitmap, walk, "seed " + seed + " rank " + n);
                }
            } finally {
                impl.lock.readLock().unlock();
            }
        }
    }

    /**
     * Times the lookup for several quantiles while the backlog is fragmented into 25k, 100k and 200k individual ack
     * ranges spread evenly over it. The ledger is built once and fragmented in stages. Prints timings rather than
     * asserting on them, since the numbers depend on the machine. Disabled because building the ledger takes about
     * 25 minutes with the mocked BookKeeper; enable it to run the measurement.
     */
    @Test(timeOut = 3600000, enabled = false)
    void measureNthEntryCostAtHighFragmentation() throws Exception {
        ManagedLedgerConfig config = initManagedLedgerConfig(defaultConfig());
        // Keep persisting the ack state cheap. The in-memory ranges, which the lookup walks, are not affected.
        config.setMaxUnackedRangesToPersist(100);
        config.setMaxUnackedRangesToPersistInMetadataStore(50);
        ManagedLedger ledger = factory.open("prop/ns/persistent/nthEntryFragmentation", config);
        ManagedCursor cursor = ledger.openCursor("c1");

        int total = 400_000;
        Position[] added = new Position[total];
        Semaphore inFlight = new Semaphore(1000);
        CountDownLatch latch = new CountDownLatch(total);
        long addStart = System.nanoTime();
        for (int i = 0; i < total; i++) {
            final int idx = i;
            inFlight.acquire();
            ledger.asyncAddEntry(new byte[16], new AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    added[idx] = position;
                    inFlight.release();
                    latch.countDown();
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    inFlight.release();
                    latch.countDown();
                }
            }, null);
            if (i % 50_000 == 0) {
                System.out.printf("FRAG-BENCH added %d entries so far, %d s%n", i,
                        (System.nanoTime() - addStart) / 1_000_000_000L);
            }
        }
        assertTrue(latch.await(3000, TimeUnit.SECONDS));
        System.out.printf("FRAG-BENCH added %d entries in %d s%n", total,
                (System.nanoTime() - addStart) / 1_000_000_000L);

        ManagedCursorImpl impl = (ManagedCursorImpl) cursor;
        // Candidate k acks entry 2 * (k + 1), so each acked entry sits between two unacked ones and forms its own
        // range. Stage 1 acks every 8th candidate (25k ranges), stage 2 every 2nd (100k), stage 3 all (200k).
        int candidates = total / 2 - 1;
        int[] strides = {8, 2, 1};
        for (int stage = 0; stage < strides.length; stage++) {
            final int currentStage = stage;
            List<Position> batch = new ArrayList<>();
            for (int k = 0; k < candidates; k++) {
                // Stage 0 acks k % 8 == 0 (~25k ranges). Stage 1 adds the other even k (~100k in total). Stage 2
                // adds the odd k (~200k in total).
                boolean acksNow = currentStage == 0 ? k % 8 == 0
                        : currentStage == 1 ? (k % 2 == 0 && k % 8 != 0) : k % 2 == 1;
                if (acksNow) {
                    batch.add(added[2 * (k + 1)]);
                    if (batch.size() == 2000) {
                        cursor.delete(batch);
                        batch.clear();
                    }
                }
            }
            if (!batch.isEmpty()) {
                cursor.delete(batch);
            }
            long backlog = cursor.getNumberOfEntriesInBacklog(true);
            int ranges = impl.getTotalNonContiguousDeletedMessagesRange();
            System.out.printf("FRAG-BENCH stage=%d backlog=%d ackRanges=%d%n", currentStage, backlog, ranges);

            double[] quantiles = {0.99, 0.9, 0.5, 0.1, 0.0};
            for (double q : quantiles) {
                long n = Math.max(1, (long) Math.ceil(backlog * (1 - q) - 1e-9));
                long firstStart = System.nanoTime();
                release(cursor.getNthEntry((int) n, IndividualDeletedEntries.Exclude));
                double firstMs = (System.nanoTime() - firstStart) / 1e6;
                int reps = 5;
                long[] times = new long[reps];
                for (int r = 0; r < reps; r++) {
                    long t = System.nanoTime();
                    release(cursor.getNthEntry((int) n, IndividualDeletedEntries.Exclude));
                    times[r] = System.nanoTime() - t;
                }
                Arrays.sort(times);
                long walkStart = System.nanoTime();
                impl.getNumIndividualDeletedEntriesToSkip(impl.getMarkDeletedPosition(), n);
                double walkMs = (System.nanoTime() - walkStart) / 1e6;
                double bitmapMs;
                long[] bitmapTimes = new long[reps];
                impl.lock.readLock().lock();
                try {
                    Position md = impl.getMarkDeletedPosition();
                    Position expected = impl.getPositionAfterMarkDelete(md, n, IndividualDeletedEntries.Exclude);
                    assertEquals(impl.getNthNotDeletedPositionAfterMarkDelete(md, n), expected);
                    for (int r = 0; r < reps; r++) {
                        long t = System.nanoTime();
                        impl.getNthNotDeletedPositionAfterMarkDelete(md, n);
                        bitmapTimes[r] = System.nanoTime() - t;
                    }
                } finally {
                    impl.lock.readLock().unlock();
                }
                Arrays.sort(bitmapTimes);
                bitmapMs = bitmapTimes[reps / 2] / 1e6;
                System.out.printf("FRAG-BENCH ranges=%d q=%.2f rank=%d firstMs=%.3f medianMs=%.3f maxMs=%.3f "
                                + "walkMs=%.3f bitmapMs=%.4f%n", ranges, q, n, firstMs, times[reps / 2] / 1e6,
                        times[reps - 1] / 1e6, walkMs, bitmapMs);
            }
        }
    }

    private static void printStats(String label, long[] nanos) {
        long[] sorted = nanos.clone();
        Arrays.sort(sorted);
        System.out.printf("%s n=%d p50=%.3fms p99=%.3fms max=%.3fms%n", label, sorted.length,
                sorted[sorted.length / 2] / 1e6, sorted[(int) (sorted.length * 0.99)] / 1e6,
                sorted[sorted.length - 1] / 1e6);
    }

    private static void release(Entry entry) {
        if (entry != null) {
            entry.release();
        }
    }
}
