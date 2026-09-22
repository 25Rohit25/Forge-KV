package com.forgekv.benchmark;

import com.forgekv.client.ForgeKVClient;
import com.forgekv.client.ForgeKVQueueClient;
import com.forgekv.queue.proto.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Benchmark runner implementing the workloads from Section 20 of the guide:
 * - Write-only (PUT random 32-byte keys, 256-byte values)
 * - Read-only (GET preloaded existing keys)
 * - Mixed (80% GET / 20% PUT)
 * - Queue (Enqueue + worker claim/ack loop)
 *
 * Produces throughput (ops/sec) and p50, p95, p99 latency percentiles.
 */
public class BenchmarkRunner {

    private static final Logger log = LoggerFactory.getLogger(BenchmarkRunner.class);

    public record BenchmarkResult(
            String workload,
            int concurrency,
            long totalOps,
            double opsPerSec,
            double p50Ms,
            double p95Ms,
            double p99Ms,
            long errors
    ) {
        public void printSummary() {
            System.out.println(String.format(
                    "| %-12s | %-7d | %-10.1f | %-8.2f | %-8.2f | %-8.2f | %-6d |",
                    workload, concurrency, opsPerSec, p50Ms, p95Ms, p99Ms, errors
            ));
        }
    }

    public static BenchmarkResult runWriteBenchmark(Map<String, String> nodes, int concurrency, int durationSeconds) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(concurrency);

        List<Long> latenciesNanos = Collections.synchronizedList(new ArrayList<>());
        AtomicLong errorCount = new AtomicLong(0);
        long endTime = System.currentTimeMillis() + (durationSeconds * 1000L);

        for (int i = 0; i < concurrency; i++) {
            final int workerId = i;
            pool.submit(() -> {
                try (ForgeKVClient client = new ForgeKVClient(nodes)) {
                    startLatch.await();
                    Random rng = new Random(workerId);
                    byte[] val = new byte[256];
                    rng.nextBytes(val);

                    while (System.currentTimeMillis() < endTime) {
                        String key = "bench-key-" + workerId + "-" + rng.nextInt(10000);
                        long start = System.nanoTime();
                        try {
                            client.put(key.getBytes(StandardCharsets.UTF_8), val);
                            latenciesNanos.add(System.nanoTime() - start);
                        } catch (Exception ex) {
                            errorCount.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        long actualStart = System.currentTimeMillis();
        startLatch.countDown();
        doneLatch.await();
        long actualEnd = System.currentTimeMillis();
        pool.shutdown();

        double elapsedSec = Math.max(1, (actualEnd - actualStart) / 1000.0);
        return calculateMetrics("Write-Only", concurrency, latenciesNanos, errorCount.get(), elapsedSec);
    }

    public static BenchmarkResult runMixedBenchmark(Map<String, String> nodes, int concurrency, int durationSeconds) throws Exception {
        // Preload keys first
        try (ForgeKVClient client = new ForgeKVClient(nodes)) {
            for (int i = 0; i < 50; i++) {
                client.putString("mixed-key-" + i, "val-" + i);
            }
        }

        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(concurrency);

        List<Long> latenciesNanos = Collections.synchronizedList(new ArrayList<>());
        AtomicLong errorCount = new AtomicLong(0);
        long endTime = System.currentTimeMillis() + (durationSeconds * 1000L);

        for (int i = 0; i < concurrency; i++) {
            final int workerId = i;
            pool.submit(() -> {
                try (ForgeKVClient client = new ForgeKVClient(nodes)) {
                    startLatch.await();
                    Random rng = new Random(workerId);

                    while (System.currentTimeMillis() < endTime) {
                        int r = rng.nextInt(100);
                        long start = System.nanoTime();
                        try {
                            if (r < 80) {
                                // 80% GET
                                client.getString("mixed-key-" + rng.nextInt(50));
                            } else {
                                // 20% PUT
                                client.putString("mixed-key-" + rng.nextInt(50), "update-" + rng.nextInt());
                            }
                            latenciesNanos.add(System.nanoTime() - start);
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        long actualStart = System.currentTimeMillis();
        startLatch.countDown();
        doneLatch.await();
        long actualEnd = System.currentTimeMillis();
        pool.shutdown();

        double elapsedSec = Math.max(1, (actualEnd - actualStart) / 1000.0);
        return calculateMetrics("Mixed 80/20", concurrency, latenciesNanos, errorCount.get(), elapsedSec);
    }

    public static BenchmarkResult runQueueBenchmark(Map<String, String> nodes, int concurrency, int totalJobs) throws Exception {
        try (ForgeKVQueueClient client = new ForgeKVQueueClient(nodes)) {
            for (int i = 0; i < totalJobs; i++) {
                client.enqueue("bench-q", "payload-" + i);
            }
        }

        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        CountDownLatch doneLatch = new CountDownLatch(concurrency);
        List<Long> latenciesNanos = Collections.synchronizedList(new ArrayList<>());
        AtomicLong errorCount = new AtomicLong(0);
        long startMs = System.currentTimeMillis();

        for (int i = 0; i < concurrency; i++) {
            final String workerId = "bench-worker-" + i;
            pool.submit(() -> {
                try (ForgeKVQueueClient client = new ForgeKVQueueClient(nodes)) {
                    while (true) {
                        long start = System.nanoTime();
                        Optional<Job> jobOpt = client.claim("bench-q", workerId, 5000);
                        if (jobOpt.isEmpty()) {
                            break; // no more ready jobs
                        }
                        client.ack(jobOpt.get().getId(), workerId);
                        latenciesNanos.add(System.nanoTime() - start);
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        doneLatch.await();
        long endMs = System.currentTimeMillis();
        pool.shutdown();

        double elapsedSec = Math.max(0.1, (endMs - startMs) / 1000.0);
        return calculateMetrics("Queue Claim/Ack", concurrency, latenciesNanos, errorCount.get(), elapsedSec);
    }

    private static BenchmarkResult calculateMetrics(String workload, int concurrency, List<Long> latenciesNanos, long errors, double elapsedSec) {
        long totalOps = latenciesNanos.size();
        double opsPerSec = totalOps / elapsedSec;

        Collections.sort(latenciesNanos);
        double p50 = totalOps > 0 ? latenciesNanos.get((int) (totalOps * 0.50)) / 1_000_000.0 : 0;
        double p95 = totalOps > 0 ? latenciesNanos.get((int) (totalOps * 0.95)) / 1_000_000.0 : 0;
        double p99 = totalOps > 0 ? latenciesNanos.get((int) (totalOps * 0.99)) / 1_000_000.0 : 0;

        return new BenchmarkResult(workload, concurrency, totalOps, opsPerSec, p50, p95, p99, errors);
    }

    public static void printHeader() {
        System.out.println("+--------------+---------+------------+----------+----------+----------+--------+");
        System.out.println("| Workload     | Clients | Write ops/s| p50 ms   | p95 ms   | p99 ms   | Errors |");
        System.out.println("+--------------+---------+------------+----------+----------+----------+--------+");
    }

    public static void printFooter() {
        System.out.println("+--------------+---------+------------+----------+----------+----------+--------+");
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> nodes = Map.of(
                "node1", "127.0.0.1:7001",
                "node2", "127.0.0.1:7002",
                "node3", "127.0.0.1:7003"
        );
        System.out.println("Starting ForgeKV Benchmark against cluster...");
        printHeader();
        BenchmarkResult writeRes = runWriteBenchmark(nodes, 10, 5);
        writeRes.printSummary();

        BenchmarkResult mixedRes = runMixedBenchmark(nodes, 10, 5);
        mixedRes.printSummary();
        printFooter();
    }
}
