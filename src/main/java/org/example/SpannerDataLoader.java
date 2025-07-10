package org.example;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.AsyncRunner;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Value;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;

/**
 * A data loading utility for Cloud Spanner.
 *
 * <p>This application bulk-loads nodes and edges into a Spanner database using efficient,
 * asynchronous batching. It is designed for one-time data setup rather than continuous throughput
 * testing.
 *
 * <p>Modes of Operation:
 * <ul>
 * <li><b>load_nodes:</b> Inserts a specified number of nodes in batches.
 * <li><b>load_edges:</b> Inserts a predictable edge for each node.
 * <li><b>load_nodes_and_edges:</b> Executes node loading, then basic edge loading. This is
 * the default mode.
 * </ul>
 */
public class SpannerDataLoader {

  // Default configuration values
  private static final int DEFAULT_NUM_KEYS = 30_000_000;
  private static final int DEFAULT_BATCH_SIZE = 2000;
  private static final int DEFAULT_NUM_THREADS = 512;
  private static final String KEY_PREFIX = "deterministic-key-";
  private static final int EDGE_KEY_OFFSET = 250000;


  public static void main(String[] args) throws InterruptedException {
    // 1. Set up and parse command-line options
    org.apache.commons.cli.Options options = setupOptions();
    CommandLine cmd = parseArguments(options, args);

    String project = cmd.getOptionValue("project");
    String instance = cmd.getOptionValue("instance");
    String database = cmd.getOptionValue("database");
    String mode = cmd.getOptionValue("mode", "load_nodes_and_edges");
    final int numKeys =
        Integer.parseInt(cmd.getOptionValue("numKeys", String.valueOf(DEFAULT_NUM_KEYS)));
    final int batchSize =
        Integer.parseInt(cmd.getOptionValue("batchSize", String.valueOf(DEFAULT_BATCH_SIZE)));
    final int numThreads =
        Integer.parseInt(cmd.getOptionValue("numThreads", String.valueOf(DEFAULT_NUM_THREADS)));
    final int maxInFlightOperations = numThreads * 2;

    // 2. Initialize Spanner client, thread pool, and concurrency controls
    Spanner spanner = SpannerOptions.newBuilder().setProjectId(project).build().getService();
    DatabaseClient dbClient =
        spanner.getDatabaseClient(DatabaseId.of(project, instance, database));
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

    System.out.printf(
        "Starting Spanner Data Loader in '%s' mode.%nConfiguration: Nodes=%d, BatchSize=%d, Threads=%d%n",
        mode, numKeys, batchSize, numThreads);

    try {
      if ("load_nodes".equals(mode) || "load_nodes_and_edges".equals(mode)) {
        loadNodes(dbClient, executorService, numKeys, batchSize, maxInFlightOperations);
      }
      if ("load_edges".equals(mode) || "load_nodes_and_edges".equals(mode)) {
        // Every basic edge connects node[i] -> node[i + 2500000]
        loadBasicEdges(dbClient, executorService, numKeys - EDGE_KEY_OFFSET, batchSize);
      }
    } finally {
      System.out.println("Shutting down resources...");
      spanner.close();
      executorService.shutdown();
      System.out.println("Load process complete.");
    }
  }

  /**
   * Orchestrates the loading of nodes into Spanner.
   *
   * <p>It divides the total number of nodes into batches and submits them asynchronously.
   */
  private static void loadNodes(
      DatabaseClient dbClient, ExecutorService executor, int numKeys, int batchSize, int maxInFlight)
      throws InterruptedException {
    System.out.println("\n----- Starting Node Loading -----");
    long startTime = System.currentTimeMillis();
    final int numBatches = (int) Math.ceil((double) numKeys / batchSize);
    final CountDownLatch latch = new CountDownLatch(numBatches);
    final AtomicLong mutationsLoaded = new AtomicLong(0);

    List<Mutation> mutationBatch = new ArrayList<>(batchSize);
    for (int i = 0; i < numKeys; i++) {
      String key = KEY_PREFIX + i;
      String label = "label-" + (i % 100);
      mutationBatch.add(createNodeInsertMutation(label, key));

      if (mutationBatch.size() == batchSize || i == numKeys - 1) {
        final List<Mutation> finalBatch = new ArrayList<>(mutationBatch);
        CompletableFuture.runAsync(
            () -> {
              try {
                dbClient.write(finalBatch);
                long total = mutationsLoaded.addAndGet(finalBatch.size());
                if (total % (batchSize * 10) == 0) {
                  System.out.printf("Loaded %,d of %,d nodes...%n", total, numKeys);
                }
              } catch (Exception e) {
                System.err.println("Failed to write node batch: " + e.getMessage());
              } finally {
                latch.countDown();
              }
            },
            executor);
        mutationBatch.clear();
      }
    }

    latch.await();
    long duration = System.currentTimeMillis() - startTime;
    System.out.printf(
        "----- Node Loading Complete. %,d nodes loaded in %.2f seconds. -----%n",
        mutationsLoaded.get(), duration / 1000.0);
  }

  /**
   * Orchestrates the loading of a single, predictable edge for each node.
   *
   * <p>This is useful for creating a baseline graph structure that can be reliably queried.
   */
  private static void loadBasicEdges(
      DatabaseClient dbClient, ExecutorService executor, int numKeys, int batchSize)
      throws InterruptedException {
    System.out.println("\n----- Starting Basic Edge Loading -----");
    long startTime = System.currentTimeMillis();
    final int numBatches = (int) Math.ceil((double) numKeys / batchSize);
    final CountDownLatch latch = new CountDownLatch(numBatches);
    final AtomicLong mutationsLoaded = new AtomicLong(0);

    List<Mutation> mutationBatch = new ArrayList<>(batchSize);
    for (int i = 0; i < numKeys; i++) {
      String key1 = KEY_PREFIX + i;
      String label1 = "label-" + (i % 100);
      String key2 = KEY_PREFIX + (i + EDGE_KEY_OFFSET);
      String label2 = "label-" + ((i + EDGE_KEY_OFFSET) % 100);
      String edgeLabel = "edgelabel-" + (i % 100);
      mutationBatch.add(createEdgeInsertMutation(label1, key1, edgeLabel, label2, key2));

      if (mutationBatch.size() == batchSize || i == numKeys - 1) {
        final List<Mutation> finalBatch = new ArrayList<>(mutationBatch);
        CompletableFuture.runAsync(
            () -> {
              try {
                dbClient.write(finalBatch);
                long total = mutationsLoaded.addAndGet(finalBatch.size());
                if (total % (batchSize * 10) == 0) {
                  System.out.printf("Loaded %,d of %,d basic edges...%n", total, numKeys);
                }
              } catch (Exception e) {
                System.err.println("Failed to write basic edge batch: " + e.getMessage());
              } finally {
                latch.countDown();
              }
            },
            executor);
        mutationBatch.clear();
      }
    }

    latch.await();
    long duration = System.currentTimeMillis() - startTime;
    System.out.printf(
        "----- Basic Edge Loading Complete. %,d edges loaded in %.2f seconds. -----%n",
        mutationsLoaded.get(), duration / 1000.0);
  }

  /** Creates a pre-defined JSON object of approximately 500 bytes. */
  public static JsonObject create500ByteJsonObject() {
    JsonObject dataObject = new JsonObject();
    dataObject.addProperty("transactionId", "a1b2c3d4-e5f6-7890-1234-567890abcdef");
    dataObject.addProperty("timestamp", "2025-07-10T17:55:00Z"); // Updated timestamp
    dataObject.addProperty("status", "LOADED");
    dataObject.addProperty("sourceSystem", "DataLoader-Primary");
    String description =
        "This is a sample description designed to add weight to the JSON object for data loading purposes.";
    dataObject.addProperty("description", description);
    return dataObject;
  }

  /** Creates an insert mutation for a single node. */
  private static Mutation createNodeInsertMutation(String label, String key) {
    Timestamp now = Timestamp.now();
    JsonObject details = create500ByteJsonObject();
    details.addProperty("upd_count", 0);
    return Mutation.newInsertBuilder("GraphNode")
        .set("label").to(label)
        .set("key").to(key)
        .set("details").to(Value.json(details.toString()))
        .set("first_seen").to(now)
        .set("last_seen").to(now)
        .set("creation_ts").to(now)
        .set("update_ts").to(now)
        .build();
  }

  /** Creates an insert mutation for a single, predictable edge. */
  private static Mutation createEdgeInsertMutation(
      String label1, String key1, String edgeLabel, String label2, String key2) {
    Timestamp now = Timestamp.now();
    JsonObject details = create500ByteJsonObject();
    return Mutation.newInsertBuilder("GraphEdge")
        .set("label").to(label1)
        .set("key").to(key1)
        .set("edge_label").to(edgeLabel)
        .set("other_node_label").to(label2)
        .set("other_node_key").to(key2)
        .set("details").to(Value.json(details.toString()))
        .set("first_seen").to(now)
        .set("last_seen").to(now)
        .set("creation_ts").to(now)
        .set("update_ts").to(now)
        .build();
  }

  /** Sets up the expected command-line options. */
  private static org.apache.commons.cli.Options setupOptions() {
    org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();
    options.addRequiredOption("p", "project", true, "Your Google Cloud project ID.");
    options.addRequiredOption("i", "instance", true, "The Spanner instance ID.");
    options.addRequiredOption("d", "database", true, "The Spanner database ID.");
    options.addOption(
        "m",
        "mode",
        true,
        "Load mode: load_nodes, load_edges, or load_nodes_and_edges (default).");
    options.addOption(
        null, "numKeys", true, "Total number of nodes to generate. Default: " + DEFAULT_NUM_KEYS);
    options.addOption(
        null,
        "batchSize",
        true,
        "Number of mutations per transaction. Default: " + DEFAULT_BATCH_SIZE);
    options.addOption(
        null,
        "numThreads",
        true,
        "Number of threads in the thread pool. Default: " + DEFAULT_NUM_THREADS);
    return options;
  }

  /** Parses command-line arguments and handles errors. */
  private static CommandLine parseArguments(
      org.apache.commons.cli.Options options, String[] args) {
    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    try {
      return parser.parse(options, args);
    } catch (ParseException e) {
      System.err.println(e.getMessage());
      formatter.printHelp("SpannerDataLoader", options);
      System.exit(1);
      return null; // Will not be reached
    }
  }
}

