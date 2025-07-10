package org.example;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.AsyncRunner;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.JsonObject;
import com.google.spanner.v1.TransactionOptions.IsolationLevel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;

/**
 * A throughput benchmark tool for Cloud Spanner.
 *
 * <p>This application runs various database operations (e.g., creating nodes/edges, updating
 * data, and querying subgraphs) in a highly concurrent manner to measure the performance of a
 * Spanner database. It uses an asynchronous API with a semaphore to control the number of in-flight
 * operations.
 *
 * <p>Command-line arguments allow customization of the target database, test mode, and performance
 * parameters like the number of operations and concurrency level.
 */
public class ThroughputRunner {

  // Prefixes for deterministic key generation.
  private static final String KEY_PREFIX = "deterministic-key-";
  private static final String NEW_KEY_PREFIX = "new-maybe-key-";
  private static final int EDGE_KEY_OFFSET = 250000;

  /**
   * Creates a pre-defined JSON object of approximately 500 bytes to be used as data in database
   * operations.
   *
   * @return A {@link JsonObject} containing sample data.
   */
  public static JsonObject create500ByteJsonObject() {
    JsonObject dataObject = new JsonObject();
    dataObject.addProperty("transactionId", "a1b2c3d4-e5f6-7890-1234-567890abcdef");
    dataObject.addProperty("timestamp", "2025-06-23T16:27:24Z");
    dataObject.addProperty("status", "SUCCESS");
    dataObject.addProperty("sourceSystem", "WebApp-Primary");
    dataObject.addProperty("isValidated", true);
    dataObject.addProperty("retryAttempts", 3);
    dataObject.addProperty("processingTimeMs", 125);

    JsonObject userDetails = new JsonObject();
    userDetails.addProperty("userId", "user-98765");
    userDetails.addProperty("username", "jane_doe_extra_long_username_for_padding");
    userDetails.addProperty("emailVerified", true);
    dataObject.add("userDetails", userDetails);

    String description =
        "This is a sample description designed to add more weight to the JSON object, "
            + "reaching closer to the 500-byte target. More text content helps in bloating "
            + "the size efficiently without complex structures.";
    dataObject.addProperty("description", description);

    return dataObject;
  }

  public static void main(String[] args) throws InterruptedException {
    // 1. Set up command-line options.
    org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();
    options.addRequiredOption("p", "project", true, "Your Google Cloud project ID.");
    options.addRequiredOption("i", "instance", true, "The Spanner instance ID.");
    options.addRequiredOption("d", "database", true, "The Spanner database ID.");
    options.addOption("m", "mode", true,
        "Test mode: findOrCreateNode, findOrCreateEdge, updateNode, updateEdge, findSubGraph");
    options.addOption(null, "numRuns", true, "Total number of operations to execute.");
    options.addOption(null, "numThreads", true, "Number of threads in the thread pool.");
    options.addOption(null, "keyStartOffset", true, "Starting offset for numeric keys.");
    options.addOption(null, "numTotalKeys", true, "Total number of keys for edge creation.");

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.err.println(e.getMessage());
      formatter.printHelp("ThroughputRunner", options);
      System.exit(1);
      return;
    }

    // 2. Parse command-line arguments with defaults.
    String project = cmd.getOptionValue("project");
    String instance = cmd.getOptionValue("instance");
    String database = cmd.getOptionValue("database");
    String mode = cmd.getOptionValue("mode", "findOrCreateNode");
    final int numRuns = Integer.parseInt(cmd.getOptionValue("numRuns", "10000000"));
    final int numThreads = Integer.parseInt(cmd.getOptionValue("numThreads", "2048"));
    final int keyStartOffset = Integer.parseInt(cmd.getOptionValue("keyStartOffset", "0"));
    final int maxInFlightOperations = numThreads * 20; // Heuristic for semaphore limit
    final int numTotalKeys = Integer.parseInt(cmd.getOptionValue("numTotalKeys", String.valueOf(numRuns)));


    // 3. Initialize concurrency controls and Spanner client.
    Semaphore semaphore = new Semaphore(maxInFlightOperations);
    AtomicInteger completedCount = new AtomicInteger(0);
    AtomicInteger failureCount = new AtomicInteger(0);
    Random random = new Random();

    final int numChannels = Runtime.getRuntime().availableProcessors();
    System.out.printf("Optimizing Spanner client with %d gRPC channels.%n", numChannels);

    SpannerOptions spannerOptions =
        SpannerOptions.newBuilder().setProjectId(project).setNumChannels(numChannels).build();
    Spanner spanner = spannerOptions.getService();
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

    try {
      DatabaseClient dbClient =
          spanner.getDatabaseClient(DatabaseId.of(spannerOptions.getProjectId(), instance, database));

      System.out.printf("Starting test in '%s' mode with %,d operations...%n", mode, numRuns);
      System.out.printf(
          "Configuration: Threads=%d, Max In-Flight=%d, Key Offset=%d, Total Keys=%d%n",
          numThreads, maxInFlightOperations, keyStartOffset, numTotalKeys);

      JsonObject hardcodedDetails = create500ByteJsonObject();
      long startTime = System.currentTimeMillis();

      // Create a list of indices and shuffle it to ensure random + non-repeating execution.
      List<Integer> shuffledIndices = IntStream.range(0, numRuns).boxed().collect(Collectors.toList());
      System.out.println("Shuffling execution order...");
      Collections.shuffle(shuffledIndices);
      System.out.println("Shuffle complete. Starting main loop.");

      // 4. Main test loop: dispatch operations based on the selected mode.
      for (int currentIndex : shuffledIndices) {
        semaphore.acquire();
        // Use a wildcard to accept CompletableFutures of any type that extends ApiFuture.
        CompletableFuture<? extends ApiFuture<?>> dispatchFuture;

        switch (mode) {
          case "findOrCreateNode": {
            String key;
            if (random.nextDouble() < 0.30) {
              // 30% of the time, generate a new key that won't be found.
              key = NEW_KEY_PREFIX + (keyStartOffset + currentIndex);
            } else {
              // 70% of the time, use a key that should already exist.
              key = KEY_PREFIX + (keyStartOffset + currentIndex);
            }
            String label = "label-" + ((keyStartOffset + currentIndex) % 100);
            dispatchFuture = findOrCreateNodeAsync(
                dbClient, label, key, hardcodedDetails, 0, executorService);
            break;
          }
          case "findOrCreateEdge": {
            String key1, key2, label1, label2, edgeLabel;
            if (random.nextDouble() < 0.30) {
              // 30% of the time, create a new edge between two existing, pre-loaded nodes.
              // The edge itself is new because of the prefixed edge label.
              int sourceIndex = random.nextInt(numTotalKeys);
              int destIndex;
              do {
                destIndex = random.nextInt(numTotalKeys);
              } while (sourceIndex == destIndex); // Avoid self-loops

              key1 = KEY_PREFIX + (keyStartOffset + sourceIndex);
              label1 = "label-" + ((keyStartOffset + sourceIndex) % 100);
              key2 = KEY_PREFIX + (keyStartOffset + destIndex);
              label2 = "label-" + ((keyStartOffset + destIndex) % 100);
              edgeLabel = "new-edgelabel-" + (currentIndex % 100); // Use a new prefix to force creation

            } else {
              // 70% of the time, find an edge that should already exist.
              key1 = KEY_PREFIX + (keyStartOffset + currentIndex);
              label1 = "label-" + ((keyStartOffset + currentIndex) % 100);
              key2 = KEY_PREFIX + (keyStartOffset + currentIndex + EDGE_KEY_OFFSET);
              label2 = "label-" + ((keyStartOffset + currentIndex + EDGE_KEY_OFFSET) % 100);
              edgeLabel = "edgelabel-" + (currentIndex % 100);
            }
            dispatchFuture = findOrCreateEdgeAsync(dbClient, label1, key1, edgeLabel, key2, label2,
                hardcodedDetails, 0, executorService);
            break;
          }
          case "updateNode": {
            String key = KEY_PREFIX + (keyStartOffset + currentIndex);
            dispatchFuture = singleUpdateNodeWithMutationAsyncCaller(
                dbClient, hardcodedDetails, key, 0, executorService);
            break;
          }
          case "updateEdge": {
            String key1 = KEY_PREFIX + (keyStartOffset + currentIndex);
            String label1 = "label-" + ((keyStartOffset + currentIndex) % 100);
            String key2 = KEY_PREFIX + (keyStartOffset + currentIndex + EDGE_KEY_OFFSET);
            String label2 = "label-" + ((keyStartOffset + currentIndex + EDGE_KEY_OFFSET) % 100);
            String edgeLabel = "edgelabel-" + (currentIndex % 100);
            dispatchFuture = singleUpdateEdgeWithMutationAsyncCaller(dbClient, hardcodedDetails, label1,
                key1, edgeLabel, label2, key2, 0, executorService);
            break;
          }
          case "findSubGraph": {
            String key = KEY_PREFIX + (keyStartOffset + currentIndex);
            String label = "label-" + ((keyStartOffset + currentIndex) % 100);
            dispatchFuture = findSubGraphAsyncCaller(dbClient, label, key, executorService);
            break;
          }
          default:
            // Release permit before throwing to avoid deadlock.
            semaphore.release();
            throw new IllegalArgumentException("Unknown mode: " + mode);
        }

        // Correctly chain the CompletableFuture with the inner ApiFuture.
        // This ensures handleCompletion is only called after the Spanner transaction is complete.
        CompletableFuture<?> finalFuture = dispatchFuture.thenCompose(
            apiFuture -> toCompletableFuture(apiFuture, executorService));

        finalFuture.whenComplete(
            (result, ex) -> handleCompletion(
                1, ex, completedCount, failureCount, semaphore, numRuns, startTime));
      }

      // 5. Wait for all operations to finish and report results.
      System.out.println("All requests submitted. Waiting for final operations to complete...");
      while (completedCount.get() < numRuns) {
        Thread.sleep(100);
      }

      long endTime = System.currentTimeMillis();
      double durationInSeconds = (endTime - startTime) / 1000.0;
      double throughput = numRuns / durationInSeconds;

      System.out.println("\n-------------------------------------------------");
      System.out.println("Test Complete");
      System.out.printf("Mode: %s\n", mode);
      System.out.printf("Total Operations: %,d\n", numRuns);
      System.out.printf("Successful: %,d\n", numRuns - failureCount.get());
      System.out.printf("Failed: %,d\n", failureCount.get());
      System.out.printf("Total Time: %.2f seconds\n", durationInSeconds);
      System.out.printf("Throughput: %.2f operations/second\n", throughput);
      System.out.println("-------------------------------------------------");

    } finally {
      spanner.close();
      executorService.shutdown();
    }
  }

  /**
   * Handles the completion of an asynchronous database operation.
   *
   * <p>It logs failures, updates completion counts, and releases the semaphore permit.
   *
   * @param batchSize The number of operations in the completed batch.
   * @param ex The exception, if the operation failed; otherwise, null.
   * @param completedCount An atomic counter for successfully completed operations.
   * @param failureCount An atomic counter for failed operations.
   * @param semaphore The semaphore controlling the number of in-flight operations.
   * @param numRuns The total number of operations in the test run.
   */
  private static void handleCompletion(
      int batchSize,
      Throwable ex,
      AtomicInteger completedCount,
      AtomicInteger failureCount,
      Semaphore semaphore,
      final int numRuns,
      long startTime) {
    try {
      if (ex != null) {
        failureCount.addAndGet(batchSize);
        // Limit printing of errors to avoid flooding the console.
        if (failureCount.get() % 100 == 0) {
          System.err.printf("An operation failed: %s%n", ex.getMessage());
        }
      }
      int currentCompleted = completedCount.addAndGet(batchSize);

      // Log progress periodically.
      if (currentCompleted % 10000 == 0 || currentCompleted == numRuns) {
        long endTime = System.currentTimeMillis();
        double durationInSeconds = (endTime - startTime) / 1000.0;
        double throughput = currentCompleted / durationInSeconds;
        System.out.printf(
                "Completed %,d / %,d ops | Throughput: %.2f ops/sec\n",
                currentCompleted, numRuns, throughput);
      }
    } finally {
      semaphore.release(batchSize);
    }
  }

  /**
   * Converts a Guava {@link ApiFuture} to a standard Java {@link CompletableFuture}.
   *
   * <p>This allows for better interoperability in modern asynchronous Java code and is key to
   * handling the results of Spanner operations correctly.
   *
   * @param apiFuture The Guava ApiFuture to convert.
   * @param executor The executor to run the callback on.
   * @param <T> The type of the future's result.
   * @return A {@link CompletableFuture} that completes only when the ApiFuture completes.
   */
  private static <T> CompletableFuture<T> toCompletableFuture(
      ApiFuture<T> apiFuture, ExecutorService executor) {
    CompletableFuture<T> completableFuture = new CompletableFuture<T>() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        boolean cancelled = super.cancel(mayInterruptIfRunning);
        apiFuture.cancel(mayInterruptIfRunning);
        return cancelled;
      }
    };

    ApiFutures.addCallback(
        apiFuture,
        new ApiFutureCallback<T>() {
          @Override
          public void onSuccess(T result) {
            completableFuture.complete(result);
          }

          @Override
          public void onFailure(Throwable t) {
            completableFuture.completeExceptionally(t);
          }
        },
        executor);

    return completableFuture;
  }

  /**
   * Submits a {@link #findSubGraphAsync} task to the executor service.
   */
  private static CompletableFuture<ApiFuture<Void>> findSubGraphAsyncCaller(
      DatabaseClient dbClient, String label, String key, ExecutorService executor) {
    return CompletableFuture.supplyAsync(() -> findSubGraphAsync(dbClient, label, key), executor);
  }

  /**
   * Asynchronously executes a graph query to find the neighbors of a given node.
   *
   * @return An {@link ApiFuture} that completes when the query finishes.
   */
  private static ApiFuture<Void> findSubGraphAsync(
      DatabaseClient dbClient, String label, String key) {
    String graphQuery =
        "GRAPH AFGraphSchemaless "
            + "MATCH (n {label:@label, key:@key}) -[e:Forward|Reverse]-> (m) "
            + "RETURN n.details AS n_details, e.edge_label AS e_label, e.details AS deets, "
            + "m.label AS m_label, m.key AS m_key, m.details AS m_details";

    Statement statement =
        Statement.newBuilder(graphQuery).bind("label").to(label).bind("key").to(key).build();

    AsyncRunner runner = dbClient.runAsync(
        Options.tag("app=ink-graph-poc,service=findSubGraph"),
        Options.isolationLevel(IsolationLevel.REPEATABLE_READ));

    return runner.runAsync(
        txn -> {
          try (com.google.cloud.spanner.ResultSet resultSet = txn.executeQuery(statement)) {
            // Iterate through the entire result set to account for data download time.
            while (resultSet.next()) {
              // Row processing could be done here.
            }
            return ApiFutures.immediateFuture(null);
          } catch (Exception e) {
            System.err.printf(
                "Error executing graph query for key: '%s'. Exception: %s%n",
                key, e.getMessage());
            throw new RuntimeException("Failed to execute graph query for key: " + key, e);
          }
        },
        MoreExecutors.directExecutor());
  }

  /**
   * Submits a {@link #nodeAsyncRunner} task to the executor service.
   */
  private static CompletableFuture<ApiFuture<Long>> findOrCreateNodeAsync(
      DatabaseClient databaseClient,
      String label,
      String key,
      JsonObject details,
      int maxCommitDelayMillis,
      ExecutorService executor) {
    return CompletableFuture.supplyAsync(
        () -> nodeAsyncRunner(databaseClient, label, key, details, maxCommitDelayMillis), executor);
  }

  /**
   * Asynchronously finds a node or creates it if it doesn't exist using a read-write transaction.
   *
   * @return An {@link ApiFuture} containing the number of rows created (0 or 1).
   */
  private static ApiFuture<Long> nodeAsyncRunner(
      DatabaseClient databaseClient, String label, String key, JsonObject details,
      int maxCommitDelayMillis) {
    AsyncRunner runner =
        databaseClient.runAsync(Options.tag("app=ink-graph-poc,service=findOrCreateNode"));

    return runner.runAsync(
        txn -> {
          // First, read to see if the node already exists.
          ApiFuture<Struct> existingNodeFut =
              txn.readRowAsync("GraphNode", Key.of(label, key),
                       ImmutableList.of("details", "first_seen", "last_seen", "creation_ts", "update_ts"));

          return ApiFutures.transformAsync(
              existingNodeFut,
              (existingNode) -> {
                // If the node exists, do nothing and return 0.
                if (existingNode != null) {
                  return ApiFutures.immediateFuture(0L);
                }

                // If the node does not exist, create and buffer an insert mutation.
                details.addProperty("upd_count", 0);
                Timestamp currentTime = Timestamp.now();
                Mutation insertMutation =
                    Mutation.newInsertBuilder("GraphNode")
                        .set("label").to(label)
                        .set("key").to(key)
                        .set("details").to(Value.json(details.toString()))
                        .set("first_seen").to(currentTime)
                        .set("last_seen").to(currentTime)
                        .set("creation_ts").to(currentTime)
                        .set("update_ts").to(currentTime)
                        .build();

                txn.buffer(insertMutation);
                return ApiFutures.immediateFuture(1L);
              },
              MoreExecutors.directExecutor());
        },
        MoreExecutors.directExecutor());
  }

  /**
   * Submits an {@link #edgeAsyncRunner} task to the executor service.
   */
  private static CompletableFuture<ApiFuture<Long>> findOrCreateEdgeAsync(
      DatabaseClient databaseClient,
      String label,
      String key,
      String edge_label,
      String key2,
      String label2,
      JsonObject details,
      int maxCommitDelayMillis,
      ExecutorService executor) {
    return CompletableFuture.supplyAsync(
        () -> edgeAsyncRunner(
            databaseClient, label, key, edge_label, key2, label2, details, maxCommitDelayMillis),
        executor);
  }

  /**
   * Asynchronously finds an edge or creates it if it doesn't exist using a read-write transaction.
   *
   * @return An {@link ApiFuture} containing the number of rows created (0 or 1).
   */
  private static ApiFuture<Long> edgeAsyncRunner(
      DatabaseClient databaseClient,
      String label,
      String key,
      String edge_label,
      String key2,
      String label2,
      JsonObject details,
      int maxCommitDelayMillis) {
    AsyncRunner runner =
        databaseClient.runAsync(Options.tag("app=ink-graph-poc,service=findOrCreateEdge"));

    return runner.runAsync(
        txn -> {
          // Check if the edge already exists by reading its primary key.
          ApiFuture<Struct> existingEdgeFut = txn.readRowAsync(
              "GraphEdge", Key.of(label, key, edge_label, label2, key2),
                         ImmutableList.of("details", "first_seen", "last_seen",
                             "creation_ts", "update_ts"));

          return ApiFutures.transformAsync(
              existingEdgeFut,
              (existingEdge) -> {
                if (existingEdge != null) {
                  return ApiFutures.immediateFuture(0L); // Edge found, do nothing.
                }

                // Edge not found, build and buffer an insert mutation.
                details.addProperty("upd_count", 0);
                Timestamp currentTime = Timestamp.now();
                Mutation insertMutation =
                    Mutation.newInsertBuilder("GraphEdge")
                        .set("label").to(label)
                        .set("key").to(key)
                        .set("edge_label").to(edge_label)
                        .set("other_node_label").to(label2)
                        .set("other_node_key").to(key2)
                        .set("details").to(Value.json(details.toString()))
                        .set("first_seen").to(currentTime)
                        .set("last_seen").to(currentTime)
                        .set("creation_ts").to(currentTime)
                        .set("update_ts").to(currentTime)
                        .build();

                txn.buffer(insertMutation);
                return ApiFutures.immediateFuture(1L); // One row will be created.
              },
              MoreExecutors.directExecutor());
        },
        MoreExecutors.directExecutor());
  }

  /**
   * Submits a {@link #singleUpdateNodeWithMutationAsync} task to the executor service.
   */
  private static CompletableFuture<ApiFuture<Void>> singleUpdateNodeWithMutationAsyncCaller(
      DatabaseClient dbClient,
      JsonObject detailsTemplate,
      String key,
      int maxCommitDelayMillis,
      ExecutorService executor) {
    return CompletableFuture.supplyAsync(
        () -> singleUpdateNodeWithMutationAsync(dbClient, detailsTemplate, key, maxCommitDelayMillis),
        executor);
  }

  /**
   * Asynchronously updates a single node using a mutation.
   *
   * @return An {@link ApiFuture} that completes when the transaction is committed.
   */
  private static ApiFuture<Void> singleUpdateNodeWithMutationAsync(
      DatabaseClient dbClient, JsonObject detailsTemplate, String key, int maxCommitDelayMillis) {
    AsyncRunner runner = dbClient.runAsync(Options.tag("app=ink-graph-poc,service=updateNode"));

    return runner.runAsync(
        txn -> {
          Timestamp currentTime = Timestamp.now();
          int baseUpdCount =
              detailsTemplate.has("upd_count") ? detailsTemplate.get("upd_count").getAsInt() : 0;
          JsonObject details = detailsTemplate.deepCopy();
          details.addProperty("upd_count", baseUpdCount + 1);

          long keyIndex = Long.parseLong(key.substring(KEY_PREFIX.length()));
          String label = "label-" + (keyIndex % 100);

          Mutation mutation =
              Mutation.newUpdateBuilder("GraphNode")
                  .set("label").to(label)
                  .set("key").to(key)
                  .set("details").to(Value.json(details.toString()))
                  .set("last_seen").to(currentTime)
                  .set("update_ts").to(currentTime)
                  .build();

          txn.buffer(mutation);
          return ApiFutures.immediateFuture(null);
        },
        MoreExecutors.directExecutor());
  }

  /**
   * Submits a {@link #singleUpdateEdgeWithMutationAsync} task to the executor service.
   */
  private static CompletableFuture<ApiFuture<Void>> singleUpdateEdgeWithMutationAsyncCaller(
      DatabaseClient dbClient,
      JsonObject detailsTemplate,
      String label,
      String key,
      String edgeLabel,
      String otherLabel,
      String otherKey,
      int maxCommitDelayMillis,
      ExecutorService executor) {
    return CompletableFuture.supplyAsync(
        () -> singleUpdateEdgeWithMutationAsync(dbClient, detailsTemplate, label, key, edgeLabel,
            otherLabel, otherKey, maxCommitDelayMillis),
        executor);
  }

  /**
   * Asynchronously updates a single edge using a mutation.
   *
   * @return An {@link ApiFuture} that completes when the transaction is committed.
   */
  private static ApiFuture<Void> singleUpdateEdgeWithMutationAsync(
      DatabaseClient dbClient,
      JsonObject detailsTemplate,
      String label,
      String key,
      String edgeLabel,
      String otherLabel,
      String otherKey,
      int maxCommitDelayMillis) {
    AsyncRunner runner = dbClient.runAsync(Options.tag("app=ink-graph-poc,service=updateEdge"));

    return runner.runAsync(
        txn -> {
          Timestamp currentTime = Timestamp.now();
          int baseUpdCount =
              detailsTemplate.has("upd_count") ? detailsTemplate.get("upd_count").getAsInt() : 0;
          JsonObject details = detailsTemplate.deepCopy();
          details.addProperty("upd_count", baseUpdCount + 1);

          Mutation mutation =
              Mutation.newUpdateBuilder("GraphEdge")
                  .set("label").to(label)
                  .set("key").to(key)
                  .set("edge_label").to(edgeLabel)
                  .set("other_node_label").to(otherLabel)
                  .set("other_node_key").to(otherKey)
                  .set("details").to(Value.json(details.toString()))
                  .set("last_seen").to(currentTime)
                  .set("update_ts").to(currentTime)
                  .build();

          txn.buffer(mutation);
          return ApiFutures.immediateFuture(null);
        },
        MoreExecutors.directExecutor());
  }
}

