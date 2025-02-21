package com.example.SnowpipeRest.rest;

import com.example.SnowpipeRest.buffer.DrainManager;
import com.example.SnowpipeRest.utils.EnqueueResponse;
import com.example.SnowpipeRest.buffer.BufferManager;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Responsible for ingesting data to a Snowflake table from a REST request. The general strategy is:
 *
 * <ul>
 *   <li>Enqueue requests arrive from one or more threads
 *   <li>Each thread enqueues a request in an in-memory buffer
 *   <li>After enqueue, the thread responds back to the Client saying that data has been received,
 *       but not yet committed
 * </ul>
 *
 * - Enqueue requests can come in from one or more threads - Each thread
 */
public class IngestEngine {

  private static final Logger LOGGER = LoggerFactory.getLogger(IngestEngine.class);

  private BufferManager bufferManager;

  private DrainManager drainManager;

  // This is a fun one. Basically the idea is that this gets set once, and we use this as an epoch
  // of the app. This is used to reason about replay and the like later on
  private long epochTs;

  /**
   * Default constructor. Note that this MUST be empty due to how Spring does property to BEAN
   * binding.
   */
  public IngestEngine(
      long maxBufferRowCount, int numThreads, int maxDurationToDrainMs, int maxRecordsToDrain) {

    LOGGER.info("Initializing Ingest Engine...");
    this.bufferManager = new BufferManager(maxBufferRowCount);
    this.epochTs = System.currentTimeMillis();
    this.drainManager =
        new DrainManager(
            epochTs, bufferManager, numThreads, maxDurationToDrainMs, maxRecordsToDrain);
    try (ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor()) {
      executorService.scheduleWithFixedDelay(drainManager, 0, 60, TimeUnit.SECONDS);
    }
  }

  /**
   * Enqueues data to be inserted into a table
   *
   * @param database the destination database
   * @param schema the destination schema
   * @param table the destination table
   * @param requestData the application supplied request body containing one or more rows
   * @return
   */
  public EnqueueResponse enqueueData(
      final String database, final String schema, final String table, final String requestData) {
    return bufferManager.getBuffer(database, schema, table).expandRowsEnqueueData(requestData);
  }
}
