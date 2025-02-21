package com.example.SnowpipeRest.utils;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class IngestEngineConfig {
  // The maximum row count that any buffer should have
  @Value("${rest_api.buffer_manager_max_buffer_row_count}")
  private int maxBufferRowCount;

  @Value("${rest_api.drain_manager_num_threads}")
  private int numThreads;

  @Value("${rest_api.drain_manager_max_duration_to_drain_ms}")
  private int maxDurationToDrainMs;

  @Value("${rest_api.drain_manager_max_records_to_drain}")
  private int maxRecordsToDrain;

  // Default constructor. Fuck it, hardwire values for now because Springboot's config is a hot mess
  public IngestEngineConfig() {
    numThreads = 15;
    maxBufferRowCount = 100_000;
    maxRecordsToDrain = 10_000;
    maxDurationToDrainMs = 3600000;
  }

  public int getMaxBufferRowCount() {
    return maxBufferRowCount;
  }

  public int getNumThreads() {
    return numThreads;
  }

  public int getMaxDurationToDrainMs() {
    return maxDurationToDrainMs;
  }

  public int getMaxRecordsToDrain() {
    return maxRecordsToDrain;
  }
}
