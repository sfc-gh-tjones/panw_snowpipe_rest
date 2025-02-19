package com.example.SnowpipeRest.snowflake;

import com.example.SnowpipeRest.utils.TableKey;
import com.example.SnowpipeRest.utils.Utils;
import com.google.common.annotations.VisibleForTesting;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;

import java.util.concurrent.ConcurrentHashMap;

/** Manages Channels across tables */
public class ChannelManager {

  private static ChannelManager INSTANCE;

  public static ChannelManager getInstance() {
    if (INSTANCE == null) {
      // Lazy load this to get around local testing and Spring Boot reflection insanity
      INSTANCE = new ChannelManager(new ClientManager().getClient());
    }
    return INSTANCE;
  }

  @VisibleForTesting
  public static void setInstance(ChannelManager instance) {
    INSTANCE = instance;
  }

  private final ConcurrentHashMap<TableKey, SnowflakeStreamingIngestChannel> cachedChannels;

  private final SnowflakeStreamingIngestClient client;

  public ChannelManager(final SnowflakeStreamingIngestClient client) {
    this.client = client;
    this.cachedChannels = new ConcurrentHashMap<>();
  }

  /** Gets or computes a channel instance. */
  public SnowflakeStreamingIngestChannel getChannelForTable(
      String database, String schema, String table) {
    OpenChannelRequest request =
        OpenChannelRequest.builder(Utils.getHostName())
            .setDBName(database)
            .setSchemaName(schema)
            .setTableName(table)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();
    final TableKey tableKey = new TableKey(database, schema, table);
    return cachedChannels.computeIfAbsent(tableKey, t -> client.openChannel(request));
  }

  /** Invalidates a channel by removing it from the map */
  public void invalidateChannel(String database, String schema, String table) {
    cachedChannels.remove(new TableKey(database, schema, table));
  }
}
