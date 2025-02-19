package com.example.SnowpipeRest.snowflake;

import com.example.SnowpipeRest.utils.Utils;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.ParameterProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Properties;

/**
 * Snowpipe Streaming Client Manager. May return unique Clients or may share a Client. For now this
 * simply creates one Client per destination table as to not interleave.
 */
@Component
public class ClientManager {

  @Value("${snowflake.url}")
  private String snowflakeUrl;

  @Value("${snowflake.user}")
  private String snowflakeUser;

  @Value("${snowflake.role}")
  private String snowflakeRole;

  @Value("${snowflake.private_key}")
  private String snowflakePrivateKey;

  @Value("${rest_api.max_client_lag}")
  private long MAX_CLIENT_LAG;

  // Shared Client instance across all tables. We have relatively few tables hence one Client.
  private final SnowflakeStreamingIngestClient client;

  /** Initializes a Client manager backed by a single Snowpipe Streaming Client instance */
  public ClientManager() {
    this.client = buildSingletonClientInstance();
  }

  /**
   * Returns the Client instance (currently a singleton)
   *
   * @return
   */
  public SnowflakeStreamingIngestClient getClient() {
    return client;
  }

  private SnowflakeStreamingIngestClient buildSingletonClientInstance() {
    java.util.Properties props = new Properties();
    props.put("url", snowflakeUrl);
    props.put("user", snowflakeUser);
    props.put("role", snowflakeRole);
    props.put("private_key", snowflakePrivateKey);
    if (this.MAX_CLIENT_LAG > 0) props.put(ParameterProvider.MAX_CLIENT_LAG, this.MAX_CLIENT_LAG);
    try {
      return SnowflakeStreamingIngestClientFactory.builder("REST_" + Utils.getHostName())
          .setProperties(props)
          .build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
