package com.example.SnowpipeRest.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/** Set of utilities used across channels and the application */
public class Utils {

  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

  /**
   * @return host name
   */
  public static String getHostName() {
    try {
      InetAddress localHost = InetAddress.getLocalHost();
      return localHost.getHostName(); // Returns hostname
    } catch (UnknownHostException e) {
      LOGGER.error("Unable to get channel name", e);
      throw new RuntimeException("Unable to get channel name", e);
    }
  }

  /** Gets an offset token for the current epoch */
  public static String getOffsetToken(long offsetCounter, long epochTs) {
    return offsetCounter + "-" + epochTs;
  }

  /** Given a persisted offset token, extract the buffer index */
  public static String getBufferIndexFromOffsetToken(String offsetToken) {
    return offsetToken.split("-")[0];
  }
}
