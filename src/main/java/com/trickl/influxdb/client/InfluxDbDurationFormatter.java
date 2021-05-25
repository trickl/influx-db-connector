package com.trickl.influxdb.client;

import java.text.MessageFormat;
import java.time.Duration;

public class InfluxDbDurationFormatter {
  /**
   * Format a duration as a InfluxDb compatible string.
   * @param duration Duration
   * @return A formatted duration
   */
  public static String format(Duration duration) {
    return duration.compareTo(Duration.ofMinutes(1)) == -1 
        ? MessageFormat.format("{0}s", duration.toSeconds())
        : MessageFormat.format("{0}m", duration.toMinutes());
  }
}
