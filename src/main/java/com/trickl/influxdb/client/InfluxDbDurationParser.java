package com.trickl.influxdb.client;

import java.time.Duration;
import java.time.format.DateTimeParseException;

public class InfluxDbDurationParser {
  /**
   * Try parsing a duration string.
   *
   * @param text duration text
   * @return A duration period
   */
  public static Duration tryParse(String text) {
    try {
      return Duration.parse("PT" + text.toUpperCase());
    } catch (DateTimeParseException ex) {
      return null;
    }
  }
}
