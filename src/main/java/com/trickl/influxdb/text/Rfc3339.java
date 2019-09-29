package com.trickl.influxdb.text;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class Rfc3339 {

  public static final String WITH_NANO_PRECISION = "yyyy-MM-dd'T'HH:mm:ss.nnnnnnnnnX";
  public static final String WITH_MILLISECOND_PRECISION = "yyyy-MM-dd'T'HH:mm:ss.nnnX";
  public static final String WITH_SECOND_PRECISION = "yyyy-MM-dd'T'HH:mm:ssX";
  public static final DateTimeFormatter YMDHMSN_FORMATTER =
      DateTimeFormatter.ofPattern(WITH_NANO_PRECISION).withZone(ZoneId.of("UTC"));
  public static final DateTimeFormatter YMDHMS_FORMATTER =
      DateTimeFormatter.ofPattern(WITH_SECOND_PRECISION).withZone(ZoneId.of("UTC"));
  public static final Instant YEAR_OF_2264 = Instant.ofEpochMilli(9223368436000L);
  public static final DateTimeFormatter YMDHMSM_FORMATTER =
      DateTimeFormatter.ofPattern(WITH_MILLISECOND_PRECISION).withZone(ZoneId.of("UTC"));
  public static final Instant YEAR_OF_1970 = Instant.ofEpochMilli(1);

  private Rfc3339() {}
}
