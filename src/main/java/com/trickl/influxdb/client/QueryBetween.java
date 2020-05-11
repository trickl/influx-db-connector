package com.trickl.influxdb.client;

import java.time.Instant;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class QueryBetween {
  protected boolean startIncl;
  protected Instant start;
  protected boolean endIncl;
  protected Instant end;
  protected boolean ascending;
  protected Long limit;
  protected Integer chunkSize;
}
