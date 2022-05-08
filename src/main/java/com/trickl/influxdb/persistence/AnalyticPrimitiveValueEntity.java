package com.trickl.influxdb.persistence;

import com.influxdb.annotations.Column;
import java.time.Instant;
import javax.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@ToString
public class AnalyticPrimitiveValueEntity<T> {
  @NotNull
  @Column(name = "time", timestamp = true)
  private Instant time;

  @NotNull
  @Column(name = "instrumentId", tag = true)
  private String instrumentId;

  @NotNull
  @Column(name = "exchangeId", tag = true)
  private String exchangeId;

  @NotNull
  @Column(name = "temporalSource", tag = true)
  private String temporalSource;

  @NotNull
  @Column(name = "domain", tag = true)
  private String domain;

  @NotNull
  @Column(name = "analyticName", tag = true)
  private String analyticName;

  @NotNull
  @Column(name = "parameters", tag = true)
  private String parameters;

  @Column(name = "value")
  private T value;
}
