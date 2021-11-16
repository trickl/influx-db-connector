package com.trickl.influxdb.persistence;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import java.time.Instant;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Measurement(name = "analytic_double_value")
@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class AnalyticDoubleValueEntity {
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
  private Double value;
}

