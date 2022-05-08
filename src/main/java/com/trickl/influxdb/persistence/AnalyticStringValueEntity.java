package com.trickl.influxdb.persistence;

import com.influxdb.annotations.Measurement;
import java.time.Instant;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NoArgsConstructor;

@Measurement(name = "analytic_string_value")
@Builder
@NoArgsConstructor
public class AnalyticStringValueEntity extends AnalyticPrimitiveValueEntity<String> {
  @Builder
  public AnalyticStringValueEntity(
      Instant time,
      String instrumentId,
      String exchangeId,
      String temporalSource,
      String domain,
      String analyticName,
      String parameters,
      String value) {
    super(time, instrumentId, exchangeId, temporalSource, domain, analyticName, parameters, value);
  }
}
