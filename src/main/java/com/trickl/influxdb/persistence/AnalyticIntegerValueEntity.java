package com.trickl.influxdb.persistence;

import com.influxdb.annotations.Measurement;
import java.time.Instant;
import lombok.Builder;
import lombok.NoArgsConstructor;

@Measurement(name = "analytic_integer_value")
@NoArgsConstructor
public class AnalyticIntegerValueEntity extends AnalyticPrimitiveValueEntity<Integer> {
  @Builder
  public AnalyticIntegerValueEntity(
      Instant time,
      String instrumentId,
      String exchangeId,
      String temporalSource,
      String domain,
      String analyticName,
      String parameters,
      int value) {
    super(time, instrumentId, exchangeId, temporalSource, domain, analyticName, parameters, value);
  }
}
