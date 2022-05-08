package com.trickl.influxdb.persistence;

import com.influxdb.annotations.Measurement;
import java.time.Instant;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NoArgsConstructor;

@Measurement(name = "analytic_double_value")
@Builder
@NoArgsConstructor
public class AnalyticDoubleValueEntity extends AnalyticPrimitiveValueEntity<Double> {
  @Builder
  public AnalyticDoubleValueEntity(
      Instant time,
      String instrumentId,
      String exchangeId,
      String temporalSource,
      String domain,
      String analyticName,
      String parameters,
      double value) {
    super(time, instrumentId, exchangeId, temporalSource, domain, analyticName, parameters, value);
  }
}
