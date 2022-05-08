package com.trickl.influxdb.persistence;

import com.influxdb.annotations.Measurement;
import java.time.Instant;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NoArgsConstructor;

@Measurement(name = "analytic_boolean_value")
@Builder
@NoArgsConstructor
public class AnalyticBooleanValueEntity extends AnalyticPrimitiveValueEntity<Boolean> {
  @Builder
  public AnalyticBooleanValueEntity(
      Instant time,
      String instrumentId,
      String exchangeId,
      String temporalSource,
      String domain,
      String analyticName,
      String parameters,
      boolean value) {
    super(time, instrumentId, exchangeId, temporalSource, domain, analyticName, parameters, value);
  }
}
