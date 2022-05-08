package com.trickl.influxdb.persistence;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import java.time.Instant;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Measurement(name = "analytic_double_value")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper = true)
public class AnalyticDoubleValueEntity extends AnalyticPrimitiveValueEntity {
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
    super(time, instrumentId, exchangeId, temporalSource, domain, analyticName, parameters);
    this.value = value;
  }

  @Column(name = "value")
  private Double value;
}
