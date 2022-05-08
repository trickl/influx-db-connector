package com.trickl.influxdb.persistence;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import java.time.Instant;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Measurement(name = "analytic_string_value")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper = true)
public class AnalyticStringValueEntity extends AnalyticPrimitiveValueEntity {
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
    super(time, instrumentId, exchangeId, temporalSource, domain, analyticName, parameters);
    this.value = value;
  }

  @Column(name = "value")
  private String value;
}
