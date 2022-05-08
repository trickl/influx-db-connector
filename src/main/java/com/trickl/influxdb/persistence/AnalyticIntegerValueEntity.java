package com.trickl.influxdb.persistence;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import java.time.Instant;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Measurement(name = "analytic_integer_value")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper = true)
public class AnalyticIntegerValueEntity extends AnalyticPrimitiveValueEntity {
  /**
   * Build a new analyic integer value entity.
   *
   * @param time time
   * @param instrumentId instrumentId
   * @param exchangeId exchangeId
   * @param temporalSource sim or live
   * @param domain analytic domain
   * @param analyticName name
   * @param parameters analytic parameters
   * @param value the integer value
   */
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
    super(time, instrumentId, exchangeId, temporalSource, domain, analyticName, parameters);
    this.value = (long) value;
  }

  @Column(name = "value")
  private Long value;
}
