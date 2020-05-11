package com.trickl.influxdb.persistence;

import java.time.Instant;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;
import org.influxdb.annotation.TimeColumn;

@Measurement(name = "order")
@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class OrderEntity {
  @NotNull
  @TimeColumn
  @Column(name = "time")
  private Instant time;

  @NotNull
  @Column(name = "instrumentId", tag = true)
  private String instrumentId;

  @NotNull
  @Column(name = "exchangeId", tag = true)
  private String exchangeId;

  @Column(name = "isBid", tag = true)
  protected boolean isBid;

  @Column(name = "depth", tag = true)
  protected int depth;

  @NotNull
  @Min(0)
  @Column(name = "price")
  protected Double price;

  /** The amount of liquidity. */
  @Min(0)
  @Column(name = "volume")
  protected Long volume;
}
