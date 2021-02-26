package com.trickl.influxdb.persistence;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import java.time.Instant;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Measurement(name = "order")
@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class OrderEntity {
  @NotNull
  @Column(name = "time", timestamp = true)
  private Instant time;

  @NotNull
  @Column(name = "instrumentId", tag = true)
  private String instrumentId;

  @NotNull
  @Column(name = "exchangeId", tag = true)
  private String exchangeId;

  @Column(name = "bidOrAsk", tag = true)
  protected String bidOrAsk;

  @Column(name = "depth", tag = true)
  protected String depth;

  @NotNull
  @Min(0)
  @Column(name = "price")
  protected Double price;

  /** The amount of liquidity. */
  @Min(0)
  @Column(name = "volume")
  protected Long volume;
}
