package com.trickl.influxdb.persistence;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Measurement(name = "ohlvc_bar")
@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class OhlcvBarEntity {
  @NotNull
  @Column(name = "time", timestamp = true)
  private Instant time;

  @NotNull
  @Column(name = "instrumentId", tag = true)
  private String instrumentId;

  @NotNull
  @Column(name = "exchangeId", tag = true)
  private String exchangeId;

  @Min(0)
  @NotNull
  @Column(name = "open")
  private Double open;

  @Min(0)
  @NotNull
  @Column(name = "high")
  private Double high;

  @Min(0)
  @NotNull
  @Column(name = "low")
  private Double low;

  @Min(0)
  @NotNull
  @Column(name = "close")
  private Double close;

  @Column(name = "volume")
  private Long volume;
}
