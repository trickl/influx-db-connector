package com.trickl.influxdb.persistence;

import java.math.BigDecimal;
import java.time.Instant;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.Value;
import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;

@Value
@Measurement(name = "ohlvc_bar")
public class OhlcvBarEntity {
  @NotNull
  @Column(name = "time")
  private Instant time;  
    
  @Min(0)
  @NotNull
  @Column(name = "open")
  private BigDecimal open;

  @Min(0)
  @NotNull
  @Column(name = "high")
  private BigDecimal high;

  @Min(0)
  @NotNull
  @Column(name = "low")
  private BigDecimal low;

  @Min(0)
  @NotNull
  @Column(name = "close")
  private BigDecimal close;

  @Column(name = "volume")
  private Long volume;
}
