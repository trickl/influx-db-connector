package com.trickl.influxdb.persistence;

import java.time.Instant;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;
import org.influxdb.annotation.TimeColumn;

@Measurement(name = "market_state_change")
@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class MarketStateChangeEntity {
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

  @NotNull
  @Column(name = "eventId", tag = true)
  private String eventId;

  @NotNull
  @Column(name = "state")
  private String state;

  @NotNull
  @Column(name = "description")
  private String description;
}
