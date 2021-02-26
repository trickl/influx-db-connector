package com.trickl.influxdb.persistence;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import java.time.Instant;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Measurement(name = "market_state_change")
@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class MarketStateChangeEntity {
  @NotNull
  @Column(name = "time", timestamp = true)
  private Instant time;

  @NotNull
  @Column(name = "instrumentId", tag = true)
  private String instrumentId;

  @NotNull
  @Column(name = "exchangeId", tag = true)
  private String exchangeId;

  @NotNull
  @Column(name = "eventId")
  private String eventId;

  @NotNull
  @Column(name = "state")
  private String state;

  @NotNull
  @Column(name = "description")
  private String description;
}
