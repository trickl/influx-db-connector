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

@Measurement(name = "sports_event_period_update")
@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class SportsEventPeriodUpdateEntity {
  @NotNull
  @Column(name = "time", timestamp = true)
  private Instant time;

  @NotNull
  @Column(name = "instrumentId", tag = true)
  private String instrumentId;

  @NotNull
  @Column(name = "exchangeId", tag = true)
  private String exchangeId;

  @Column(name = "period")
  private String period;
}

