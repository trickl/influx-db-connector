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

@Measurement(name = "aggregated_sports_event_match_time_update")
@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class AggregatedSportsEventMatchTimeUpdateEntity {
  @NotNull
  @Column(name = "time", timestamp = true)
  private Instant time;

  @NotNull
  @Column(name = "instrumentId", tag = true)
  private String instrumentId;

  @NotNull
  @Column(name = "exchangeId", tag = true)
  private String exchangeId;

  @Column(name = "firstTime", timestamp = true)
  private String firstTime;

  @Column(name = "firstMatchTime")
  private String firstMatchTime;

  @Column(name = "firstRemainingTime")
  private String firstRemainingTime;

  @Column(name = "firstRemainingTimeInPeriod")
  private String firstRemainingTimeInPeriod;

  @Column(name = "lastTime", timestamp = true)
  private String lastTime;

  @Column(name = "lastMatchTime")
  private String lastMatchTime;

  @Column(name = "lastRemainingTime")
  private String lastRemainingTime;

  @Column(name = "lastRemainingTimeInPeriod")
  private String lastRemainingTimeInPeriod;

  @NotNull
  @Column(name = "count")
  private Long count;
}
