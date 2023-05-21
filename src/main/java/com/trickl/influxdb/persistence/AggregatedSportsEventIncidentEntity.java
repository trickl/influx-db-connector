package com.trickl.influxdb.persistence;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Measurement(name = "aggregated_sports_event_incident")
@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class AggregatedSportsEventIncidentEntity {
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

  @Column(name = "firstPeriod")
  private String firstPeriod;

  @Column(name = "firstIncidentType")
  private String firstIncidentType;

  @Column(name = "firstSide")
  private String firstSide;

  @Column(name = "lastTime", timestamp = true)
  private String lastTime;

  @Column(name = "lastMatchTime")
  private String lastMatchTime;

  @Column(name = "lastPeriod")
  private String lastPeriod;

  @Column(name = "lastIncidentType")
  private String lastIncidentType;

  @Column(name = "lastSide")
  private String lastSide;

  @NotNull
  @Column(name = "count")
  private Long count;
}
