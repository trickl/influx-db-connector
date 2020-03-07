package com.trickl.influxdb.persistence;

import java.time.Instant;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;
import org.influxdb.annotation.TimeColumn;

@Measurement(name = "sports_event_incident")
@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class SportsEventIncidentEntity {
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

  @Column(name = "matchTime")
  private String matchTime;

  @Column(name = "incidentType")
  private String incidentType;

  @Column(name = "period")
  private String period;

  @Column(name = "side")
  private String side;
}
