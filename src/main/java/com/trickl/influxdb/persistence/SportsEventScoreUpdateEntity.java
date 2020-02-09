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

@Measurement(name = "sports_event_score_update")
@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class SportsEventScoreUpdateEntity {
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

  @Column(name = "current")
  private String current;

  @Column(name = "firstHalf")  
  private String firstHalf;

  @Column(name = "secondHalf")
  private String secondHalf;

  @Column(name = "normalTime")
  private String normalTime;

  @Column(name = "fullTime")
  private String fullTime;
}
