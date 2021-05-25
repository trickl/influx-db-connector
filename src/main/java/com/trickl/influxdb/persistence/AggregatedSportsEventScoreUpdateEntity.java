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

@Measurement(name = "aggregated_sports_event_score_update")
@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class AggregatedSportsEventScoreUpdateEntity {
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

  @Column(name = "firstCurrent")
  private String firstCurrent;

  @Column(name = "firstFirstHalf")
  private String firstFirstHalf;

  @Column(name = "firstSecondHalf")
  private String firstSecondHalf;

  @Column(name = "firstNormalTime")
  private String firstNormalTime;

  @Column(name = "firstFullTime")
  private String firstFullTime;

  @Column(name = "firstGame")
  private String firstGame;

  @Column(name = "firstSetOne")
  private String firstSetOne;

  @Column(name = "firstSetTwo")
  private String firstSetTwo;

  @Column(name = "firstSets")
  private String firstSets;

  @Column(name = "firstTieBreakOne")
  private String firstTieBreakOne;

  @Column(name = "firstTieBreakTwo")
  private String firstTieBreakTwo;

  @Column(name = "lastTime", timestamp = true)
  private String lastTime;

  @Column(name = "lastCurrent")
  private String lastCurrent;

  @Column(name = "lastFirstHalf")
  private String lastFirstHalf;

  @Column(name = "lastSecondHalf")
  private String lastSecondHalf;

  @Column(name = "lastNormalTime")
  private String lastNormalTime;

  @Column(name = "lastFullTime")
  private String lastFullTime;

  @Column(name = "lastGame")
  private String lastGame;

  @Column(name = "lastSetOne")
  private String lastSetOne;

  @Column(name = "lastSetTwo")
  private String lastSetTwo;

  @Column(name = "lastSets")
  private String lastSets;

  @Column(name = "lastTieBreakOne")
  private String lastTieBreakOne;

  @Column(name = "lastTieBreakTwo")
  private String lastTieBreakTwo;

  @NotNull
  @Column(name = "count")
  private Long count;
}
