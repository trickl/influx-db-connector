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

@Measurement(name = "sports_event_score_update")
@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class SportsEventScoreUpdateEntity {
  @NotNull
  @Column(name = "time", timestamp = true)
  private Instant time;

  @NotNull
  @Column(name = "instrumentId", tag = true)
  private String instrumentId;

  @NotNull
  @Column(name = "exchangeId", tag = true)
  private String exchangeId;

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

  @Column(name = "game")
  private String game;

  @Column(name = "setOne")
  private String setOne;

  @Column(name = "setTwo")
  private String setTwo;

  @Column(name = "setThree")
  private String setThree;

  @Column(name = "setFour")
  private String setFour;

  @Column(name = "setFive")
  private String setFive;

  @Column(name = "quarterOne")
  private String quarterOne;

  @Column(name = "quarterTwo")
  private String quarterTwo;

  @Column(name = "quarterThree")
  private String quarterThree;

  @Column(name = "quarterFour")
  private String quarterFour;

  @Column(name = "sets")
  private String sets;

  @Column(name = "tieBreakOne")
  private String tieBreakOne;

  @Column(name = "tieBreakTwo")
  private String tieBreakTwo;
}
