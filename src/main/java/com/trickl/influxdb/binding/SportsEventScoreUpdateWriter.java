package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.SportsEventScoreUpdateEntity;
import com.trickl.model.event.sports.SportsEventScoreUpdate;
import com.trickl.model.event.sports.SportsEventScores;
import com.trickl.model.pricing.primitives.PriceSource;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SportsEventScoreUpdateWriter
    implements Function<SportsEventScoreUpdate, SportsEventScoreUpdateEntity> {

  private final PriceSource priceSource;

  @Override
  public SportsEventScoreUpdateEntity apply(SportsEventScoreUpdate instrumentEvent) {
    SportsEventScores scores = instrumentEvent.getScores();
    return SportsEventScoreUpdateEntity.builder()
        .instrumentId(priceSource.getInstrumentId().toUpperCase())
        .exchangeId(priceSource.getExchangeId().toUpperCase())
        .time(instrumentEvent.getTime())
        .current(toScoreString(scores.getCurrent()))
        .firstHalf(toScoreString(scores.getFirstHalf()))
        .secondHalf(toScoreString(scores.getSecondHalf()))
        .normalTime(toScoreString(scores.getNormalTime()))
        .fullTime(toScoreString(scores.getFullTime()))
        .game(toScoreString(scores.getGame()))
        .setOne(toScoreString(scores.getSetOne()))
        .setTwo(toScoreString(scores.getSetTwo()))
        .setThree(toScoreString(scores.getSetThree()))
        .setFour(toScoreString(scores.getSetFour()))
        .setFive(toScoreString(scores.getSetFive()))
        .quarterOne(toScoreString(scores.getQuarterOne()))
        .quarterTwo(toScoreString(scores.getQuarterTwo()))
        .quarterThree(toScoreString(scores.getQuarterThree()))
        .quarterFour(toScoreString(scores.getQuarterFour()))
        .tieBreakOne(toScoreString(scores.getTiebreakOne()))
        .tieBreakTwo(toScoreString(scores.getTiebreakTwo()))
        .build();
  }

  protected String toScoreString(List<Integer> scores) {
    if (scores == null) {
      return null;
    }
    return scores.stream().map(Object::toString).collect(Collectors.joining(","));
  }
}
