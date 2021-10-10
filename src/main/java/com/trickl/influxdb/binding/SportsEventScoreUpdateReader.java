package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.SportsEventScoreUpdateEntity;
import com.trickl.model.event.sports.SportsEventScoreUpdate;
import com.trickl.model.event.sports.SportsEventScores;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SportsEventScoreUpdateReader
    implements Function<SportsEventScoreUpdateEntity, SportsEventScoreUpdate> {

  @Override
  public SportsEventScoreUpdate apply(SportsEventScoreUpdateEntity instrumentEventEntity) {
    return SportsEventScoreUpdate.builder()
        .time(instrumentEventEntity.getTime())
        .scores(
            SportsEventScores.builder()
                .current(parseScoreString(instrumentEventEntity.getCurrent()))
                .firstHalf(parseScoreString(instrumentEventEntity.getFirstHalf()))
                .secondHalf(parseScoreString(instrumentEventEntity.getSecondHalf()))
                .normalTime(parseScoreString(instrumentEventEntity.getNormalTime()))
                .fullTime(parseScoreString(instrumentEventEntity.getFullTime()))
                .game(parseScoreString(instrumentEventEntity.getGame()))
                .setOne(parseScoreString(instrumentEventEntity.getSetOne()))
                .setTwo(parseScoreString(instrumentEventEntity.getSetTwo()))
                .setThree(parseScoreString(instrumentEventEntity.getSetThree()))
                .setFour(parseScoreString(instrumentEventEntity.getSetFour()))
                .setFive(parseScoreString(instrumentEventEntity.getSetFive()))
                .quarterOne(parseScoreString(instrumentEventEntity.getQuarterOne()))
                .quarterTwo(parseScoreString(instrumentEventEntity.getQuarterTwo()))
                .quarterThree(parseScoreString(instrumentEventEntity.getQuarterThree()))
                .quarterFour(parseScoreString(instrumentEventEntity.getQuarterFour()))
                .tiebreakOne(parseScoreString(instrumentEventEntity.getTieBreakOne()))
                .tiebreakTwo(parseScoreString(instrumentEventEntity.getTieBreakTwo()))
                .build())
        .build();
  }

  /**
   * Parse a score string into a typed score array.
   * @param scores The score string
   * @return An array of scores
   */
  public static List<Integer> parseScoreString(String scores) {
    if (scores == null) {
      return Collections.emptyList();
    }

    return Arrays.asList(scores.split(",")).stream()
        .map(Integer::parseInt)
        .collect(Collectors.toList());
  }
}
