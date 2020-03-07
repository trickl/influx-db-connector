package com.trickl.influxdb.transformers;

import com.trickl.influxdb.persistence.SportsEventScoreUpdateEntity;
import com.trickl.model.event.sports.SportsEventScoreUpdate;
import com.trickl.model.event.sports.SportsEventScores;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SportsEventScoreUpdateReader
    implements Function<SportsEventScoreUpdateEntity, SportsEventScoreUpdate> {

  @Override
  public SportsEventScoreUpdate apply(SportsEventScoreUpdateEntity instrumentEventEntity) {
    return SportsEventScoreUpdate.builder()
        .eventId(instrumentEventEntity.getEventId())
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
                .tiebreakOne(parseScoreString(instrumentEventEntity.getTieBreakOne()))
                .tiebreakTwo(parseScoreString(instrumentEventEntity.getTieBreakTwo()))
                .build())
        .build();
  }

  protected List<Integer> parseScoreString(String scores) {
    return Arrays.asList(scores.split(",")).stream()
        .map(Integer::parseInt)
        .collect(Collectors.toList());
  }
}
