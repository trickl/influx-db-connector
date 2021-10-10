package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.AggregatedSportsEventScoreUpdateEntity;
import com.trickl.model.event.AggregatedInstrumentEvents;
import com.trickl.model.event.sports.SportsEventScoreUpdate;
import com.trickl.model.event.sports.SportsEventScores;
import java.time.Instant;
import java.util.function.Function;

public class AggregatedSportsEventScoreUpdateReader
    implements Function<AggregatedSportsEventScoreUpdateEntity, AggregatedInstrumentEvents> {
  @Override
  public AggregatedInstrumentEvents apply(
      AggregatedSportsEventScoreUpdateEntity instrumentEventEntity) {
    SportsEventScoreUpdate firstEvent =
        SportsEventScoreUpdate.builder()
            .time(
            instrumentEventEntity.getFirstTime() != null
                ? Instant.parse(instrumentEventEntity.getFirstTime())
                : null)
            .scores(
                SportsEventScores.builder()
                    .current(
                        SportsEventScoreUpdateReader.parseScoreString(
                            instrumentEventEntity.getFirstCurrent()))
                    .firstHalf(
                        SportsEventScoreUpdateReader.parseScoreString(
                            instrumentEventEntity.getFirstFirstHalf()))
                    .secondHalf(
                        SportsEventScoreUpdateReader.parseScoreString(
                            instrumentEventEntity.getFirstSecondHalf()))
                    .normalTime(
                        SportsEventScoreUpdateReader.parseScoreString(
                            instrumentEventEntity.getFirstNormalTime()))
                    .fullTime(
                        SportsEventScoreUpdateReader.parseScoreString(
                            instrumentEventEntity.getFirstFullTime()))
                    .game(
                        SportsEventScoreUpdateReader.parseScoreString(
                            instrumentEventEntity.getFirstGame()))
                    .setOne(
                        SportsEventScoreUpdateReader.parseScoreString(
                            instrumentEventEntity.getFirstSetOne()))
                    .setTwo(
                        SportsEventScoreUpdateReader.parseScoreString(
                            instrumentEventEntity.getFirstSetTwo()))
                    .tiebreakOne(
                        SportsEventScoreUpdateReader.parseScoreString(
                            instrumentEventEntity.getFirstTieBreakOne()))
                    .tiebreakTwo(
                        SportsEventScoreUpdateReader.parseScoreString(
                            instrumentEventEntity.getFirstTieBreakTwo()))
                    .build())
            .build();
    SportsEventScoreUpdate lastEvent =
        SportsEventScoreUpdate.builder()
        .time(
            instrumentEventEntity.getFirstTime() != null
                ? Instant.parse(instrumentEventEntity.getLastTime())
                : null)
            .scores(
                SportsEventScores.builder()
                    .current(
                        SportsEventScoreUpdateReader.parseScoreString(
                            instrumentEventEntity.getLastCurrent()))
                    .firstHalf(
                        SportsEventScoreUpdateReader.parseScoreString(
                            instrumentEventEntity.getLastFirstHalf()))
                    .secondHalf(
                        SportsEventScoreUpdateReader.parseScoreString(
                            instrumentEventEntity.getLastSecondHalf()))
                    .normalTime(
                        SportsEventScoreUpdateReader.parseScoreString(
                            instrumentEventEntity.getLastNormalTime()))
                    .fullTime(
                        SportsEventScoreUpdateReader.parseScoreString(
                            instrumentEventEntity.getLastFullTime()))
                    .game(
                        SportsEventScoreUpdateReader.parseScoreString(
                            instrumentEventEntity.getLastGame()))
                    .setOne(
                        SportsEventScoreUpdateReader.parseScoreString(
                            instrumentEventEntity.getLastSetOne()))
                    .setTwo(
                        SportsEventScoreUpdateReader.parseScoreString(
                            instrumentEventEntity.getLastSetTwo()))
                    .tiebreakOne(
                        SportsEventScoreUpdateReader.parseScoreString(
                            instrumentEventEntity.getLastTieBreakOne()))
                    .tiebreakTwo(
                        SportsEventScoreUpdateReader.parseScoreString(
                            instrumentEventEntity.getLastTieBreakTwo()))
                    .build())
            .build();
    return AggregatedInstrumentEvents.builder()
        .exchangeId(instrumentEventEntity.getExchangeId().toUpperCase())
        .code(instrumentEventEntity.getInstrumentId().toUpperCase())
        .time(instrumentEventEntity.getTime())
        .firstEvent(firstEvent)
        .lastEvent(lastEvent)
        .count(instrumentEventEntity.getCount())
        .build();
  }
}
