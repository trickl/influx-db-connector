package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.AggregatedSportsEventMatchTimeUpdateEntity;
import com.trickl.model.event.AggregatedInstrumentEvents;
import com.trickl.model.event.sports.SportsEventMatchTime;
import com.trickl.model.event.sports.SportsEventMatchTimeUpdate;
import java.time.Instant;
import java.util.function.Function;

public class AggregatedSportsEventMatchTimeUpdateReader
    implements Function<AggregatedSportsEventMatchTimeUpdateEntity, AggregatedInstrumentEvents> {
  @Override
  public AggregatedInstrumentEvents apply(
      AggregatedSportsEventMatchTimeUpdateEntity instrumentEventEntity) {
    SportsEventMatchTimeUpdate firstEvent =
        SportsEventMatchTimeUpdate.builder()
            .time(
            instrumentEventEntity.getFirstTime() != null
                ? Instant.parse(instrumentEventEntity.getFirstTime())
                : null)
            .matchTime(
                SportsEventMatchTime.builder()
                    .matchTime(
                        SportsEventMatchTimeUpdateReader.parseDuration(
                            instrumentEventEntity.getFirstMatchTime()))
                    .remainingTime(
                        SportsEventMatchTimeUpdateReader.parseDuration(
                            instrumentEventEntity.getFirstRemainingTime()))
                    .remainingTimeInPeriod(
                        SportsEventMatchTimeUpdateReader.parseDuration(
                            instrumentEventEntity.getFirstRemainingTimeInPeriod()))
                    .build())
            .build();
    SportsEventMatchTimeUpdate lastEvent =
        SportsEventMatchTimeUpdate.builder()
        .time(
            instrumentEventEntity.getFirstTime() != null
                ? Instant.parse(instrumentEventEntity.getLastTime())
                : null)
            .matchTime(
                SportsEventMatchTime.builder()
                    .matchTime(
                        SportsEventMatchTimeUpdateReader.parseDuration(
                            instrumentEventEntity.getLastMatchTime()))
                    .remainingTime(
                        SportsEventMatchTimeUpdateReader.parseDuration(
                            instrumentEventEntity.getLastRemainingTime()))
                    .remainingTimeInPeriod(
                        SportsEventMatchTimeUpdateReader.parseDuration(
                            instrumentEventEntity.getLastRemainingTimeInPeriod()))
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
