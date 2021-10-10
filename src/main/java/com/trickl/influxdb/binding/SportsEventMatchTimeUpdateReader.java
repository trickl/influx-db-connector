package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.SportsEventMatchTimeUpdateEntity;
import com.trickl.model.event.sports.SportsEventMatchTime;
import com.trickl.model.event.sports.SportsEventMatchTimeUpdate;
import java.time.Duration;
import java.util.function.Function;

public class SportsEventMatchTimeUpdateReader
    implements Function<SportsEventMatchTimeUpdateEntity, SportsEventMatchTimeUpdate> {

  @Override
  public SportsEventMatchTimeUpdate apply(SportsEventMatchTimeUpdateEntity instrumentEventEntity) {
    return SportsEventMatchTimeUpdate.builder()
        .time(instrumentEventEntity.getTime())
        .matchTime(SportsEventMatchTime.builder()
            .matchTime(parseDuration(instrumentEventEntity.getMatchTime()))
            .remainingTime(parseDuration(instrumentEventEntity.getRemainingTime()))
            .remainingTimeInPeriod(parseDuration(instrumentEventEntity.getRemainingTimeInPeriod()))
            .build())
        .build();
  }

  /** Parse an optional duration string if present. 
   * @param text Text to parse
   * @return A Duration
  */
  public static Duration parseDuration(String text) {
    if (text == null) {
      return null;
    }
    return Duration.parse(text);
  }
}
