package com.trickl.influxdb.client;

import com.trickl.influxdb.persistence.SportsEventIncidentEntity;
import com.trickl.influxdb.transformers.SportsEventIncidentReader;
import com.trickl.influxdb.transformers.SportsEventIncidentTransformer;
import com.trickl.model.event.sports.SportsEventIncident;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.statistics.PriceSourceFieldFirstLastCount;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;

@RequiredArgsConstructor
public class SportsEventIncidentClient {

  private final InfluxDbClient influxDbClient;

  /**
   * Stores prices in the database.
   *
   * @param priceSource the instrument identifier
   * @param events data to store
   */
  public Flux<Integer> store(PriceSource priceSource, List<SportsEventIncident> events) {
    SportsEventIncidentTransformer transformer = new SportsEventIncidentTransformer(priceSource);
    List<SportsEventIncidentEntity> measurements =
        events.stream().map(transformer).collect(Collectors.toList());
    return influxDbClient.store(
        measurements,
        CommonDatabases.PRICES.getName(),
        SportsEventIncidentEntity.class,
        SportsEventIncidentEntity::getTime);
  }

  /**
   * Find candles.
   *
   * @param priceSource the instrument identifier
   * @param queryBetween Query parameters
   * @return A list of bars
   */
  public Flux<SportsEventIncident> findBetween(PriceSource priceSource, QueryBetween queryBetween) {
    SportsEventIncidentReader reader = new SportsEventIncidentReader();
    return influxDbClient
        .findBetween(
            priceSource,
            queryBetween,
            CommonDatabases.PRICES.getName(),
            "sports_event_incident",
            SportsEventIncidentEntity.class)
        .map(reader);
  }

  /**
   * Find a summary of incident updates between a period of time, grouped by instrument.
   *
   * @param queryBetween A time window there series must have a data point within
   * @return A list of series, including the first and last value of a field
   */
  public Flux<PriceSourceFieldFirstLastCount> findSummary(QueryBetween queryBetween) {
    return influxDbClient.findFieldFirstLastCountByDay(
        queryBetween, CommonDatabases.PRICES.getName(), "sports_event_incident", "matchTime");
  }
}
