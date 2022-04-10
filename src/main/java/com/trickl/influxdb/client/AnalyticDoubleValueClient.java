package com.trickl.influxdb.client;

import com.trickl.influxdb.binding.AnalyticDoubleValueReader;
import com.trickl.influxdb.binding.AnalyticDoubleValueWriter;
import com.trickl.influxdb.persistence.AnalyticDoubleValueEntity;
import com.trickl.model.analytics.AnalyticId;
import com.trickl.model.analytics.InstantDouble;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.primitives.TemporalPriceSource;
import com.trickl.model.pricing.statistics.PriceSourceFieldFirstLastDuration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;

@RequiredArgsConstructor
public class AnalyticDoubleValueClient {

  private final InfluxDbAdapter influxDbClient;

  /**
   * Stores analytics in the database.
   *
   * @param analyticId the analytic identifier
   * @param temporalPriceSource the instrument and temporal source
   * @param values data to store
   * @return counts of records stored
   */
  public Flux<Integer> store(
      AnalyticId analyticId, TemporalPriceSource temporalPriceSource, List<InstantDouble> values) {
    AnalyticDoubleValueWriter transformer =
        new AnalyticDoubleValueWriter(analyticId, temporalPriceSource);
    List<AnalyticDoubleValueEntity> measurements =
        values.stream().map(transformer).collect(Collectors.toList());
    return influxDbClient.store(
        measurements, AnalyticDoubleValueEntity.class, AnalyticDoubleValueEntity::getTime);
  }

  /**
   * Find analytic values.
   *
   * @param analyticId the analytic identifier
   * @param temporalPriceSource the analytic source
   * @param queryBetween Query parameters
   * @return A list of bars
   */
  public Flux<InstantDouble> findBetween(
      AnalyticId analyticId, TemporalPriceSource temporalPriceSource, QueryBetween queryBetween) {
    AnalyticDoubleValueReader reader = new AnalyticDoubleValueReader();
    Map<String, Set<String>> analyticSpec =
        new HashMap<String, Set<String>>() {
          {
            put("domain", Collections.singleton(analyticId.getDomain()));
            put("analyticName", Collections.singleton(analyticId.getName()));
          }
        };

    if (analyticId.getParameters() != null && analyticId.getParameters() != "") {
      analyticSpec.put("parameters", Collections.singleton(analyticId.getParameters()));
    }

    return influxDbClient
        .findBetween(
            temporalPriceSource.getPriceSource(),
            queryBetween,
            "analytic_double_value",
            AnalyticDoubleValueEntity.class,
            Collections.unmodifiableMap(analyticSpec),
            Optional.of(temporalPriceSource.getTemporalSource()))
        .map(reader);
  }

  /**
   * Find a summary of changes between a period of time, grouped by instrument.
   *
   * @param queryBetween A time window there series must have a data point within
   * @param priceSource The price source
   * @return A list of series, including the first and last value of a field
   */
  public Flux<PriceSourceFieldFirstLastDuration> findSummary(
      QueryBetween queryBetween, Optional<PriceSource> priceSource) {
    return influxDbClient.findFieldFirstLastCountByDay(
        queryBetween, "analytic_double_value", "time", priceSource);
  }
}
