package com.trickl.influxdb.client;

import com.influxdb.client.reactive.InfluxDBClientReactive;
import com.trickl.influxdb.binding.AnalyticBooleanValueReader;
import com.trickl.influxdb.binding.AnalyticBooleanValueWriter;
import com.trickl.influxdb.binding.AnalyticDoubleValueReader;
import com.trickl.influxdb.binding.AnalyticDoubleValueWriter;
import com.trickl.influxdb.binding.AnalyticIntegerValueReader;
import com.trickl.influxdb.binding.AnalyticIntegerValueWriter;
import com.trickl.influxdb.binding.AnalyticStringValueReader;
import com.trickl.influxdb.binding.AnalyticStringValueWriter;
import com.trickl.influxdb.persistence.AnalyticBooleanValueEntity;
import com.trickl.influxdb.persistence.AnalyticDoubleValueEntity;
import com.trickl.influxdb.persistence.AnalyticIntegerValueEntity;
import com.trickl.influxdb.persistence.AnalyticStringValueEntity;
import com.trickl.model.analytics.AnalyticId;
import com.trickl.model.analytics.InstantBoolean;
import com.trickl.model.analytics.InstantDouble;
import com.trickl.model.analytics.InstantInteger;
import com.trickl.model.analytics.InstantString;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.primitives.TemporalPriceSource;
import com.trickl.model.pricing.statistics.PriceSourceFieldFirstLastDuration;
import com.trickl.model.pricing.statistics.PriceSourceInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class AnalyticPrimitiveValueClient {

  private final InfluxDBClientReactive influxDbClient;

  private final String bucket;

  /**
   * Stores analytics in the database.
   *
   * @param analyticId the analytic identifier
   * @param temporalPriceSource the instrument and temporal source
   * @param values data to store
   * @return counts of records stored
   */
  public Flux<Integer> storeDoubles(
      AnalyticId analyticId, TemporalPriceSource temporalPriceSource, List<InstantDouble> values) {
    InfluxDbStorage influxDbStorage = new InfluxDbStorage(influxDbClient, bucket);
    AnalyticDoubleValueWriter transformer =
        new AnalyticDoubleValueWriter(analyticId, temporalPriceSource);
    List<AnalyticDoubleValueEntity> measurements =
        values.stream().map(transformer).collect(Collectors.toList());
    return influxDbStorage.store(
        measurements, AnalyticDoubleValueEntity.class, AnalyticDoubleValueEntity::getTime);
  }

  /**
   * Stores analytics in the database.
   *
   * @param analyticId the analytic identifier
   * @param temporalPriceSource the instrument and temporal source
   * @param values data to store
   * @return counts of records stored
   */
  public Flux<Integer> storeIntegers(
      AnalyticId analyticId, TemporalPriceSource temporalPriceSource, List<InstantInteger> values) {
    InfluxDbStorage influxDbStorage = new InfluxDbStorage(influxDbClient, bucket);
    AnalyticIntegerValueWriter transformer =
        new AnalyticIntegerValueWriter(analyticId, temporalPriceSource);
    List<AnalyticIntegerValueEntity> measurements =
        values.stream().map(transformer).collect(Collectors.toList());
    return influxDbStorage.store(
        measurements, AnalyticIntegerValueEntity.class, AnalyticIntegerValueEntity::getTime);
  }

  /**
   * Stores analytics in the database.
   *
   * @param analyticId the analytic identifier
   * @param temporalPriceSource the instrument and temporal source
   * @param values data to store
   * @return counts of records stored
   */
  public Flux<Integer> storeStrings(
      AnalyticId analyticId, TemporalPriceSource temporalPriceSource, List<InstantString> values) {
    InfluxDbStorage influxDbStorage = new InfluxDbStorage(influxDbClient, bucket);
    AnalyticStringValueWriter transformer =
        new AnalyticStringValueWriter(analyticId, temporalPriceSource);
    List<AnalyticStringValueEntity> measurements =
        values.stream().map(transformer).collect(Collectors.toList());
    return influxDbStorage.store(
        measurements, AnalyticStringValueEntity.class, AnalyticStringValueEntity::getTime);
  }

  /**
   * Stores analytics in the database.
   *
   * @param analyticId the analytic identifier
   * @param temporalPriceSource the instrument and temporal source
   * @param values data to store
   * @return counts of records stored
   */
  public Flux<Integer> storeBooleans(
      AnalyticId analyticId, TemporalPriceSource temporalPriceSource, List<InstantBoolean> values) {
    InfluxDbStorage influxDbStorage = new InfluxDbStorage(influxDbClient, bucket);
    AnalyticBooleanValueWriter transformer =
        new AnalyticBooleanValueWriter(analyticId, temporalPriceSource);
    List<AnalyticBooleanValueEntity> measurements =
        values.stream().map(transformer).collect(Collectors.toList());
    return influxDbStorage.store(
        measurements, AnalyticBooleanValueEntity.class, AnalyticBooleanValueEntity::getTime);
  }

  /**
   * Find analytic values.
   *
   * @param analyticId the analytic identifier
   * @param temporalPriceSource the analytic source
   * @param queryBetween Query parameters
   * @return A list of bars
   */
  private <T> Flux<T> findBetween(
      AnalyticId analyticId,
      TemporalPriceSource temporalPriceSource,
      QueryBetween queryBetween,
      String measurementName,
      Class<T> measurementClass) {
    Map<String, Set<String>> analyticSpec =
        new HashMap<String, Set<String>>() {
          {
            put("domain", Collections.singleton(analyticId.getDomain()));
            put("analyticName", Collections.singleton(analyticId.getName()));
          }
        };

    if (analyticId.getParameters() != null && analyticId.getParameters().length() > 0) {
      analyticSpec.put("parameters", Collections.singleton(analyticId.getParameters()));
    }
    InfluxDbFindBetween findBetween = new InfluxDbFindBetween(this.influxDbClient, bucket);
    return findBetween.findBetween(
        temporalPriceSource.getPriceSource(),
        queryBetween,
        measurementName,
        measurementClass,
        Collections.unmodifiableMap(analyticSpec),
        Optional.of(temporalPriceSource.getTemporalSource()));
  }

  /**
   * Find analytic values.
   *
   * @param analyticId the analytic identifier
   * @param temporalPriceSource the analytic source
   * @param queryBetween Query parameters
   * @return A list of bars
   */
  public Flux<InstantDouble> findDoublesBetween(
      AnalyticId analyticId, TemporalPriceSource temporalPriceSource, QueryBetween queryBetween) {
    AnalyticDoubleValueReader reader = new AnalyticDoubleValueReader();
    return findBetween(
            analyticId,
            temporalPriceSource,
            queryBetween,
            "analytic_double_value",
            AnalyticDoubleValueEntity.class)
        .map(reader);
  }

  /**
   * Find analytic values.
   *
   * @param analyticId the analytic identifier
   * @param temporalPriceSource the analytic source
   * @param queryBetween Query parameters
   * @return A list of bars
   */
  public Flux<InstantInteger> findIntegersBetween(
      AnalyticId analyticId, TemporalPriceSource temporalPriceSource, QueryBetween queryBetween) {
    AnalyticIntegerValueReader reader = new AnalyticIntegerValueReader();
    return findBetween(
            analyticId,
            temporalPriceSource,
            queryBetween,
            "analytic_integer_value",
            AnalyticIntegerValueEntity.class)
        .map(reader);
  }

  /**
   * Find analytic values.
   *
   * @param analyticId the analytic identifier
   * @param temporalPriceSource the analytic source
   * @param queryBetween Query parameters
   * @return A list of bars
   */
  public Flux<InstantString> findStringsBetween(
      AnalyticId analyticId, TemporalPriceSource temporalPriceSource, QueryBetween queryBetween) {
    AnalyticStringValueReader reader = new AnalyticStringValueReader();
    return findBetween(
            analyticId,
            temporalPriceSource,
            queryBetween,
            "analytic_string_value",
            AnalyticStringValueEntity.class)
        .map(reader);
  }

  /**
   * Find analytic values.
   *
   * @param analyticId the analytic identifier
   * @param temporalPriceSource the analytic source
   * @param queryBetween Query parameters
   * @return A list of bars
   */
  public Flux<InstantBoolean> findBooleansBetween(
      AnalyticId analyticId, TemporalPriceSource temporalPriceSource, QueryBetween queryBetween) {
    AnalyticBooleanValueReader reader = new AnalyticBooleanValueReader();
    return findBetween(
            analyticId,
            temporalPriceSource,
            queryBetween,
            "analytic_boolean_value",
            AnalyticBooleanValueEntity.class)
        .map(reader);
  }

  /**
   * Find a summary of changes between a period of time, grouped by instrument.
   *
   * @param queryBetween A time window there series must have a data point within
   * @param priceSource The price source
   * @return A list of series, including the first and last value of a field
   */
  public Flux<PriceSourceFieldFirstLastDuration> firstLastDuration(
      QueryBetween queryBetween, PriceSource priceSource) {
    InfluxDbFirstLastDuration influxDbAdapter =
        new InfluxDbFirstLastDuration(influxDbClient, bucket);
    return Flux.concat(
        Stream.of(
                "analytic_double_value",
                "analytic_integer_value",
                "analytic_string_value",
                "analytic_boolean_value")
            .map(
                measurementName ->
                    influxDbAdapter.firstLastDuration(
                        queryBetween, "measurementName", "time", priceSource))
            .collect(Collectors.toList()));
  }

  /**
   * Find a count of analytic values between a period of time, grouped by instrument.
   *
   * @param queryBetween A time window there series must have a data point within
   * @param priceSource The price source
   * @return Counts by instruments
   */
  public Mono<Integer> count(QueryBetween queryBetween, PriceSource priceSource) {
    InfluxDbCount influxDbClient = new InfluxDbCount(this.influxDbClient, bucket);
    return influxDbClient
        .count(queryBetween, "measurementName", "time", priceSource)
        .map(PriceSourceInteger::getValue);
  }
}
