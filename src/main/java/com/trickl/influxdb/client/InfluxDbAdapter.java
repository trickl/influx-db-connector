package com.trickl.influxdb.client;

import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.reactive.InfluxDBClientReactive;
import com.influxdb.client.reactive.QueryReactiveApi;
import com.influxdb.client.reactive.WriteReactiveApi;
import com.trickl.influxdb.text.Rfc3339;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.statistics.PriceSourceFieldFirstLastDuration;
import io.reactivex.Flowable;
import java.text.MessageFormat;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalField;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.apache.commons.lang3.tuple.Pair;
import reactor.adapter.rxjava.RxJava2Adapter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Log
@RequiredArgsConstructor
public class InfluxDbAdapter {

  protected final InfluxDBClientReactive influxDbClient;

  protected final String bucket;

  /**
   * Stores prices in the database.
   *
   * @param <T> the type of measurement
   * @param measurements data to store
   * @param measurementClazz the type of measurement
   * @param timeAccessor the time of the data
   * @return counts of records stored
   */
  @Valid
  public <T> Flux<Integer> store(
      List<T> measurements, Class<T> measurementClazz, Function<T, Instant> timeAccessor) {
    return storeBatchedByTime(
        measurements, IsoFields.WEEK_OF_WEEK_BASED_YEAR, measurementClazz, timeAccessor);
  }

  /**
   * Stores prices in the database.
   *
   * @param <T> the type of measurement
   * @param measurements data to store
   * @param batchField the temporal field to batch prices by
   * @param measurementClazz the type of measurement
   * @param timeAccessor the time of the data
   * @return counts of records stored
   */
  @Valid
  public <T> Flux<Integer> storeBatchedByTime(
      List<T> measurements,
      TemporalField batchField,
      Class<T> measurementClazz,
      Function<T, Instant> timeAccessor) {
    Map<Integer, List<T>> batchedMeasurements =
        measurements.stream()
            .collect(
                Collectors.groupingBy(
                    measurement -> {
                      Instant time = timeAccessor.apply(measurement);
                      if (time == null) {
                        return -1;
                      }
                      ZonedDateTime zonedTime = ZonedDateTime.ofInstant(time, ZoneOffset.UTC);
                      return zonedTime.get(batchField);
                    }));

    if (batchedMeasurements.containsKey(-1)) {
      String warningMessage =
          MessageFormat.format(
              "At least one record, e.g. {0} contains a invalid timestamp."
                  + " All such records will be ignored.",
              batchedMeasurements.get(-1).get(0));
      log.warning(warningMessage);
      batchedMeasurements.remove(-1);
    }

    return Flux.merge(
        Flux.fromIterable(batchedMeasurements.values())
            .map(measurement -> storeNoBatch(measurement)));
  }

  /**
   * Stores prices in the database.
   *
   * @param <T> the type of measurement
   * @param measurements data to store
   * @return count of records stored
   */
  @Valid
  public <T> Mono<Integer> storeNoBatch(List<T> measurements) {
    try (WriteReactiveApi writeApi = influxDbClient.getWriteReactiveApi()) {
      writeApi.writeMeasurements(WritePrecision.MS, Flowable.fromIterable(measurements));
    }

    return Mono.just(measurements.size());
  }

  /**
   * Find measurements in the database.   
   * @param <T> the type of measurement
   * @param priceSource The price source for the measurements
   * @param queryBetween Query parameters
   * @param measurementName The measurement name
   * @param measurementClazz the time of measurement
   * @return A list of measurements
   */
  public <T> Flux<T> findBetween(
      PriceSource priceSource,
      QueryBetween queryBetween,
      String measurementName,
      Class<T> measurementClazz) {
    return findBetween(
        priceSource, queryBetween, measurementName, measurementClazz, Optional.empty());
  }

  /**
   * Find measurements in the database.
   *
   * @param <T> the type of measurement
   * @param priceSource The price source for the measurements
   * @param queryBetween Query parameters
   * @param measurementName The measurement name
   * @param measurementClazz the time of measurement
   * @param filter An optional filter for fields
   * @return A list of measurements
   */
  public <T> Flux<T> findBetween(
      PriceSource priceSource,
      QueryBetween queryBetween,
      String measurementName,
      Class<T> measurementClazz,
      Optional<Pair<String, Set<String>>> filter) {

    String sortClause =
        MessageFormat.format(
            "|> sort(columns: [\"_time\"], desc: {0})\n", queryBetween.isMostRecentFirst());

    String limitClause = "";
    if (queryBetween.getLimit() != null) {
      limitClause = MessageFormat.format("|> limit(n: {0})\n", queryBetween.getLimit().toString());
    }

    String additionalFilterClause =
        filter.isPresent() ? FluxStatementFilterBuilder.buildFrom(filter.get()) : "";

    String flux =
        MessageFormat.format(
            "from(bucket:\"{0}\")\n"
                + "|> range(start: {4}, stop: {5})\n"
                + "|> filter(fn: (r) => r._measurement == \"{1}\" and "
                + "r.exchangeId == \"{2}\" and "
                + "r.instrumentId == \"{3}\")\n"
                + "|> pivot (rowKey:[\"_time\", \"exchangeId\", \"instrumentId\"], "
                + "columnKey: [\"_field\"], valueColumn: \"_value\")\n"
                + "{8}"
                + "|> group()\n{6}{7}",
            bucket,
            measurementName,
            priceSource.getExchangeId(),
            priceSource.getInstrumentId(),
            Rfc3339.YMDHMS_FORMATTER.format(
                ZonedDateTime.ofInstant(queryBetween.getStart(), ZoneOffset.UTC)),
            Rfc3339.YMDHMS_FORMATTER.format(
                ZonedDateTime.ofInstant(queryBetween.getEnd(), ZoneOffset.UTC)),
            sortClause,
            limitClause,
            additionalFilterClause);

    QueryReactiveApi queryApi = influxDbClient.getQueryReactiveApi();
    return RxJava2Adapter.flowableToFlux(queryApi.query(flux, measurementClazz));
  }

  /**
   * Find all available series that overlap a time window.
   *
   * @param queryBetween A time window there series must have a data point within
   * @param measurementName the name of the measurement
   * @param fieldName the name of the field to query
   * @return A list of series
   */
  public Flux<PriceSourceFieldFirstLastDuration> findFieldFirstLastCountByDay(
      QueryBetween queryBetween, String measurementName, String fieldName) {

    String flux =
        MessageFormat.format(
            "fieldFirstLastDuration = (measurement, field, start, stop) => '{'\n"
                + "  firstValue = from(bucket:\"{0}\")\n"
                + "  |> range(start: start, stop: stop)\n"
                + "  |> filter(fn: (r) => r._measurement == measurement and r._field == field)\n"
                + "  |> group(columns: [\"instrumentId\", \"exchangeId\"])\n"
                + "  |> first()\n"
                + "\n"
                + "  lastValue = from(bucket:\"{0}\")\n"
                + "    |> range(start: start, stop: stop)\n"
                + "    |> filter(fn: (r) => r._measurement == measurement and r._field == field)\n"
                + "    |> group(columns: [\"instrumentId\", \"exchangeId\"])\n"
                + "    |> last()\n"
                + "\n"
                + "  return join( tables: '{'f:firstValue, l:lastValue'}', on: [\"exchangeId\","
                + " \"instrumentId\"])\n"
                + "    |> map(fn: (r) => ('{'\n"
                + "\n"
                + "      _time: r._time_l,\n"
                + "      duration: string(v: duration(v: uint(v: r._time_l) - uint(v:"
                + " r._time_f))),\n"
                + "      first: r._value_f,\n"
                + "      last: r._value_l,\n"
                + "      exchangeId: r.exchangeId,\n"
                + "      instrumentId: r.instrumentId\n"
                + "   '}'))\n"
                + "'}'\n"
                + "\n"
                + "fieldFirstLastDuration(measurement: \"{1}\", field: \"state\", start: {2},"
                + " stop: {3})",
            bucket,
            measurementName,
            Rfc3339.YMDHMS_FORMATTER.format(
                ZonedDateTime.ofInstant(queryBetween.getStart(), ZoneOffset.UTC)),
            Rfc3339.YMDHMS_FORMATTER.format(
                ZonedDateTime.ofInstant(queryBetween.getEnd(), ZoneOffset.UTC)));

    QueryReactiveApi queryApi = influxDbClient.getQueryReactiveApi();
    return RxJava2Adapter.flowableToFlux(
        queryApi.query(flux, PriceSourceFieldFirstLastDuration.class));
  }
}
