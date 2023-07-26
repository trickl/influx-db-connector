package com.trickl.influxdb.client;

import com.influxdb.client.reactive.InfluxDBClientReactive;
import com.influxdb.client.reactive.QueryReactiveApi;
import com.trickl.influxdb.text.Rfc3339;
import com.trickl.model.pricing.primitives.PriceSource;
import java.text.MessageFormat;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;

@RequiredArgsConstructor
public class InfluxDbFindBetween {

  protected final InfluxDBClientReactive influxDbClient;

  protected final String bucket;

  /**
   * Find measurements in the database.
   *
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
        priceSource, queryBetween, measurementName, measurementClazz, Collections.emptyMap());
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
      Map<String, Set<String>> filter) {
    return findBetween(
        priceSource, queryBetween, measurementName, measurementClazz, filter, Optional.empty());
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
   * @param temporalSource An optional temporal source
   * @return A list of measurements
   */
  public <T> Flux<T> findBetween(
      PriceSource priceSource,
      QueryBetween queryBetween,
      String measurementName,
      Class<T> measurementClazz,
      Map<String, Set<String>> filter,
      Optional<String> temporalSource) {

    String sortClause =
        MessageFormat.format(
            "|> sort(columns: [\"_time\"], desc: {0})\n", queryBetween.isMostRecentFirst());

    String limitClause = "";
    if (queryBetween.getLimit() != null) {
      limitClause = MessageFormat.format("|> limit(n: {0})\n", queryBetween.getLimit().toString());
    }

    String additionalFilterClause = FluxStatementFilterBuilder.buildFrom(filter);

    String additionalTemporalClause =
        temporalSource.isPresent()
            ? String.format("r.temporalSource == \"%s\" and ", temporalSource.get())
            : "";

    String flux =
        MessageFormat.format(
            "from(bucket:\"{0}\")\n"
                + "|> range(start: {4}, stop: {5})\n"
                + "|> filter(fn: (r) => r._measurement == \"{1}\" and "
                + "r.exchangeId == \"{2}\" and "
                + "{9}"
                + "r.instrumentId == \"{3}\")\n"
                + "|> pivot (rowKey:[\"_time\", \"exchangeId\", \"instrumentId\"], "
                + "columnKey: [\"_field\"], valueColumn: \"_value\")\n"
                + "{8}"
                + "|> group()\n{6}{7}",
            bucket,
            measurementName,
            priceSource.getExchangeId().toUpperCase(),
            priceSource.getInstrumentId().toUpperCase(),
            Rfc3339.YMDHMS_FORMATTER.format(
                ZonedDateTime.ofInstant(queryBetween.getStart(), ZoneOffset.UTC)),
            Rfc3339.YMDHMS_FORMATTER.format(
                ZonedDateTime.ofInstant(queryBetween.getEnd(), ZoneOffset.UTC)),
            sortClause,
            limitClause,
            additionalFilterClause,
            additionalTemporalClause);

    QueryReactiveApi queryApi = influxDbClient.getQueryReactiveApi();
    return Flux.from(queryApi.query(flux, measurementClazz));
  }
}
