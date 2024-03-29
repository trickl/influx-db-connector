package com.trickl.influxdb.client;

import com.influxdb.client.reactive.InfluxDBClientReactive;
import com.influxdb.client.reactive.QueryReactiveApi;
import com.influxdb.exceptions.BadRequestException;
import com.trickl.influxdb.text.Rfc3339;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.statistics.PriceSourceInteger;
import java.text.MessageFormat;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.logging.Level;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import reactor.core.publisher.Mono;

@Log
@RequiredArgsConstructor
public class InfluxDbCount {

  protected final InfluxDBClientReactive influxDbClient;

  protected final String bucket;

  /**
   * Find all available series that overlap a time window.
   *
   * @param queryBetween A time window there series must have a data point within
   * @param measurementName the name of the measurement
   * @param fieldName the name of the field to query
   * @param priceSource filter on this price source
   * @return A list of series
   */
  public Mono<PriceSourceInteger> count(
      QueryBetween queryBetween,
      String measurementName,
      String fieldName,
      PriceSource priceSource) {
    return count(queryBetween, measurementName, fieldName, priceSource, Optional.empty());
  }

  /**
   * Find all available series that overlap a time window.
   *
   * @param queryBetween A time window there series must have a data point within
   * @param measurementName the name of the measurement
   * @param fieldName the name of the field to query
   * @param priceSource filter on this price source
   * @return A list of series
   */
  public Mono<PriceSourceInteger> count(
      QueryBetween queryBetween,
      String measurementName,
      String fieldName,
      PriceSource priceSource,
      Optional<String> additionalFilter) {

    String filterExtension = additionalFilter.map(s -> " and " + s).orElse("");

    String filter =
        MessageFormat.format(
            "|> filter(fn: (r) => r._measurement == measurement and r._field == field"
                + " and r.exchangeId == \"{0}\" and r.instrumentId == \"{1}\"{2})\n",
            priceSource.getExchangeId(), priceSource.getInstrumentId(), filterExtension);

    String flux =
        MessageFormat.format(
            "filteredCount = (measurement, field, start, stop) => '{'\n"
                + "  return from(bucket:\"{0}\")\n"
                + "  |> range(start: start, stop: stop)\n"
                + "  {1}"
                + "  |> group(columns: [\"instrumentId\", \"exchangeId\"])\n"
                + "  |> count()\n"
                + "'}'\n"
                + "\n"
                + "filteredCount(measurement: \"{2}\", field: \"{3}\", start: {4},"
                + " stop: {5})",
            bucket,
            filter,
            measurementName,
            fieldName,
            Rfc3339.YMDHMS_FORMATTER.format(
                ZonedDateTime.ofInstant(queryBetween.getStart(), ZoneOffset.UTC)),
            Rfc3339.YMDHMS_FORMATTER.format(
                ZonedDateTime.ofInstant(queryBetween.getEnd(), ZoneOffset.UTC)));

    QueryReactiveApi queryApi = influxDbClient.getQueryReactiveApi();

    return Mono.from(queryApi.query(flux, PriceSourceInteger.class))
        .doOnError(
            BadRequestException.class,
            e -> {
              log.log(Level.WARNING, "Error executing query: " + flux);
            })
        .defaultIfEmpty(
            PriceSourceInteger.builder()
                .exchangeId(priceSource.getExchangeId())
                .instrumentId(priceSource.getInstrumentId())
                .value(0)
                .build());
  }
}
