package com.trickl.influxdb.client;

import com.influxdb.client.reactive.InfluxDBClientReactive;
import com.influxdb.client.reactive.QueryReactiveApi;
import com.influxdb.exceptions.BadRequestException;
import com.trickl.influxdb.text.Rfc3339;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.statistics.PriceSourceInstantDouble;
import java.text.MessageFormat;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.logging.Level;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import reactor.core.publisher.Flux;

@Log
@RequiredArgsConstructor
public class InfluxDbWindowedAverages {

  protected final InfluxDBClientReactive influxDbClient;

  protected final String bucket;

  /**
   * Find all available series that overlap a time window.
   *
   * @param queryBetween A time window there series must have a data point within
   * @param lhsMeasurementName the name of the measurement
   * @param lhsFieldName the name of the field to query
   * @param priceSource filter on this price source
   * @return A list of series
   */
  public Flux<PriceSourceInstantDouble> windowedAverages(
      QueryBetween queryBetween,
      String lhsMeasurementName,
      String lhsFieldName,
      String rhsMeasurementName,
      String rhsFieldName,
      String windowPeriod,
      PriceSource priceSource,
      Optional<String> additionalLhsFilter,
      Optional<String> additionalRhsFilter) {

    String filterLhsExtension = additionalLhsFilter.map(s -> " and " + s).orElse("");

    String lhsFilter =
        MessageFormat.format(
            "|> filter(fn: (r) => r._measurement == \"{2}\" and r._field == \"{3}\""
                + " and r.exchangeId == \"{0}\" and r.instrumentId == \"{1}\"{4})\n",
            priceSource.getExchangeId(),
            priceSource.getInstrumentId(),
            lhsMeasurementName,
            lhsFieldName,
            filterLhsExtension);

    String filterRhsExtension = additionalRhsFilter.map(s -> " and " + s).orElse("");

    String rhsFilter =
        MessageFormat.format(
            "|> filter(fn: (r) => r._measurement == \"{2}\" and r._field == \"{3}\""
                + " and r.exchangeId == \"{0}\" and r.instrumentId == \"{1}\"{4})\n",
            priceSource.getExchangeId(),
            priceSource.getInstrumentId(),
            rhsMeasurementName,
            rhsFieldName,
            filterRhsExtension);

    String flux =
        MessageFormat.format(
            "calcAverages = (start, stop) => '{'\n"
                + "  lhs = from(bucket:\"{0}\")\n"
                + "  |> range(start: start, stop: stop)\n"
                + "  {1}"
                + "  |> group(columns: [\"instrumentId\", \"exchangeId\"])\n"
                + "  |> window(every: {3})\n"
                + "  |> median()\n"
                + "\n"
                + "  rhs = from(bucket:\"{0}\")\n"
                + "    |> range(start: start, stop: stop)\n"
                + "    {2}"
                + "    |> group(columns: [\"instrumentId\", \"exchangeId\"])\n"
                + "  |> window(every: {3})\n"
                + "  |> median()\n"
                + "\n"
                + "  return join( tables: '{'f:lhs, l:rhs'}', on: [\"exchangeId\","
                + " \"instrumentId\", \"_stop\"])\n"
                + "    |> map(fn: (r) => ('{'\n"
                + "\n"
                + "      _time: r._stop,\n"
                + "      _value: (r._value_l + r._value_f) / 2.0,\n"
                + "      exchangeId: r.exchangeId,\n"
                + "      instrumentId: r.instrumentId\n"
                + "   '}'))\n"
                + "'}'\n"
                + "\n"
                + "calcAverages(start: {4}, stop: {5})",
            bucket,
            lhsFilter,
            rhsFilter,
            windowPeriod,
            Rfc3339.YMDHMS_FORMATTER.format(
                ZonedDateTime.ofInstant(queryBetween.getStart(), ZoneOffset.UTC)),
            Rfc3339.YMDHMS_FORMATTER.format(
                ZonedDateTime.ofInstant(queryBetween.getEnd(), ZoneOffset.UTC)));

    QueryReactiveApi queryApi = influxDbClient.getQueryReactiveApi();

    return Flux.from(queryApi.query(flux, PriceSourceInstantDouble.class))
        .doOnError(
            BadRequestException.class,
            e -> {
              log.log(Level.WARNING, "Error executing query: " + flux);
            });
  }
}
