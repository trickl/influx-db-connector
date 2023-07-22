package com.trickl.influxdb.client;

import com.influxdb.client.reactive.InfluxDBClientReactive;
import com.influxdb.client.reactive.QueryReactiveApi;
import com.influxdb.exceptions.BadRequestException;
import com.trickl.influxdb.text.Rfc3339;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.statistics.PriceSourceFieldFirstLastDuration;
import java.text.MessageFormat;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.logging.Level;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import reactor.core.publisher.Mono;

@Log
@RequiredArgsConstructor
public class InfluxDbFirstLastDuration {

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
  public Mono<PriceSourceFieldFirstLastDuration> firstLastDuration(
      QueryBetween queryBetween,
      String measurementName,
      String fieldName,
      PriceSource priceSource) {

    String filter =
        MessageFormat.format(
            "|> filter(fn: (r) => r._measurement == measurement and r._field == field"
                + " and r.exchangeId == \"{0}\" and r.instrumentId == \"{1}\")\n",
            priceSource.getExchangeId(), priceSource.getInstrumentId());

    String flux =
        MessageFormat.format(
            "fieldFirstLastDuration = (measurement, field, start, stop) => '{'\n"
                + "  firstValue = from(bucket:\"{0}\")\n"
                + "  |> range(start: start, stop: stop)\n"
                + "  {1}"
                + "  |> group(columns: [\"instrumentId\", \"exchangeId\"])\n"
                + "  |> first()\n"
                + "  |> toString()\n"
                + "\n"
                + "  lastValue = from(bucket:\"{0}\")\n"
                + "    |> range(start: start, stop: stop)\n"
                + "    {1}"
                + "    |> group(columns: [\"instrumentId\", \"exchangeId\"])\n"
                + "    |> last()\n"
                + "    |> toString()\n"
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
                + "fieldFirstLastDuration(measurement: \"{2}\", field: \"{3}\", start: {4},"
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
    return Mono.from(queryApi.query(flux, PriceSourceFieldFirstLastDuration.class))
        .doOnError(
            BadRequestException.class,
            e -> {
              log.log(Level.WARNING, "Error executing query: " + flux);
            });
  }
}
