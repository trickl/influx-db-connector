package com.trickl.influxdb.client;

import com.influxdb.client.reactive.InfluxDBClientReactive;
import com.influxdb.client.reactive.QueryReactiveApi;
import com.influxdb.exceptions.BadRequestException;
import com.trickl.influxdb.persistence.AggregatedSportsEventIncidentEntity;
import com.trickl.influxdb.persistence.AggregatedSportsEventMatchTimeUpdateEntity;
import com.trickl.influxdb.persistence.AggregatedSportsEventScoreUpdateEntity;
import com.trickl.influxdb.persistence.OhlcvBarEntity;
import com.trickl.influxdb.text.Rfc3339;
import com.trickl.model.pricing.primitives.PriceSource;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.apache.commons.lang3.tuple.Pair;
import reactor.core.publisher.Flux;

@Log
@RequiredArgsConstructor
public class InfluxDbAggregator {

  protected final InfluxDBClientReactive influxDbClient;

  protected final String bucket;

  protected final String organisation;

  /**
   * Aggregate price data into bid or ask candles.
   *
   * @param priceSource The source of prices
   * @param queryBetween A time window there series must have a data point within
   * @param measurementName the target measurment name
   * @param isBidRequest True if aggregating bid data, false for ask data
   * @param candleWidth the widrh of aggregated candles
   * @return A list of series
   */
  public Flux<OhlcvBarEntity> aggregateBestBidOrAskBetween(
      PriceSource priceSource,
      QueryBetween queryBetween,
      String measurementName,
      boolean isBidRequest,
      Duration candleWidth) {
    String candleWidthPeriod = InfluxDbDurationFormatter.format(candleWidth);

    String flux =
        MessageFormat.format(
            "from(bucket:\"{0}\")\n"
                + "|> range(start: {4}, stop: {5})\n"
                + "|> filter(fn: (r) => r._measurement == \"order\" and\n"
                + "  r._field == \"price\" and\n"
                + "  r.exchangeId == \"{1}\" and\n"
                + "  r.instrumentId == \"{2}\" and\n"
                + "  r.depth == \"0\" and\n"
                + "  r.bidOrAsk == \"{3}\")\n"
                + "|> drop(columns: [\"depth\", \"_field\", \"_measurement\"])\n"
                + "|> sort(columns: [\"_time\"], desc: false)\n"
                + "|> window(every: {6})\n"
                + "|> reduce(fn: (r, accumulator) => ('{'\n"
                + "  open: if accumulator.count == 0 then r._value else accumulator.open,\n"
                + "  high: if r._value > accumulator.high then r._value else accumulator.high,\n"
                + "  low: if r._value < accumulator.low then r._value else accumulator.low,\n"
                + "  close: r._value,\n"
                + "  count: accumulator.count + 1\n"
                + "  '}'),\n"
                + "  identity: '{'open: 0.0, high: 0.0, low: 999999.0, close: 0.0, count: 0'}'\n"
                + ")\n"
                + "|> drop(columns: [\"count\"])\n"
                + "|> duplicate(column: \"_stop\", as: \"_time\")\n"
                + "|> set(key: \"_measurement\", value: \"{7}\")\n"
                + "|> to(\n"
                + "  bucket: \"{0}\",\n"
                + "  org: \"{8}\",\n"
                + "  tagColumns: [\"exchangeId\", \"instrumentId\", \"bidOrAsk\"],\n"
                + "  fieldFn: (r) => ('{'\"open\": r.open, \"high\": r.high,"
                + " \"low\": r.low, \"close\": r.close'}'))",
            bucket,
            priceSource.getExchangeId().toUpperCase(),
            priceSource.getInstrumentId().toUpperCase(),
            isBidRequest ? 'B' : 'A',
            Rfc3339.YMDHMS_FORMATTER.format(
                ZonedDateTime.ofInstant(queryBetween.getStart(), ZoneOffset.UTC)),
            Rfc3339.YMDHMS_FORMATTER.format(
                ZonedDateTime.ofInstant(queryBetween.getEnd(), ZoneOffset.UTC)),
            candleWidthPeriod,
            measurementName,
            organisation);

    QueryReactiveApi queryApi = influxDbClient.getQueryReactiveApi();
    return Flux.from(queryApi.query(flux, OhlcvBarEntity.class))
        .doOnError(
            BadRequestException.class,
            e -> {
              log.log(Level.WARNING, "Error executing query: " + flux);
            });
  }

  /**
   * Aggregate event data into event aggregates.
   *
   * @param priceSource The source of prices
   * @param queryBetween A time window there series must have a data point within
   * @param measurementName the target measurment name
   * @param aggregateEventWidth the width of aggregated events
   * @param filter an optional filter for fields
   * @return A list of series
   */
  public Flux<AggregatedSportsEventIncidentEntity> aggregateSportsEventIncidentsBetween(
      PriceSource priceSource,
      QueryBetween queryBetween,
      String measurementName,
      Duration aggregateEventWidth,
      Optional<Pair<String, Set<String>>> filter) {
    String aggWidthPeriod = InfluxDbDurationFormatter.format(aggregateEventWidth);

    String additionalFilterClause =
        filter.isPresent() ? FluxStatementFilterBuilder.buildFrom(filter.get()) : "";

    String flux =
        MessageFormat.format(
            "from(bucket:\"{0}\")\n"
                + "|> range(start: {4}, stop: {5})\n"
                + "|> filter(fn: (r) => r._measurement == \"sports_event_incident\" and\n"
                + "  r.exchangeId == \"{1}\" and\n"
                + "  r.instrumentId == \"{2}\")\n"
                + "|> pivot (rowKey:[\"_time\", \"exchangeId\", \"instrumentId\"], columnKey:"
                + " [\"_field\"], valueColumn: \"_value\")\n"
                + "{3}|> drop(columns: [\"_field\", \"_measurement\"])\n"
                + "|> sort(columns: [\"matchTime\"], desc: false)\n"
                + "|> window(every: {6})\n"
                + "|> reduce(fn: (r, accumulator) => ('{'\n"
                + " firstTime: if accumulator.count == 0 then string(v: r._time) else"
                + " accumulator.firstTime,\n"
                + " firstMatchTime: if accumulator.count == 0 then r.matchTime else"
                + " accumulator.firstMatchTime,\n"
                + " firstIncidentType: if accumulator.count == 0 then r.incidentType else"
                + " accumulator.firstIncidentType,\n"
                + " firstSide: if accumulator.count == 0    then r.side else"
                + " accumulator.firstSide,\n"
                + " lastTime: string(v: r._time),\n"
                + " lastMatchTime: r.matchTime,\n"
                + " lastIncidentType: r.incidentType,\n"
                + " lastSide: r.side,\n"
                + " count: accumulator.count + 1'}'),\n"
                + "identity: '{'\n"
                + " firstTime: \"1970-01-01T00:00:00Z\", firstMatchTime: \"\", firstIncidentType:"
                + " \"\", firstSide: \"\", lastTime: \"1970-01-01T00:00:00Z\", lastMatchTime: \"\","
                + " lastIncidentType: \"\", lastSide: \"\", count: 0'}')\n"
                + "|> duplicate(column: \"_stop\", as: \"_time\")\n"
                + "|> set(key: \"_measurement\", value: \"{7}\")\n"
                + "|> to(\n"
                + "  bucket: \"{0}\",\n"
                + "  org: \"{8}\",\n"
                + "  tagColumns: [\"exchangeId\", \"instrumentId\"],\n"
                + "  fieldFn: (r) => ('{'\n"
                + " \"firstTime\": if exists r.firstTime then string(v: r.firstTime) else"
                + " \"1970-01-01T00:00:00Z\",\n"
                + " \"firstMatchTime\": if exists r.firstMatchTime then r.firstMatchTime else"
                + " \"\",\n"
                + " \"firstIncidentType\": if exists r.firstIncidentType then r.firstIncidentType"
                + " else \"\",\n"
                + " \"firstSide\": if exists r.firstSide then r.firstSide else \"\",\n"
                + " \"lastTime\": if exists r.lastTime then string(v: r.lastTime) else"
                + " \"1970-01-01T00:00:00Z\",\n"
                + " \"lastMatchTime\": if exists r.lastMatchTime then r.lastMatchTime else \"\",\n"
                + " \"lastIncidentType\": if exists r.lastIncidentType then r.lastIncidentType"
                + " else \"\",\n"
                + " \"lastSide\": if exists r.lastSide then r.lastSide else \"\",\n"
                + " \"count\": r.count'}'))",
            bucket,
            priceSource.getExchangeId().toUpperCase(),
            priceSource.getInstrumentId().toUpperCase(),
            additionalFilterClause,
            Rfc3339.YMDHMS_FORMATTER.format(
                ZonedDateTime.ofInstant(queryBetween.getStart(), ZoneOffset.UTC)),
            Rfc3339.YMDHMS_FORMATTER.format(
                ZonedDateTime.ofInstant(queryBetween.getEnd(), ZoneOffset.UTC)),
            aggWidthPeriod,
            measurementName,
            organisation);

    QueryReactiveApi queryApi = influxDbClient.getQueryReactiveApi();
    return Flux.from(queryApi.query(flux, AggregatedSportsEventIncidentEntity.class));
  }

  /**
   * Aggregate score updates into event aggregates.
   *
   * @param priceSource The source of prices
   * @param queryBetween A time window there series must have a data point within
   * @param measurementName the target measurment name
   * @param aggregateEventWidth the width of aggregated events
   * @param filter an optional filter for fields
   * @return A list of series
   */
  public Flux<AggregatedSportsEventScoreUpdateEntity> aggregateSportsEventScoreUpdatesBetween(
      PriceSource priceSource,
      QueryBetween queryBetween,
      String measurementName,
      Duration aggregateEventWidth,
      Optional<Pair<String, Set<String>>> filter) {
    String aggWidthPeriod = InfluxDbDurationFormatter.format(aggregateEventWidth);

    String additionalFilterClause =
        filter.isPresent() ? FluxStatementFilterBuilder.buildFrom(filter.get()) : "";

    String flux =
        MessageFormat.format(
            "from(bucket:\"{0}\")\n"
                + "|> range(start: {4}, stop: {5})\n"
                + "|> filter(fn: (r) => r._measurement == \"sports_event_score_update\" and\n"
                + "  r.exchangeId == \"{1}\" and\n"
                + "  r.instrumentId == \"{2}\")\n"
                + "|> pivot (rowKey:[\"_time\", \"exchangeId\", \"instrumentId\"], columnKey:"
                + " [\"_field\"], valueColumn: \"_value\")\n"
                + "{3}|> drop(columns: [\"_field\", \"_measurement\"])\n"
                + "|> sort(columns: [\"_time\"], desc: false)\n"
                + "|> window(every: {6})\n"
                + "|> reduce(fn: (r, accumulator) => ('{'\n"
                + " firstTime: if accumulator.count == 0 then string(v: r._time) else"
                + " accumulator.firstTime,\n"
                + " firstCurrent: if accumulator.count == 0 and exists r.current then r.current"
                + " else accumulator.firstCurrent,\n"
                + " firstFirstHalf: if accumulator.count == 0 and exists r.firstHalf then"
                + " r.firstHalf else accumulator.firstFirstHalf,\n"
                + " firstSecondHalf: if accumulator.count == 0 and exists r.secondHalf then"
                + " r.secondHalf else accumulator.firstSecondHalf,\n"
                + " firstNormalTime: if accumulator.count == 0 and exists r.normalTime then"
                + " r.normalTime else accumulator.firstNormalTime,\n"
                + " firstFullTime: if accumulator.count == 0 and exists r.fullTime then r.fullTime"
                + " else accumulator.firstFullTime,\n"
                + " firstGame: if accumulator.count == 0 and exists r.game then r.game else"
                + " accumulator.firstGame,\n"
                + " firstSetOne: if accumulator.count == 0 and exists r.setOne then r.setOne else"
                + " accumulator.firstSetOne,\n"
                + " firstSetTwo: if accumulator.count == 0 and exists r.setTwo then r.setTwo else"
                + " accumulator.firstSetTwo,\n"
                + " firstSets: if accumulator.count == 0 and exists r.sets then r.sets else"
                + " accumulator.firstSets,\n"
                + " firstTieBreakOne: if accumulator.count == 0 and exists r.tieBreakOne then"
                + " r.tieBreakOne else accumulator.firstTieBreakOne,\n"
                + " firstTieBreakTwo: if accumulator.count == 0 and exists r.tieBreakTwo then"
                + " r.tieBreakTwo else accumulator.firstTieBreakTwo,\n"
                + " lastTime: string(v: r._time),\n"
                + " lastCurrent: if exists r.current then r.current else \"\",\n"
                + " lastFirstHalf: if exists r.firstHalf then r.firstHalf else \"\",\n"
                + " lastSecondHalf: if exists r.secondHalf then r.secondHalf else \"\",\n"
                + " lastNormalTime: if exists r.normalTime then r.normalTime else \"\",\n"
                + " lastFullTime: if exists r.fullTime then r.fullTime else \"\",\n"
                + " lastGame: if exists r.game then r.game else \"\",\n"
                + " lastSetOne: if exists r.setOne then r.setOne else \"\",\n"
                + " lastSetTwo: if exists r.setTwo then r.setTwo else \"\",\n"
                + " lastSets: if exists r.sets then r.sets else \"\",\n"
                + " lastTieBreakOne: if exists r.tieBreakOne then r.tieBreakOne else \"\",\n"
                + " lastTieBreakTwo: if exists r.tieBreakTwo then r.tieBreakTwo else \"\",\n"
                + " count: accumulator.count + 1'}'),\n"
                + "identity: '{'\n"
                + " firstTime: \"1970-01-01T00:00:00Z\", firstCurrent: \"\", firstFirstHalf: \"\","
                + " firstSecondHalf: \"\", firstNormalTime: \"\", firstFullTime: \"\", firstGame:"
                + " \"\", firstSetOne: \"\", firstSetTwo: \"\", firstSets: \"\", firstTieBreakOne:"
                + " \"\", firstTieBreakTwo: \"\", lastTime: \"1970-01-01T00:00:00Z\",  lastCurrent:"
                + " \"\", lastFirstHalf: \"\", lastSecondHalf: \"\", lastNormalTime: \"\","
                + " lastFullTime: \"\", lastGame: \"\", lastSetOne: \"\", lastSetTwo: \"\","
                + " lastSets: \"\", lastTieBreakOne: \"\", lastTieBreakTwo: \"\", count: 0'}')\n"
                + "|> duplicate(column: \"_stop\", as: \"_time\")\n"
                + "|> set(key: \"_measurement\", value: \"{7}\")\n"
                + "|> to(\n"
                + "  bucket: \"{0}\",\n"
                + "  org: \"{8}\",\n"
                + "  tagColumns: [\"exchangeId\", \"instrumentId\"],\n"
                + "  fieldFn: (r) => ('{'\n"
                + " \"firstTime\": if exists r.firstTime then string(v: r.firstTime) else"
                + " \"1970-01-01T00:00:00Z\",\n"
                + " \"firstCurrent\": if exists r.firstCurrent then r.firstCurrent else \"\",\n"
                + " \"firstFirstHalf\": if exists r.firstFirstHalf then r.firstFirstHalf else"
                + " \"\",\n"
                + " \"firstSecondHalf\": if exists r.firstSecondHalf then r.firstSecondHalf else"
                + " \"\",\n"
                + " \"firstNormalTime\": if exists r.firstNormalTime then r.firstNormalTime else"
                + " \"\",\n"
                + " \"firstFullTime\": if exists r.firstFullTime then r.firstFullTime else \"\",\n"
                + " \"firstGame\": if exists r.firstGame then r.firstGame else \"\",\n"
                + " \"firstSetOne\": if exists r.firstSetOne then r.firstSetOne else \"\",\n"
                + " \"firstSetTwo\": if exists r.firstSetTwo then r.firstSetTwo else \"\",\n"
                + " \"firstSets\": if exists r.firstSets then r.firstSets else \"\",\n"
                + " \"firstTieBreakOne\": if exists r.firstTieBreakOne then r.firstTieBreakOne"
                + " else \"\",\n"
                + " \"firstTieBreakTwo\": if exists r.firstTieBreakTwo then r.firstTieBreakTwo"
                + " else \"\",\n"
                + " \"lastTime\": if exists r.lastTime then string(v: r.lastTime) else"
                + " \"1970-01-01T00:00:00Z\",\n"
                + " \"lastCurrent\": if exists r.lastCurrent then r.lastCurrent else \"\",\n"
                + " \"lastFirstHalf\": if exists r.lastFirstHalf then r.lastFirstHalf else \"\",\n"
                + " \"lastSecondHalf\": if exists r.lastSecondHalf then r.lastSecondHalf else"
                + " \"\",\n"
                + " \"lastNormalTime\": if exists r.lastNormalTime then r.lastNormalTime else"
                + " \"\",\n"
                + " \"lastFullTime\": if exists r.lastFullTime then r.lastFullTime else \"\",\n"
                + " \"lastGame\": if exists r.lastGame then r.lastGame else \"\",\n"
                + " \"lastSetOne\": if exists r.lastSetOne then r.lastSetOne else \"\",\n"
                + " \"lastSetTwo\": if exists r.lastSetTwo then r.lastSetTwo else \"\",\n"
                + " \"lastSets\": if exists r.lastSets then r.lastSets else \"\",\n"
                + " \"lastTieBreakOne\": if exists r.lastTieBreakOne then r.lastTieBreakOne else"
                + " \"\",\n"
                + " \"lastTieBreakTwo\": if exists r.lastTieBreakTwo then r.lastTieBreakTwo else"
                + " \"\",\n"
                + " \"count\": r.count'}'))",
            bucket,
            priceSource.getExchangeId().toUpperCase(),
            priceSource.getInstrumentId().toUpperCase(),
            additionalFilterClause,
            Rfc3339.YMDHMS_FORMATTER.format(
                ZonedDateTime.ofInstant(queryBetween.getStart(), ZoneOffset.UTC)),
            Rfc3339.YMDHMS_FORMATTER.format(
                ZonedDateTime.ofInstant(queryBetween.getEnd(), ZoneOffset.UTC)),
            aggWidthPeriod,
            measurementName,
            organisation);

    QueryReactiveApi queryApi = influxDbClient.getQueryReactiveApi();
    return Flux.from(queryApi.query(flux, AggregatedSportsEventScoreUpdateEntity.class));
  }

  /**
   * Aggregate match time updates into event aggregates.
   *
   * @param priceSource The source of prices
   * @param queryBetween A time window there series must have a data point within
   * @param measurementName the target measurment name
   * @param aggregateEventWidth the width of aggregated events
   * @param filter an optional filter for fields
   * @return A list of series
   */
  public Flux<AggregatedSportsEventMatchTimeUpdateEntity>
      aggregateSportsEventMatchTimeUpdatesBetween(
          PriceSource priceSource,
          QueryBetween queryBetween,
          String measurementName,
          Duration aggregateEventWidth,
          Optional<Pair<String, Set<String>>> filter) {
    String aggWidthPeriod = InfluxDbDurationFormatter.format(aggregateEventWidth);

    String additionalFilterClause =
        filter.isPresent() ? FluxStatementFilterBuilder.buildFrom(filter.get()) : "";

    String flux =
        MessageFormat.format(
            "from(bucket:\"{0}\")\n"
                + "|> range(start: {4}, stop: {5})\n"
                + "|> filter(fn: (r) => r._measurement == \"sports_event_match_time_update\" and\n"
                + "  r.exchangeId == \"{1}\" and\n"
                + "  r.instrumentId == \"{2}\")\n"
                + "|> pivot (rowKey:[\"_time\", \"exchangeId\", \"instrumentId\"], columnKey:"
                + " [\"_field\"], valueColumn: \"_value\")\n"
                + "{3}|> drop(columns: [\"_field\", \"_measurement\"])\n"
                + "|> sort(columns: [\"_time\"], desc: false)\n"
                + "|> window(every: {6})\n"
                + "|> reduce(fn: (r, accumulator) => ('{'\n"
                + " firstTime: if accumulator.count == 0 then string(v: r._time) else"
                + " accumulator.firstTime,\n"
                + " firstMatchTime: if accumulator.count == 0 and exists r.matchTime then"
                + " r.matchTime else accumulator.firstMatchTime,\n"
                + " firstRemainingTime: if accumulator.count == 0 and exists r.remainingTime then"
                + " r.remainingTime else accumulator.firstRemainingTime,\n"
                + " firstRemainingTimeInPeriod: if accumulator.count == 0 and exists"
                + " r.remainingTimeInPeriod then r.remainingTimeInPeriod else"
                + " accumulator.firstRemainingTimeInPeriod,\n"
                + " lastTime: string(v: r._time),\n"
                + " lastMatchTime: if exists r.matchTime then r.matchTime else \"\",\n"
                + " lastRemainingTime: if exists r.remainingTime then r.remainingTime else \"\",\n"
                + " lastRemainingTimeInPeriod: if exists r.remainingTimeInPeriod then"
                + " r.remainingTimeInPeriod else \"\",\n"
                + " count: accumulator.count + 1'}'),\n"
                + "identity: '{'\n"
                + " firstTime: \"1970-01-01T00:00:00Z\", firstMatchTime: \"\", firstRemainingTime:"
                + " \"\", firstRemainingTimeInPeriod: \"\", lastTime: \"1970-01-01T00:00:00Z\", "
                + " lastMatchTime: \"\", lastRemainingTime: \"\", lastRemainingTimeInPeriod: \"\","
                + " count: 0'}')\n"
                + "|> duplicate(column: \"_stop\", as: \"_time\")\n"
                + "|> set(key: \"_measurement\", value: \"{7}\")\n"
                + "|> to(\n"
                + "  bucket: \"{0}\",\n"
                + "  org: \"{8}\",\n"
                + "  tagColumns: [\"exchangeId\", \"instrumentId\"],\n"
                + "  fieldFn: (r) => ('{'\n"
                + " \"firstTime\": if exists r.firstTime then string(v: r.firstTime) else"
                + " \"1970-01-01T00:00:00Z\",\n"
                + " \"firstMatchTime\": if exists r.firstMatchTime then r.firstMatchTime else"
                + " \"\",\n"
                + " \"firstRemainingTime\": if exists r.firstRemainingTime then"
                + " r.firstRemainingTime else \"\",\n"
                + " \"firstRemainingTimeInPeriod\": if exists r.firstRemainingTimeInPeriod then"
                + " r.firstRemainingTimeInPeriod else \"\",\n"
                + " \"lastTime\": if exists r.lastTime then string(v: r.lastTime) else"
                + " \"1970-01-01T00:00:00Z\",\n"
                + " \"lastMatchTime\": if exists r.lastMatchTime then r.lastMatchTime else \"\",\n"
                + " \"lastRemainingTime\": if exists r.lastRemainingTime then r.lastRemainingTime"
                + " else \"\",\n"
                + " \"lastRemainingTimeInPeriod\": if exists r.lastRemainingTimeInPeriod then"
                + " r.lastRemainingTimeInPeriod else \"\",\n"
                + " \"count\": r.count'}'))",
            bucket,
            priceSource.getExchangeId().toUpperCase(),
            priceSource.getInstrumentId().toUpperCase(),
            additionalFilterClause,
            Rfc3339.YMDHMS_FORMATTER.format(
                ZonedDateTime.ofInstant(queryBetween.getStart(), ZoneOffset.UTC)),
            Rfc3339.YMDHMS_FORMATTER.format(
                ZonedDateTime.ofInstant(queryBetween.getEnd(), ZoneOffset.UTC)),
            aggWidthPeriod,
            measurementName,
            organisation);

    QueryReactiveApi queryApi = influxDbClient.getQueryReactiveApi();
    return Flux.from(queryApi.query(flux, AggregatedSportsEventMatchTimeUpdateEntity.class));
  }
}
