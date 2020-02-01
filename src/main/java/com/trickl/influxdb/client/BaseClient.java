package com.trickl.influxdb.client;

import com.trickl.influxdb.text.Rfc3339;
import com.trickl.model.pricing.exceptions.NoSuchInstrumentException;
import com.trickl.model.pricing.exceptions.ServiceUnavailableException;

import java.text.MessageFormat;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalField;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.stream.Collectors;
import javax.validation.Valid;

import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBIOException;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Log
@RequiredArgsConstructor
public abstract class BaseClient<T> {

  protected final InfluxDbClient influxDbClient;

  /**
   * Stores prices in the database.
   *
   * @param instrumentId Instrument Id
   * @param measurements data to store
   */
  @Valid
  public void store(String instrumentId, List<T> measurements) {
    storeBatchedByTime(instrumentId, measurements, IsoFields.WEEK_OF_WEEK_BASED_YEAR);
  }

  /**
   * Stores prices in the database.
   *
   * @param instrumentId Instrument Id
   * @param measurements data to store
   * @param batchField the temporal field to batch prices by
   */
  @Valid
  public void storeBatchedByTime(
      String instrumentId, List<T> measurements, TemporalField batchField) {
    Map<Integer, List<T>> batchedMeasurements =
        measurements.stream()
            .collect(
                Collectors.groupingBy(
                    measurement -> {
                      Instant time = getTimeAccessor().apply(measurement);
                      ZonedDateTime zonedTime = ZonedDateTime.ofInstant(time, ZoneOffset.UTC);
                      return zonedTime.get(batchField);
                    }));
    batchedMeasurements.values().forEach(measurement -> storeNoBatch(instrumentId, measurement));
  }

  /**
   * Stores prices in the database.
   *
   * @param instrumentId Instrument Id
   * @param measurements data to store
   */
  @Valid
  public void storeNoBatch(String instrumentId, List<T> measurements) {
    Flux.<Integer, InfluxDB>usingWhen(influxDbClient.getInfluxDb(), influxDb -> 
        storeNoBatch(influxDb, instrumentId, measurements), influxDb -> Mono.empty());
  }

  protected Mono<Integer> storeNoBatch(
      InfluxDB influxDb, String instrumentId, List<T> measurements) {
    BatchPoints batchPoints =
        BatchPoints.database(getDatabaseName())
            .tag("async", "true")
            .retentionPolicy("autogen")
            .precision(TimeUnit.MILLISECONDS)
            .consistency(InfluxDB.ConsistencyLevel.ALL)
            .build();

    for (T measurement : measurements) {
      Instant time = getTimeAccessor().apply(measurement);
      Point.Builder builder =
          Point.measurement(getMeasurementName())
              .tag("instrument", instrumentId)
              .time(time.toEpochMilli(), TimeUnit.MILLISECONDS);
      addFields(measurement, builder);
      Point point = builder.build();
      batchPoints.point(point);
    }

    influxDb.write(batchPoints);

    return Mono.just(measurements.size());
  }

  /**
   * Find prices in the database.
   *
   * @param queryBetween Query parameters
   * @return A list of bars
   */
  public Flux<T> findBetween(QueryBetween queryBetween) {
    return Flux.<T, InfluxDB>usingWhen(influxDbClient.getInfluxDb(), influxDb -> 
      findBetween(influxDb, queryBetween),
      influxDb -> Mono.empty());
  }

  protected Flux<T> findBetween(InfluxDB influxDb, QueryBetween queryBetween) {
    String orderByClause = queryBetween.isAscending() ? " ORDER BY time ASC" : "ORDER BY time DESC";
    String limitClause = queryBetween.getLimit() == null 
        ? "" : (" LIMIT " + queryBetween.getLimit().toString());
    String queryString =
        MessageFormat.format(
            "SELECT * FROM {0} WHERE instrument = ''{1}''"
                + " AND time {2} ''{3}'' AND time {4} ''{5}''{6}{7}",
            getMeasurementName(),
            queryBetween.getInstrumentId(),
            queryBetween.isStartIncl() ? ">=" : '>',
            Rfc3339.YMDHMS_FORMATTER.format(
              ZonedDateTime.ofInstant(queryBetween.getStart(), ZoneOffset.UTC)),
              queryBetween.isEndIncl() ? "<=" : '<',
            Rfc3339.YMDHMS_FORMATTER.format(
              ZonedDateTime.ofInstant(queryBetween.getEnd(), ZoneOffset.UTC)),
            orderByClause,
            limitClause);

    Query query = new Query(queryString, getDatabaseName());

    try {
      QueryResult queryResult = influxDb.query(query);
      if (queryResult.hasError()) {
        return Flux.error(new NoSuchInstrumentException(queryResult.getError()));
      }

      Result result = queryResult.getResults().stream().findFirst().get();
      return parseResult(queryBetween.getInstrumentId(), result);
    } catch (InfluxDBIOException ex) {
      log.log(Level.WARNING, ex.getMessage());
      return Flux.error(new ServiceUnavailableException("Error connecting to InfluxDB.", ex));
    }
  }
  
  protected Flux<T> parseResult(String instrumentId, Result result) {
    List<Series> allSeries = result.getSeries();
    if (allSeries == null) {
      return Flux.empty();
    }

    Optional<Series> firstSeries = result.getSeries().stream().findFirst();

    if (!firstSeries.isPresent()) {
      return Flux.empty();
    }

    Series series = firstSeries.get();
    List<T> measurements = new ArrayList<>(series.getValues().size());
    List<String> columns = series.getColumns();
    int timeIndex = columns.indexOf("time");
    List<Integer> columnIndices =
        getColumnNames().stream()
            .map(columns::indexOf)
            .collect(Collectors.toList());
    series
        .getValues()
        .forEach(
            data -> {
              Instant time =
                  Rfc3339.YMDHMS_FORMATTER.parse((String) data.get(timeIndex), Instant::from);
              measurements.add(decodeFromDatabase(instrumentId, time, columnIndices, data));
            });

    return Flux.fromIterable(measurements);
  }

  public abstract Function<T, Instant> getTimeAccessor();

  protected abstract String getDatabaseName();

  protected abstract String getMeasurementName();

  protected abstract List<String> getColumnNames();

  protected abstract void addFields(T measurement, Point.Builder builder);

  protected abstract T decodeFromDatabase(
      String instrumentId, Instant time, List<Integer> columnIndexes, List<Object> columnData);
}
