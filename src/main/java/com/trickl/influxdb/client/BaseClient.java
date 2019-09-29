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

  private final Mono<InfluxDB> influxDbConnection;

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

    // Write them to InfluxDB
    influxDbConnection.block().write(batchPoints);
  }

  /**
   * Find prices in the database.
   *
   * @param instrumentId Instrument Id
   * @param startIncl if start date is inclusive
   * @param start Start date
   * @param endIncl if end date is inclusive
   * @param end End date
   * @param ascending true if later prices last
   * @param limit if non null, maximum number of results
   * @return A list of bars
   * @throws NoSuchInstrumentException if the instrument has no data
   * @throws ServiceUnavailableException if the database is not available
   */
  public Flux<T> findBetween(
      String instrumentId,
      boolean startIncl,
      Instant start,
      boolean endIncl,
      Instant end,
      boolean ascending,
      Long limit)
      throws NoSuchInstrumentException, ServiceUnavailableException {
    String orderByClause = ascending ? " ORDER BY time ASC" : "ORDER BY time DESC";
    String limitClause = limit == null ? "" : (" LIMIT " + limit.toString());
    String queryString =
        MessageFormat.format(
            "SELECT * FROM {0} WHERE instrument = ''{1}''"
                + " AND time {2} ''{3}'' AND time {4} ''{5}''{6}{7}",
            getMeasurementName(),
            instrumentId,
            startIncl ? ">=" : '>',
            Rfc3339.YMDHMS_FORMATTER.format(ZonedDateTime.ofInstant(start, ZoneOffset.UTC)),
            endIncl ? "<=" : '<',
            Rfc3339.YMDHMS_FORMATTER.format(ZonedDateTime.ofInstant(end, ZoneOffset.UTC)),
            orderByClause,
            limitClause);
    Query query = new Query(queryString, getDatabaseName());

    try {
      QueryResult queryResult = influxDbConnection.block().query(query);
      if (queryResult.hasError()) {
        throw new NoSuchInstrumentException(queryResult.getError());
      }

      Result result = queryResult.getResults().stream().findFirst().get();
      return parseResult(instrumentId, result);
    } catch (InfluxDBIOException ex) {
      log.log(Level.WARNING, ex.getMessage());
      throw new ServiceUnavailableException("Error connecting to InfluxDB.", ex);
    }
  }

  private Flux<T> parseResult(String instrumentId, Result result) {
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
