package com.trickl.influxdb.client;

import com.trickl.influxdb.text.Rfc3339;
import com.trickl.model.pricing.exceptions.NoSuchInstrumentException;
import com.trickl.model.pricing.exceptions.ServiceUnavailableException;
import com.trickl.model.pricing.primitives.PriceSource;

import java.text.MessageFormat;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalField;
import java.util.List;
import java.util.Map;
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
import org.influxdb.impl.InfluxDBResultMapper;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Log
@RequiredArgsConstructor
public class InfluxDbClient {

  protected final ConnectionProvider connectionProvider;

  /**
   * Stores prices in the database.
   *
   * @param measurements data to store
   */
  @Valid
  public <T> Flux<Integer> store(
      List<T> measurements,
      String databaseName,
      Class<T> measurementClazz,
      Function<T, Instant> timeAccessor) {
    return storeBatchedByTime(
        measurements, IsoFields.WEEK_OF_WEEK_BASED_YEAR,
        databaseName, measurementClazz, timeAccessor);
  }

  /**
   * Stores prices in the database.
   *
   * @param measurements data to store
   * @param batchField the temporal field to batch prices by
   */
  @Valid
  public <T> Flux<Integer> storeBatchedByTime(
      List<T> measurements, 
      TemporalField batchField,
      String databaseName,
      Class<T> measurementClazz,
      Function<T, Instant> timeAccessor) {
    Map<Integer, List<T>> batchedMeasurements =
        measurements.stream()
            .collect(
                Collectors.groupingBy(
                    measurement -> {
                      Instant time = timeAccessor.apply(measurement);
                      ZonedDateTime zonedTime = ZonedDateTime.ofInstant(time, ZoneOffset.UTC);
                      return zonedTime.get(batchField);
                    }));
    return Flux.merge(
      Flux.fromIterable(batchedMeasurements.values())
          .map(measurement -> storeNoBatch(measurement, databaseName, measurementClazz)));
  }

  /**
   * Stores prices in the database.
   *
   * @param measurements data to store
   */
  @Valid
  public <T> Flux<Integer> storeNoBatch(
      List<T> measurements, 
      String databaseName,
      Class<T> measurementClazz) {
    return Flux.<Integer, InfluxDB>usingWhen(connectionProvider.getInfluxDb(), influxDb -> 
        storeNoBatch(influxDb, measurements, databaseName, measurementClazz),
        influxDb -> Mono.empty());
  }

  protected <T> Mono<Integer> storeNoBatch(
      InfluxDB influxDb, List<T> measurements, String databaseName, Class<T> measurementClazz) {
    BatchPoints batchPoints =
        BatchPoints.database(databaseName)
            .tag("async", "true")
            .retentionPolicy("autogen")
            .precision(TimeUnit.MILLISECONDS)
            .consistency(InfluxDB.ConsistencyLevel.ALL)
            .build();

    for (T measurement : measurements) {
      Point point = Point.measurementByPOJO(measurementClazz)
          .addFieldsFromPOJO(measurement).build();
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
  public <T> Flux<T> findBetween(
      PriceSource priceSource,
      QueryBetween queryBetween,
      String databaseName,      
      String measurementName,
      Class<T> measurementClazz) {
    return Flux.<T, InfluxDB>usingWhen(connectionProvider.getInfluxDb(), influxDb -> 
      findBetween(influxDb,
           priceSource, queryBetween, databaseName, measurementName, measurementClazz),
      influxDb -> Mono.empty());
  }

  protected <T> Flux<T> findBetween(
      InfluxDB influxDb,
      PriceSource priceSource,
      QueryBetween queryBetween,
      String databaseName,      
      String measurementName,
      Class<T> measurementClazz) {
    String orderByClause = queryBetween.isAscending() ? " ORDER BY time ASC" : "ORDER BY time DESC";
    String limitClause = queryBetween.getLimit() == null 
        ? "" : (" LIMIT " + queryBetween.getLimit().toString());
    String queryString =
        MessageFormat.format(
            "SELECT * FROM \"{0}\" WHERE exchangeId = ''{1}'' AND instrumentId = ''{2}''"
                + " AND time {3} ''{4}'' AND time {5} ''{6}'' {7}{8}",
            measurementName,
            priceSource.getExchangeId(),
            priceSource.getInstrumentId(),
            queryBetween.isStartIncl() ? ">=" : '>',
            Rfc3339.YMDHMS_FORMATTER.format(
              ZonedDateTime.ofInstant(queryBetween.getStart(), ZoneOffset.UTC)),
              queryBetween.isEndIncl() ? "<=" : '<',
            Rfc3339.YMDHMS_FORMATTER.format(
              ZonedDateTime.ofInstant(queryBetween.getEnd(), ZoneOffset.UTC)),
            orderByClause,
            limitClause);

    Query query = new Query(queryString, databaseName);

    try {
      QueryResult queryResult = influxDb.query(query);
      if (queryResult.hasError()) {
        return Flux.error(new NoSuchInstrumentException(queryResult.getError()));
      }

      InfluxDBResultMapper resultMapper = new InfluxDBResultMapper();
      List<T> list = resultMapper.toPOJO(queryResult, measurementClazz);
      return Flux.fromIterable(list);
    } catch (InfluxDBIOException ex) {
      log.log(Level.WARNING, ex.getMessage());
      return Flux.error(new ServiceUnavailableException("Error connecting to InfluxDB.", ex));
    }
  }
}
