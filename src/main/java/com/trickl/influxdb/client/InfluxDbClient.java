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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import org.influxdb.impl.InfluxDBResultMapper;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

@Log
@RequiredArgsConstructor
public class InfluxDbClient {

  private static final int DEFAULT_CHUNK_SIZE = 100;

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
        measurements,
        IsoFields.WEEK_OF_WEEK_BASED_YEAR,
        databaseName,
        measurementClazz,
        timeAccessor);
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
                      if (time == null) {
                        return -1;
                      }
                      ZonedDateTime zonedTime = ZonedDateTime.ofInstant(time, ZoneOffset.UTC);
                      return zonedTime.get(batchField);
                    }));

    if (batchedMeasurements.containsKey(-1)) {
      String warningMessage = MessageFormat.format(
          "At least one record, e.g. {0} contains a invalid timestamp." 
          + " All such records will be ignored.", 
          batchedMeasurements.get(-1).get(0));
      log.warning(warningMessage);
      batchedMeasurements.remove(-1);
    }

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
      List<T> measurements, String databaseName, Class<T> measurementClazz) {
    return Flux.<Integer, InfluxDB>usingWhen(
        connectionProvider.getInfluxDb(),
        influxDb -> storeNoBatch(influxDb, measurements, databaseName, measurementClazz),
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
      Point point =
          Point.measurementByPOJO(measurementClazz).addFieldsFromPOJO(measurement).build();
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
    return Flux.<T, InfluxDB>usingWhen(
        connectionProvider.getInfluxDb(),
        influxDb ->
            findBetween(
                influxDb,
                priceSource,
                queryBetween,
                databaseName,
                measurementName,
                measurementClazz),
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
    String limitClause =
        queryBetween.getLimit() == null ? "" : (" LIMIT " + queryBetween.getLimit().toString());
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
    int chunkSize = Optional.ofNullable(queryBetween.getChunkSize()).orElse(DEFAULT_CHUNK_SIZE);

    return find(influxDb, query, chunkSize, measurementClazz);
  }    

  protected <T> Flux<T> find(
      InfluxDB influxDb,
      Query query,
      int chunkSize,  
      Class<T> measurementClazz) {

    DirectProcessor<T> processor = DirectProcessor.create();
    FluxSink<T> sink = processor.sink();
    
    try {
      InfluxDBResultMapper resultMapper = new InfluxDBResultMapper();
      influxDb.query(query, chunkSize, queryResult -> {
        if (queryResult.hasError()) {
          if ("DONE".equals(queryResult.getError())) {
            sink.complete();
          } else {
            sink.error(new NoSuchInstrumentException(queryResult.getError()));
          }
        } else {
          List<T> list = resultMapper.toPOJO(queryResult, measurementClazz);
          if (list.isEmpty()) {
            sink.complete();
          } else {
            list.forEach(sink::next);            
          }
        }
      });
    } catch (InfluxDBIOException ex) {
      log.log(Level.WARNING, ex.getMessage());
      sink.error(new ServiceUnavailableException("Error connecting to InfluxDB.", ex));
    }

    return processor;
  }

  /**
   * Find all available series that overlap a time window.
   *
   * @param queryBetween A time window there series must have a data point within
   * @param databaseName the name of the database
   * @param measurementName the name of the measurement
   * @return A list of series
   */
  public Flux<PriceSeries> findSeries(
      QueryBetween queryBetween, String databaseName, String measurementName) {
    return Flux.<PriceSeries, InfluxDB>usingWhen(
        connectionProvider.getInfluxDb(),
        influxDb -> findSeries(influxDb, queryBetween, databaseName, measurementName),
        influxDb -> Mono.empty());
  }

  protected Flux<PriceSeries> findSeries(
      InfluxDB influxDb, QueryBetween queryBetween, String databaseName, String measurementName) {
    String queryString =
        MessageFormat.format(
            "SHOW SERIES FROM \"{0}\" WHERE time {1} ''{2}''",
            measurementName,
            queryBetween.isEndIncl() ? "<=" : '<',
            Rfc3339.YMDHMS_FORMATTER.format(
                ZonedDateTime.ofInstant(queryBetween.getEnd(), ZoneOffset.UTC)));
    Query query = new Query(queryString, databaseName);
    try {
      QueryResult queryResult = influxDb.query(query);
      if (queryResult.hasError()) {
        return Flux.error(new NoSuchInstrumentException(queryResult.getError()));
      }

      List<PriceSeries> priceSeries = new LinkedList<>();
      queryResult.getResults().stream()
          .filter(
              internalResult ->
                  Objects.nonNull(internalResult) && Objects.nonNull(internalResult.getSeries()))
          .forEach(
              internalResult ->
                  internalResult.getSeries().stream()
                      .forEachOrdered(
                          series ->
                              series
                                  .getValues()
                                  .forEach(
                                      row ->
                                          row.stream()
                                              .findFirst()
                                              .ifPresent(
                                                  key ->
                                                      parseSeriesKey((String) key, priceSeries)))));

      return Flux.fromIterable(priceSeries);
    } catch (InfluxDBIOException ex) {
      log.log(Level.WARNING, ex.getMessage());
      return Flux.error(new ServiceUnavailableException("Error connecting to InfluxDB.", ex));
    }
  }

  protected void parseSeriesKey(String seriesKey, List<PriceSeries> priceSeries) {
    Map<String, String> tagMap =
        Arrays.asList(seriesKey.split(",")).stream()
            .skip(1)
            .map(keyValue -> keyValue.split("=", 2))
            .collect(
                Collectors.toMap(
                    keyValue -> keyValue.length > 0 ? keyValue[0] : null,
                    keyValue -> keyValue.length > 1 ? keyValue[1] : null));

    priceSeries.add(
        PriceSeries.builder()
            .priceSource(
                PriceSource.builder()
                    .instrumentId(tagMap.get("instrumentId"))
                    .exchangeId(tagMap.get("exchangeId"))
                    .build())
            .build());
  }
}
