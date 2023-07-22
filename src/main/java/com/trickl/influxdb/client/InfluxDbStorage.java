package com.trickl.influxdb.client;

import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.reactive.InfluxDBClientReactive;
import com.influxdb.client.reactive.WriteReactiveApi;
import com.trickl.influxdb.persistence.AnalyticPrimitiveValueEntity;
import jakarta.validation.Valid;
import java.text.MessageFormat;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalField;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

@Log
@RequiredArgsConstructor
public class InfluxDbStorage {

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
    if (measurements.isEmpty()) {
      return Mono.just(0);
    }

    WriteReactiveApi writeApi = influxDbClient.getWriteReactiveApi();
    return Mono.from(writeApi.writeMeasurements(WritePrecision.MS, Flux.fromIterable(measurements)))
        .onErrorResume(
            error -> {
              log.warning(
                  MessageFormat.format("Failed to store {0} measurements", measurements.size()));
              String analyticName = "unknown";
              if (measurements.get(0) instanceof AnalyticPrimitiveValueEntity) {
                analyticName =
                    ((AnalyticPrimitiveValueEntity) measurements.get(0)).getAnalyticName();
              }
              log.warning(
                  MessageFormat.format(
                      "Analytic: {0} First measurement: {1}", analyticName, measurements.get(0)));
              return Mono.error(error);
            })
        .map(
            result -> {
              return measurements.size();
            })
        .log("StoreNoBatch", Level.WARNING, SignalType.ON_ERROR);
  }
}
