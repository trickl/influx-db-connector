package com.trickl.influxdb.client;

import com.influxdb.client.reactive.InfluxDBClientReactive;
import com.trickl.influxdb.binding.VolumeUpdateReader;
import com.trickl.influxdb.binding.VolumeUpdateWriter;
import com.trickl.influxdb.persistence.VolumeUpdateEntity;
import com.trickl.model.event.VolumeUpdate;
import com.trickl.model.pricing.primitives.EventSource;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.statistics.PriceSourceFieldFirstLastDuration;
import com.trickl.model.pricing.statistics.PriceSourceInteger;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class VolumeUpdateClient {

  private static final String MEASUREMENT = "volume";

  private final InfluxDBClientReactive influxDbClient;

  private final String bucket;

  /**
   * Stores traded volumes in the database.
   *
   * @param priceSource the instrument identifier
   * @param events data to store
   * @return counts of records stored
   */
  public Flux<Integer> store(PriceSource priceSource, List<VolumeUpdate> events) {
    VolumeUpdateWriter transformer = new VolumeUpdateWriter(priceSource);
    List<VolumeUpdateEntity> measurements =
        events.stream().map(transformer).collect(Collectors.toList());
    InfluxDbStorage influxDbStorage = new InfluxDbStorage(influxDbClient, bucket);
    return influxDbStorage.store(
        measurements, VolumeUpdateEntity.class, VolumeUpdateEntity::getTime);
  }

  /**
   * Find traded volumes.
   *
   * @param eventSource the instrument identifier
   * @param queryBetween Query parameters
   * @return The traded volumes in time order
   */
  public Flux<VolumeUpdate> findBetween(EventSource eventSource, QueryBetween queryBetween) {
    VolumeUpdateReader reader = new VolumeUpdateReader();
    InfluxDbFindBetween findBetween = new InfluxDbFindBetween(influxDbClient, bucket);
    return findBetween
        .findBetween(
            eventSource.getPriceSource(),
            queryBetween,
            MEASUREMENT,
            VolumeUpdateEntity.class,
            Collections.emptyMap())
        .map(reader);
  }

  /**
   * Find a summary of traded volumes between a period of time.
   *
   * @param queryBetween A time window there series must have a data point within
   * @param priceSource The price source
   * @return The first and last value, and the duration between them
   */
  public Mono<PriceSourceFieldFirstLastDuration> firstLastDuration(
      QueryBetween queryBetween, PriceSource priceSource) {
    InfluxDbFirstLastDuration finder = new InfluxDbFirstLastDuration(this.influxDbClient, bucket);
    return finder.firstLastDuration(queryBetween, MEASUREMENT, "volume", priceSource);
  }

  /**
   * Find a count of traded volumes between a period of time.
   *
   * @param queryBetween A time window there series must have a data point within
   * @param priceSource The price source
   * @return The count
   */
  public Mono<Integer> count(QueryBetween queryBetween, PriceSource priceSource) {
    InfluxDbCount influxDbCount = new InfluxDbCount(this.influxDbClient, bucket);
    return influxDbCount
        .count(queryBetween, MEASUREMENT, "volume", priceSource)
        .map(PriceSourceInteger::getValue);
  }
}
