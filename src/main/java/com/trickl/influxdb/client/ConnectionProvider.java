package com.trickl.influxdb.client;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.domain.HealthCheck.StatusEnum;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class ConnectionProvider {

  private final String url;

  private final String token;

  private final String org;

  private final String bucket;

  private Mono<InfluxDBClient> dbCache = null;

  /**
   * Get the session token.
   *
   * @return A valid session token
   */
  private Mono<InfluxDBClient> getConnection() {
    return Mono.just(InfluxDBClientFactory.create(url, token.toCharArray(), org, bucket));
  }

  /**
   * Get a influxdb instance, making use of a cached value if one exists.
   *
   * @return An influxdb instance
   */
  public Mono<InfluxDBClient> getInfluxDb() {
    return getInfluxDbUsingCache(false);
  }

  protected Flux<InfluxDBClient> getDbReconnect() {
    return getInfluxDbUsingCache(true).flux();
  }

  private Mono<InfluxDBClient> getInfluxDbUsingCache(boolean reconnect) {
    if (dbCache == null || reconnect) {
      dbCache =
          getConnection()
              .flux()
              .cache(1)
              .filter(db -> !db.health().getStatus().equals(StatusEnum.FAIL))
              .concatWith(Flux.defer(this::getDbReconnect))
              .limitRequest(1)
              .last();
    }
    return dbCache;
  }
}
