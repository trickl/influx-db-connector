package com.trickl.influxdb.client;

import lombok.RequiredArgsConstructor;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class ConnectionProvider {

  private final String url; 

  private final String username;

  private final String password;

  private Mono<InfluxDB> dbCache = null;

  /**
   * Get the session token.
   *
   * @return A valid session token
   */
  private Mono<InfluxDB> getConnection() {
    return Mono.just(InfluxDBFactory.connect(url, username, password));
  }

  /**
   * Get a influxdb instance, making use of a cached value if one exists.
   *
   * @return An influxdb instance
   */
  public Mono<InfluxDB> getInfluxDb() {
    return getInfluxDbUsingCache(false);
  }

  protected Flux<InfluxDB> getDbReconnect() {
    return getInfluxDbUsingCache(true).flux();
  }

  private Mono<InfluxDB> getInfluxDbUsingCache(boolean reconnect) {
    if (dbCache == null || reconnect) {
      dbCache =
        getConnection()              
        .flux()
        .cache(1)
        .filter(db -> db.ping().isGood())
        .concatWith(Flux.defer(this::getDbReconnect)) 
        .limitRequest(1)     
        .last();
    }
    return dbCache;
  }

  
}
