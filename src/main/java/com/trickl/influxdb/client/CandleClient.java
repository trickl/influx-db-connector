package com.trickl.influxdb.client;

import com.trickl.influxdb.persistence.OhlcvBarEntity;
import com.trickl.influxdb.transformers.CandleReader;
import com.trickl.influxdb.transformers.CandleTransformer;
import com.trickl.model.pricing.primitives.Candle;
import com.trickl.model.pricing.primitives.PriceSource;

import java.util.List;
import java.util.stream.Collectors;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;

@RequiredArgsConstructor
public class CandleClient {

  private final InfluxDbClient influxDbClient;

  /**
  * Stores prices in the database.
  *
  * @param priceSource the instrument identifier
  * @param candles data to store
  */
  public Flux<Integer> store(PriceSource priceSource, List<Candle> candles) {
    CandleTransformer transformer = new CandleTransformer(priceSource);
    List<OhlcvBarEntity> measurements = candles.stream().map(transformer)
        .collect(Collectors.toList());
    return influxDbClient.store(measurements, "prices", OhlcvBarEntity.class,
        OhlcvBarEntity::getTime);
  }

  /**
   * Find candles.
   *
   * @param priceSource the instrument identifier
   * @param queryBetween Query parameters
   * @return A list of bars
   */
  public Flux<Candle> findBetween(PriceSource priceSource, QueryBetween queryBetween) {
    CandleReader reader = new CandleReader();
    return influxDbClient.findBetween(
        priceSource, queryBetween, "prices", "ohlvc_bar", OhlcvBarEntity.class)
        .map(reader);
  }
}
