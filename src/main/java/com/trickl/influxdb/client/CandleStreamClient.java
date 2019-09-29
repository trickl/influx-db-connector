package com.trickl.influxdb.client;

import com.trickl.model.pricing.primitives.Candle;
import reactor.core.publisher.Flux;

public class CandleStreamClient {

  /**
   * Get a live stream of prices for an instrument.
   *
   * @param instrumentId Instrument identifier
   * @return A stream of candlesticks
   */
  public Flux<Candle> get(String instrumentId) {
    return null;
  }
}
