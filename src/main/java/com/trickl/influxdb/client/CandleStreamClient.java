package com.trickl.influxdb.client;

import com.trickl.flux.mappers.DifferentialMapper;
import com.trickl.flux.publishers.FixedRateTimePublisher;
import com.trickl.model.pricing.primitives.Candle;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

@RequiredArgsConstructor
public class CandleStreamClient {

  private final CandleClient candleClient;

  private final Supplier<Instant> timeSupplier;

  private final Duration pollPeriod;

  /**
   * Get a live stream of prices for an instrument.
   *
   * @param instrumentId Instrument identifier
   * @return A stream of candlesticks
   */
  public Flux<Candle> get(String instrumentId) {

    FixedRateTimePublisher timePublisher =
        new FixedRateTimePublisher(Duration.ZERO, pollPeriod, timeSupplier, Schedulers.parallel());

    return timePublisher.get().flatMap(new DifferentialMapper<Instant, Candle>(
        (start, end) -> pollCandlesBetween(instrumentId, start, end), null));    
  }

  private Publisher<Candle> pollCandlesBetween(String instrumentId, Instant start, Instant end) {
    QueryBetween.QueryBetweenBuilder queryBuilder = QueryBetween.builder();
    queryBuilder.instrumentId(instrumentId);
    queryBuilder.startIncl(false);
    queryBuilder.endIncl(true);
    if (start != null) {
      queryBuilder.start(start);
    }
    if (end != null) {
      queryBuilder.end(end);
    }

    return candleClient.findBetween(queryBuilder.build());  
  }
}