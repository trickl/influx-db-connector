package com.trickl.influxdb.client;

import com.trickl.flux.publishers.DifferentialPollPublisher;
import com.trickl.flux.publishers.FixedRateTimePublisher;
import com.trickl.model.pricing.exceptions.NoSuchInstrumentException;
import com.trickl.model.pricing.exceptions.ServiceUnavailableException;
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

    DifferentialPollPublisher<Candle> poller =
        new DifferentialPollPublisher<>(
            timePublisher.get(), (start, end) -> pollCandlesBetween(instrumentId, start, end));

    return poller.get();
  }

  private Publisher<Candle> pollCandlesBetween(String instrumentId, Instant start, Instant end) {
    try {
      return candleClient.findBetween(instrumentId, false, start, true, end, true, null);
    } catch (NoSuchInstrumentException | ServiceUnavailableException ex) {
      return Flux.error(ex);
    }
  }
}
