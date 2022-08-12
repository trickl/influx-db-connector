package com.trickl.influxdb.client;

import com.trickl.influxdb.binding.CandleReader;
import com.trickl.influxdb.binding.CandleWriter;
import com.trickl.influxdb.persistence.OhlcvBarEntity;
import com.trickl.model.pricing.primitives.Candle;
import com.trickl.model.pricing.primitives.CandleSource;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.statistics.PriceSourceFieldFirstLastDuration;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;

@RequiredArgsConstructor
public class CandleClient {

  private final InfluxDbAdapter influxDbAdapter;

  private final InfluxDbAggregator influxDbAggregator;

  /**
   * Stores prices in the database.
   *
   * @param candleSource the instrument identifier
   * @param candles data to store
   * @return The stored candles
   */
  public Flux<Integer> store(CandleSource candleSource, List<Candle> candles) {
    CandleWriter transformer = new CandleWriter(candleSource.getPriceSource());
    List<OhlcvBarEntity> measurements =
        candles.stream().map(transformer).collect(Collectors.toList());
    return influxDbAdapter.store(
        measurements,
        OhlcvBarEntity.class,
        OhlcvBarEntity::getTime);
  }

  /**
   * Find candles.
   *
   * @param candleSource the candle source
   * @param queryBetween Query parameters
   * @return A list of bars
   */
  public Flux<Candle> findBetween(CandleSource candleSource, QueryBetween queryBetween) {
    CandleReader reader = new CandleReader();
    return influxDbAdapter
        .findBetween(
            candleSource.getPriceSource(),
            queryBetween,
            candleSource.getCandleName(),
            OhlcvBarEntity.class)
        .map(reader);
  }

  /**
   * Aggregate bid orders into candles.
   *
   * @param priceSource the instrument identifier
   * @param queryBetween Query parameters
   * @param candleWidth candleWidth
   * @return A list of bars
   */
  public Flux<Candle> aggregateBestBidsBetween(
      PriceSource priceSource, 
      QueryBetween queryBetween,
      Duration candleWidth) {
    String candleWidthPeriod = InfluxDbDurationFormatter.format(candleWidth);
    CandleReader reader = new CandleReader();
    return influxDbAggregator
        .aggregateBestBidOrAskBetween(
            priceSource,
            queryBetween,
            "best_bid_" + candleWidthPeriod,
            true,
            Duration.ofMinutes(1))
        .map(reader);
  }

  /**
   * Aggregate ask orders into candles.
   *
   * @param priceSource the instrument identifier
   * @param queryBetween Query parameters
   * @param candleWidth candleWidth
   * @return A list of bars
   */
  public Flux<Candle> aggregateBestAsksBetween(
      PriceSource priceSource, 
      QueryBetween queryBetween,
       Duration candleWidth) {
    String candleWidthPeriod = InfluxDbDurationFormatter.format(candleWidth);
    CandleReader reader = new CandleReader();
    return influxDbAggregator
        .aggregateBestBidOrAskBetween(
            priceSource,
            queryBetween,
            "best_ask_" + candleWidthPeriod,
            false,
            Duration.ofMinutes(1))
        .map(reader);
  }

  /**
   * Find a summary of price updates between a period of time, grouped by instrument.
   *
   * @param queryBetween A time window there series must have a data point within
   * @param candleName Candle name
   * @param priceSource The price source
   * @return A list of series, including the first and last value of a field
   */
  public Flux<PriceSourceFieldFirstLastDuration> findSummary(
      QueryBetween queryBetween, String candleName, PriceSource priceSource) {
    return influxDbAdapter.findFieldFirstLastCountByDay(
        queryBetween, candleName, "close", priceSource);
  }
}
