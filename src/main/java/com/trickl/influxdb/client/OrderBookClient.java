package com.trickl.influxdb.client;

import com.trickl.model.pricing.primitives.Order;
import com.trickl.model.pricing.primitives.OrderBook;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.primitives.Quote;
import com.trickl.model.pricing.statistics.PriceSourceFieldFirstLastDuration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class OrderBookClient {

  private final OrderClient orderClient;

  /**
   * Stores prices in the database.
   *
   * @param priceSource the instrument identifier
   * @param orderBooks data to store
   * @return counts of records stored
   */
  public Flux<Integer> store(PriceSource priceSource, List<OrderBook> orderBooks) {

    Mono<Flux<Integer>> storeBids =
        Flux.fromIterable(orderBooks)
            .flatMap(
                orderBook -> Flux.fromIterable(
                    getOrders(orderBook.getBids(), true, orderBook.getTime())))
            .collectList()
            .map(list -> orderClient.store(priceSource, list));

    Mono<Flux<Integer>> storeAsks =
        Flux.fromIterable(orderBooks)
            .flatMap(
                orderBook -> Flux.fromIterable(
                    getOrders(orderBook.getAsks(), false, orderBook.getTime())))
            .collectList()
            .map(list -> orderClient.store(priceSource, list));

    return Flux.merge(storeBids, storeAsks).flatMap(rows -> rows);
  }

  /**
   * Find candles.
   *
   * @param priceSource the instrument identifier
   * @param queryBetween Query parameters
   * @return A list of order books
   */
  public Flux<OrderBook> findBetween(PriceSource priceSource, QueryBetween queryBetween) {
    return orderClient
        .findBetween(priceSource, queryBetween)
        .windowUntilChanged(Order::getTime)
        .flatMap(OrderBookClient::getOrderBook);
  }

  protected static Mono<OrderBook> getOrderBook(Flux<Order> orderFlux) {
    return orderFlux.collectList()
      .map(orders -> 
          OrderBook.builder()
            .bids(getQuotes(orders, true))
            .asks(getQuotes(orders, false))
            .time(orders.get(0).getTime())
            .build());       
  }

  protected static List<Order> getOrders(List<Quote> quotes, boolean isBid, Instant time) {
    return IntStream.range(0, quotes.size())
      .mapToObj(depth -> Order.builder().quote(
          quotes.get(depth)).isBid(isBid).time(time).depth(depth).build())
      .collect(Collectors.toList());
  }


  protected static List<Quote> getQuotes(List<Order> orders, boolean isBid) {
    return orders.stream().filter(quote -> quote.isBid() == isBid)
        .sorted((a, b) -> a.getDepth() - b.getDepth())
        .map(Order::getQuote).collect(Collectors.toList());
  }

  /**
   * Find all available series that overlap a time window.
   *
   * @param queryBetween A time window there series must have a data point within
   * @param priceSource The price source
   * @return A list of series
   */
  public Flux<PriceSourceFieldFirstLastDuration> findSummary(
      QueryBetween queryBetween, PriceSource priceSource) {
    return orderClient.findSummary(queryBetween, priceSource);
  }
}
