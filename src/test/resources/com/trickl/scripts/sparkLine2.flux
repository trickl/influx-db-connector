calcAverages = (start, stop) => {
  lhs = from(bucket:"prices")
  |> range(start: start, stop: stop)
  |> filter(fn: (r) => r._measurement == "order" and r._field == "price" and r.exchangeId == "SMARKETS" and r.instrumentId == "47202945-136383105" and r.depth == "0" and r.bidOrAsk == "B")
  |> group(columns: ["instrumentId", "exchangeId"])
  |> window(every: 5m)
  |> median()

  rhs = from(bucket:"prices")
    |> range(start: start, stop: stop)
    |> filter(fn: (r) => r._measurement == "order" and r._field == "price" and r.exchangeId == "SMARKETS" and r.instrumentId == "47202945-136383105" and r.depth == "0" and r.bidOrAsk == "A")
    |> group(columns: ["instrumentId", "exchangeId"])
  |> window(every: 5m)
  |> median()

  return join( tables: {f:lhs, l:rhs}, on: ["exchangeId", "instrumentId"])
    |> map(fn: (r) => ({

      _time: r._stop,
      _value: (r._value_l + r._value_f) / 2.0,
      exchangeId: r.exchangeId,
      instrumentId: r.instrumentId
   }))
}

calcAverages(start: 2021-07-22T18:38:00Z, stop: 2023-07-22T18:38:00Z) 

