fieldFirstLastDuration = (measurement, field, start, stop) => {
  firstValue = from(bucket:"prices")
  |> range(start: start, stop: stop)
  |> filter(fn: (r) => r._measurement == measurement and r._field == field and r.exchangeId == "SMARKETS" and r.instrumentId == "47202945-136383105")
  |> group(columns: ["instrumentId", "exchangeId"])
  |> first()
  |> toString()

  lastValue = from(bucket:"prices")
    |> range(start: start, stop: stop)
    |> filter(fn: (r) => r._measurement == measurement and r._field == field and r.exchangeId == "SMARKETS" and r.instrumentId == "47202945-136383105")
    |> group(columns: ["instrumentId", "exchangeId"])
    |> last()
    |> toString()

  return join.inner( tables: {f:firstValue, l:lastValue}, on: ["exchangeId", "instrumentId"])
    |> map(fn: (r) => ({

      _time: r._time_l,
      duration: string(v: duration(v: uint(v: r._time_l) - uint(v: r._time_f))),
      first: r._value_f,
      last: r._value_l,
      exchangeId: r.exchangeId,
      instrumentId: r.instrumentId
   }))
}

fieldFirstLastDuration(measurement: "order", field: "price", start: 2021-07-16T20:19:58Z, stop: 2023-07-16T20:19:58Z)
