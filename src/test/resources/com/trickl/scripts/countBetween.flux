myfunc = (measurement, field, start, stop) => {
  return from(bucket:"prices")
  |> range(start: start, stop: stop)
  |> filter(fn: (r) => r._measurement == measurement and r._field == field and r.exchangeId == "SMARKETS" and r.instrumentId == "47202945-136383105" and r.depth == "0")
  |> group(columns: ["instrumentId", "exchangeId"])
  |> count()
}

myfunc(measurement: "order", field: "price", start: 2021-07-16T20:19:58Z, stop: 2023-07-16T20:19:58Z)
