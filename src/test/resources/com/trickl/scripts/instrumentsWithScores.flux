fieldFirstLastDuration = (measurement, field, start, stop) => {

  firstValue = from(bucket:"prices")
    |> range(start: start, stop: stop)
    |> filter(fn: (r) => r._measurement == measurement and r._field == field)
    |> group(columns: ["instrumentId", "exchangeId"])
    |> first()

  lastValue = from(bucket:"prices")
    |> range(start: start, stop: stop) 
    |> filter(fn: (r) => r._measurement == measurement and r._field == field)
    |> group(columns: ["instrumentId", "exchangeId"])
    |> last()

  return join( tables: {f:firstValue, l:lastValue}, on: ["exchangeId", "instrumentId"])
    |> map(fn: (r) => ({
      _time: r._time_l,
      duration: string(v: duration(v: uint(v: r._time_l) - uint(v: r._time_f))),
      first_value: r._value_f,
      last_value: r._value_l,
      exchangeId: r.exchangeId,
      instrumentId: r.instrumentId
    }))
}

fieldFirstLastDuration(measurement: "sports_event_score_update", field: "fullTime", start: 0, stop: 2021-12-26T18:36:51Z)
