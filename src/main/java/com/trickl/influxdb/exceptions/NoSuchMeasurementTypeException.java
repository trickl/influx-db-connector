package com.trickl.influxdb.exceptions;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(code = HttpStatus.NOT_FOUND, reason = "Measurement Type Not Found")
public class NoSuchMeasurementTypeException extends Exception {

  private static final long serialVersionUID = -2618855214071451042L;

  /**
   * Construct a {@link MeasurementNotSupportedException} with a generic message.
   *
   * @param msg the message
   */
  public NoSuchMeasurementTypeException(String msg) {
    super(msg);
  }

  /**
   * Construct a {@link MeasurementNotSupportedException} with a generic message and a cause.
   *
   * @param msg the message
   * @param cause the cause of the exception
   */
  public NoSuchMeasurementTypeException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
