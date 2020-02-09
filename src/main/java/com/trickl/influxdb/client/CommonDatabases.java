package com.trickl.influxdb.client;

import lombok.Getter;

public enum CommonDatabases {
  PRICES("prices");

  @Getter
  private String name;

  private CommonDatabases(String name) {
    this.name = name;
  }
}