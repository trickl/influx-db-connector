package com.trickl.influxdb.client;

import com.trickl.influxdb.client.CandleStreamClient;
import com.trickl.influxdb.config.InfluxDbConfiguration;
import java.io.IOException;

import org.influxdb.InfluxDB;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import lombok.extern.java.Log;

@RunWith(SpringRunner.class)
@ActiveProfiles({ "unittest" })
@SpringBootTest(classes = InfluxDbConfiguration.class)
@Log
public class CandleStreamClientTest {

  @Mock
  private InfluxDB influxDb;

  private InfluxDbClient influxDbClient;

  private CandleStreamClient candleStreamClient;

  @BeforeEach
  private void setup() {
  }

  @AfterEach
  private void shutdown() throws IOException, InterruptedException {
  }

  @Test
  public void testGet() throws IOException, InterruptedException {

  }
}
