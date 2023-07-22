package com.trickl.influxdb.client;

import com.influxdb.client.InfluxDBClient;
import com.trickl.influxdb.config.InfluxDbConfiguration;
import java.io.IOException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"unittest"})
@SpringBootTest(classes = InfluxDbConfiguration.class)
public class OrderBookClientTest {

  @Mock private InfluxDBClient influxDb;

  private InfluxDbStorage influxDbClient;

  private OrderBookClient orderBookClient;

  @BeforeEach
  public void setup() {}

  @AfterEach
  public void shutdown() throws IOException {}

  @Test
  public void testFindByIds() throws IOException {}
}
