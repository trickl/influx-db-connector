package com.trickl.smarkets.client;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trickl.influxdb.client.CandleClient;
import com.trickl.influxdb.config.InfluxDbConfiguration;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;

import org.influxdb.InfluxDB;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@RunWith(SpringRunner.class)
@ActiveProfiles({"unittest"})
@SpringBootTest(classes = InfluxDbConfiguration.class)
public class CandleClientTest {

  @Mock
  private InfluxDB influxDb;

  private Mono<InfluxDB> influxDbConnection;

  private CandleClient candleClient;

  @BeforeEach
  private void setup() {
    influxDbConnection = Mono.just(influxDb);        
    candleClient = new CandleClient(influxDbConnection);
  }

  @AfterEach
  private void shutdown() throws IOException {
  }

  @Test
  public void testFindByIds() throws IOException {
  }
}
