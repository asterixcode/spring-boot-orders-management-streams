package com.learnkafkastreams.topology;

import static com.learnkafkastreams.topology.OrdersTopology.*;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderLineItem;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.service.OrderService;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.streams.KeyValue;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@EmbeddedKafka(topics = {ORDERS, STORES})
@TestPropertySource(
    properties = {
      "spring.kafka.streams.bootstrap-servers = ${spring.embedded.kafka.brokers}",
      "spring.kafka.producer.bootstrap-servers = ${spring.embedded.kafka.brokers}"
    })
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class OrdersTopologyIntegrationTest {

  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;
  @Autowired
  StreamsBuilderFactoryBean streamsBuilderFactoryBean;
  @Autowired
  ObjectMapper objectMapper;
  @Autowired
  OrderService orderService;

  @BeforeEach
  void setup() {
  }

  @AfterEach
  void destroy() {
    Objects.requireNonNull(streamsBuilderFactoryBean.getKafkaStreams()).close();
    streamsBuilderFactoryBean.getKafkaStreams().cleanUp();
  }

  @Test
  void ordersCount() {
    // Given
    publishOrders();
    // Then
    Awaitility.await().atMost(Duration.ofSeconds(10))
            .pollDelay(Duration.ofSeconds(1))
            .ignoreExceptions()
            .until(() -> orderService.getOrdersCount(GENERAL_ORDERS).size(), equalTo(1));

    var generalOrdersCount = orderService.getOrdersCount(GENERAL_ORDERS);
    assertEquals(1, generalOrdersCount.getFirst().orderCount());
  }

  @Test
  void ordersRevenue() {
    // Given
    publishOrders();
    // Then
    Awaitility.await().atMost(Duration.ofSeconds(10))
            .pollDelay(Duration.ofSeconds(1))
            .ignoreExceptions()
            .until(() -> orderService.getOrdersCount(GENERAL_ORDERS).size(), equalTo(1));

    Awaitility.await().atMost(Duration.ofSeconds(10))
            .pollDelay(Duration.ofSeconds(1))
            .ignoreExceptions()
            .until(() -> orderService.getOrdersCount(RESTAURANT_ORDERS).size(), equalTo(1));

    var generalOrdersRevenue = orderService.getRevenueByOrderType(GENERAL_ORDERS);
    assertEquals(new BigDecimal("27.00"), generalOrdersRevenue.getFirst().totalRevenue().runningRevenue());

    var restaurantOrdersRevenue = orderService.getRevenueByOrderType(RESTAURANT_ORDERS);
    assertEquals(new BigDecimal("15.00"), restaurantOrdersRevenue.getFirst().totalRevenue().runningRevenue());
  }

  @Test
  void ordersRevenue_multipleOrders() {
    // Given
    publishOrders();
    publishOrders();
    // Then
    Awaitility.await().atMost(Duration.ofSeconds(60))
            .pollDelay(Duration.ofSeconds(1))
            .ignoreExceptions()
            .until(() -> orderService.getOrdersCount(GENERAL_ORDERS).size(), equalTo(1));

    Awaitility.await().atMost(Duration.ofSeconds(60))
            .pollDelay(Duration.ofSeconds(1))
            .ignoreExceptions()
            .until(() -> orderService.getOrdersCount(RESTAURANT_ORDERS).size(), equalTo(1));

    var generalOrdersRevenue = orderService.getRevenueByOrderType(GENERAL_ORDERS);
    assertEquals(new BigDecimal("54.00"), generalOrdersRevenue.getFirst().totalRevenue().runningRevenue());

    var restaurantOrdersRevenue = orderService.getRevenueByOrderType(RESTAURANT_ORDERS);
    assertEquals(new BigDecimal("30.00"), restaurantOrdersRevenue.getFirst().totalRevenue().runningRevenue());
  }




  private void publishOrders() {
    orders()
        .forEach(
            order -> {
              String orderJSON = null;
              try {
                orderJSON = objectMapper.writeValueAsString(order.value);
              } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
              }
              kafkaTemplate.send(ORDERS, order.key, orderJSON);
            });
  }

  static List<KeyValue<String, Order>> orders() {

    var orderItems =
        List.of(
            new OrderLineItem("Bananas", 2, new BigDecimal("2.00")),
            new OrderLineItem("Iphone Charger", 1, new BigDecimal("25.00")));

    var orderItemsRestaurant =
        List.of(
            new OrderLineItem("Pizza", 2, new BigDecimal("12.00")),
            new OrderLineItem("Coffee", 1, new BigDecimal("3.00")));

    var order1 =
        new Order(
            12345,
            "store_1234",
            new BigDecimal("27.00"),
            OrderType.GENERAL,
            orderItems,
            // LocalDateTime.now()
            LocalDateTime.parse("2023-02-21T21:25:01"));

    var order2 =
        new Order(
            54321,
            "store_1234",
            new BigDecimal("15.00"),
            OrderType.RESTAURANT,
            orderItemsRestaurant,
            // LocalDateTime.now()
            LocalDateTime.parse("2023-02-21T21:25:01"));
    var keyValue1 = KeyValue.pair(order1.orderId().toString(), order1);

    var keyValue2 = KeyValue.pair(order2.orderId().toString(), order2);

    return List.of(keyValue1, keyValue2);
  }
}
