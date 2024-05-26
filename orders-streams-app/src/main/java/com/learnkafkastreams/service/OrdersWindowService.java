package com.learnkafkastreams.service;

import static com.learnkafkastreams.service.OrderService.mapOrderType;
import static com.learnkafkastreams.topology.OrdersTopology.*;

import com.learnkafkastreams.domain.OrdersCountPerStoreByWindowsDTO;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class OrdersWindowService {

  private final OrderStoreService orderStoreService;

  public OrdersWindowService(OrderStoreService orderStoreService) {
    this.orderStoreService = orderStoreService;
  }

  public List<OrdersCountPerStoreByWindowsDTO> getOrdersCountWindowsByType(String orderType) {
    var countWindowsStore = getCountWindowsStore(orderType);
    var orderTypeEnum = mapOrderType(orderType);
    var countWindowsIterator = countWindowsStore.all();
    var spliterator = Spliterators.spliteratorUnknownSize(countWindowsIterator, 0);

    return StreamSupport.stream(spliterator, false)
        .map(
            keyValue ->
                new OrdersCountPerStoreByWindowsDTO(
                    keyValue.key.key(),
                    keyValue.value,
                    orderTypeEnum,
                    LocalDateTime.ofInstant(keyValue.key.window().startTime(), ZoneId.of("GMT")),
                    LocalDateTime.ofInstant(keyValue.key.window().endTime(), ZoneId.of("GMT"))))
        .toList();
  }

  private ReadOnlyWindowStore<String, Long> getCountWindowsStore(String orderType) {
    return switch (orderType) {
      case GENERAL_ORDERS ->
          orderStoreService.ordersWindowsCountStore(GENERAL_ORDERS_COUNT_WINDOWS);
      case RESTAURANT_ORDERS ->
          orderStoreService.ordersWindowsCountStore(RESTAURANT_ORDERS_COUNT_WINDOWS);
      default -> throw new IllegalStateException("Not a valid order type");
    };
  }

  public List<OrdersCountPerStoreByWindowsDTO> getAllOrdersCountByWindows() {
    var generalOrdersCountByWindows = getOrdersCountWindowsByType(GENERAL_ORDERS);
    var restaurantOrdersCountByWindows = getOrdersCountWindowsByType(RESTAURANT_ORDERS);

    return Stream.of(generalOrdersCountByWindows, restaurantOrdersCountByWindows)
        .flatMap(Collection::stream)
        .toList();
  }
}
