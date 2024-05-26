package com.learnkafkastreams.service;

import static com.learnkafkastreams.topology.OrdersTopology.*;

import com.learnkafkastreams.domain.AllOrdersCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderType;
import java.util.Collection;
import java.util.List;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class OrderService {

  private final OrderStoreService orderStoreService;

  public OrderService(OrderStoreService orderStoreService) {
    this.orderStoreService = orderStoreService;
  }

  public List<OrderCountPerStoreDTO> getOrdersCount(String orderType) {
    var ordersCountStore = gerOrderStore(orderType);
    var orders = ordersCountStore.all();

    var spliterator = Spliterators.spliteratorUnknownSize(orders, 0);

    return StreamSupport.stream(spliterator, false)
        .map(keyValue -> new OrderCountPerStoreDTO(keyValue.key, keyValue.value))
        .toList();
  }

  private ReadOnlyKeyValueStore<String, Long> gerOrderStore(String orderType) {
    return switch (orderType) {
      case GENERAL_ORDERS -> orderStoreService.ordersCountStore(GENERAL_ORDERS_COUNT);
      case RESTAURANT_ORDERS -> orderStoreService.ordersCountStore(RESTAURANT_ORDERS_COUNT);
      default -> throw new IllegalArgumentException("Not a valid order type");
    };
  }

  public OrderCountPerStoreDTO getOrdersCountByLocation(String orderType, String locationId) {
    var ordersCountStore = gerOrderStore(orderType);

    var orderCount = ordersCountStore.get(locationId);

    if (orderCount != null) {
      return new OrderCountPerStoreDTO(locationId, orderCount);
    }

    log.warn("No orders found for locationId: {}", locationId);
    return null;
  }

  public List<AllOrdersCountPerStoreDTO> getAllOrdersCount() {
    BiFunction<OrderCountPerStoreDTO, OrderType, AllOrdersCountPerStoreDTO> mapper =
        (orderCountPerStoreDTO, orderType) ->
            new AllOrdersCountPerStoreDTO(
                orderCountPerStoreDTO.locationId(), orderCountPerStoreDTO.orderCount(), orderType);

    var generalOrdersCount =
        getOrdersCount(GENERAL_ORDERS).stream()
            .map(orderCountPerStoreDTO -> mapper.apply(orderCountPerStoreDTO, OrderType.GENERAL))
            .toList();

    var restaurantOrdersCount =
        getOrdersCount(RESTAURANT_ORDERS).stream()
            .map(orderCountPerStoreDTO -> mapper.apply(orderCountPerStoreDTO, OrderType.RESTAURANT))
            .toList();

    return Stream.of(generalOrdersCount, restaurantOrdersCount)
        .flatMap(Collection::stream)
        .toList();
  }
}
