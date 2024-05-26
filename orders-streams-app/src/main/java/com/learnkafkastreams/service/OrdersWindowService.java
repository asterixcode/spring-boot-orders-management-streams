package com.learnkafkastreams.service;

import static com.learnkafkastreams.service.OrderService.mapOrderType;
import static com.learnkafkastreams.topology.OrdersTopology.*;

import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.OrdersCountPerStoreByWindowsDTO;
import com.learnkafkastreams.domain.OrdersRevenuePerStoreByWindowsDTO;
import com.learnkafkastreams.domain.TotalRevenue;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
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

    return mapToOrdersCountPerStoreByWindowsDTOS(countWindowsIterator, orderTypeEnum);
  }

  private static List<OrdersCountPerStoreByWindowsDTO> mapToOrdersCountPerStoreByWindowsDTOS(
      KeyValueIterator<Windowed<String>, Long> countWindowsIterator, OrderType orderTypeEnum) {
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

  public List<OrdersCountPerStoreByWindowsDTO> getAllOrdersCountByWindows(
      LocalDateTime fromTime, LocalDateTime toTime) {

    var fromTimeInstant = fromTime.toInstant(ZoneOffset.of("UTC"));
    var toTimeInstant = toTime.toInstant(ZoneOffset.of("UTC"));

    var generalOrdersCountByWindows =
        getCountWindowsStore(GENERAL_ORDERS)
                // .fetchAll(fromTimeInstant, toTimeInstant)
                .backwardFetchAll(fromTimeInstant, toTimeInstant)
                ;


    var generalOrdersCountByWindowsDTO =
            mapToOrdersCountPerStoreByWindowsDTOS(generalOrdersCountByWindows, OrderType.GENERAL);

    var restaurantOrdersCountByWindows =
        getCountWindowsStore(RESTAURANT_ORDERS).fetchAll(fromTimeInstant, toTimeInstant);

    var restaurantOrdersCountByWindowsDTO =
            mapToOrdersCountPerStoreByWindowsDTOS(restaurantOrdersCountByWindows, OrderType.RESTAURANT);

    return Stream.of(generalOrdersCountByWindowsDTO, restaurantOrdersCountByWindowsDTO)
        .flatMap(Collection::stream)
        .toList();
  }

  public List<OrdersRevenuePerStoreByWindowsDTO> getOrdersRevenueWindowsByType(String orderType) {
    var revenueWindowsStore = getRevenueWindowsStore(orderType);
    var orderTypeEnum = mapOrderType(orderType);
    var revenueWindowsIterator = revenueWindowsStore.all();

    var spliterator = Spliterators.spliteratorUnknownSize(revenueWindowsIterator, 0);
    return StreamSupport.stream(spliterator, false)
            .map(
                    keyValue ->
                            new OrdersRevenuePerStoreByWindowsDTO(
                                    keyValue.key.key(),
                                    keyValue.value,
                                    orderTypeEnum,
                                    LocalDateTime.ofInstant(keyValue.key.window().startTime(), ZoneId.of("GMT")),
                                    LocalDateTime.ofInstant(keyValue.key.window().endTime(), ZoneId.of("GMT"))))
            .toList();
  }

  private ReadOnlyWindowStore<String, TotalRevenue> getRevenueWindowsStore(String orderType) {
    return switch (orderType) {
      case GENERAL_ORDERS ->
              orderStoreService.ordersWindowsRevenueStore(GENERAL_ORDERS_REVENUE_WINDOWS);
      case RESTAURANT_ORDERS ->
              orderStoreService.ordersWindowsRevenueStore(RESTAURANT_ORDERS_REVENUE_WINDOWS);
      default -> throw new IllegalStateException("Not a valid order type");
    };
  }
}
