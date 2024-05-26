package com.learnkafkastreams.controller;

import com.learnkafkastreams.domain.AllOrdersCountPerStoreDTO;
import com.learnkafkastreams.service.OrderService;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/v1/orders")
public class OrdersController {

  private final OrderService orderService;

  public OrdersController(OrderService orderService) {
    this.orderService = orderService;
  }

  @GetMapping("/count/{order_type}")
  public ResponseEntity<?> getOrderCountByType(
      @PathVariable("order_type") String orderType,
      @RequestParam(value = "location_id", required = false) String locationId) {

    if (StringUtils.hasLength(locationId)) {
      return ResponseEntity.ok(orderService.getOrdersCountByLocation(orderType, locationId));
    }

    return ResponseEntity.ok(orderService.getOrdersCount(orderType));
  }

  @GetMapping("/count")
  public List<AllOrdersCountPerStoreDTO> getAllOrdersCount() {
    return orderService.getAllOrdersCount();
  }
}
