package com.trading.orderservice.controller;

import com.trading.orderservice.dto.OrderRequest;
import com.trading.orderservice.entity.Order;
import com.trading.orderservice.service.OrderService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/orders")
@Slf4j
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    /**
     * place buy or sell order
     * @param userId
     * @param request
     * @return
     */
    @PostMapping
    public ResponseEntity<Order> placeOrder(
            @RequestHeader("X-User-Id") String userId,
            @Valid @RequestBody OrderRequest request){

        request.setUserId(userId);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(orderService.placeOrder(request));
    }

    /**
     * get order detail
     * @param orderId
     * @param userId
     * @return
     */
    @GetMapping("{ordeerId}")
    public ResponseEntity<Order> getOrder(
            @PathVariable String orderId,
            @RequestHeader("X-User-Id") String userId) {

        Order order = orderService.getOrder(orderId);

        // Security check
        if(!order.getUserId().equals(userId)){
            return  ResponseEntity.status(403).build();
        }

        return ResponseEntity.ok(order);
    }

    /**
     * Get all orders
     * @param userId
     * @return
     */
    @GetMapping("/my-orders")
    public ResponseEntity<List<Order>> getMyOrders(
            @RequestHeader("X-User-Id") String userId){

        return ResponseEntity.ok(orderService.getMyOrders(userId));
    }



}
