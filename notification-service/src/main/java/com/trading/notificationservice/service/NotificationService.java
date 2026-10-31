package com.trading.notificationservice.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
public class NotificationService {

    @KafkaListener(topics = "order.executed")
    public void consumeOrderExecuted(@Payload Map<String, Object> payload){

        try {
            String userId = (String) payload.get("userId");
            String type = (String) payload.get("type");
            String symbol = (String) payload.get("symbol");
            Object quantity = payload.get("quantity");
            Object price = payload.get("price");
            Object totalAmount = payload.get("totalAmount");

            sendNotification(userId, type.equals("BUY")
                    ? "ORDER EXECUTED - BUY"
                    : "ORDER EXECUTED - SELL",
                    String.format("%s %s shares of %s at %s. Total: %s",
                            type, quantity, symbol, price, totalAmount));
        }
        catch (Exception e){
            log.error("Error: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "order.failed")
    public void consumeOrderFailed(@Payload Map<String, Object> payload){

        try {
            String userId = (String) payload.get("userId");
            String symbol = (String) payload.get("symbol");
            String reason = (String) payload.get("reason");

            sendNotification(userId,
                    "Order Failed",
                    String.format("Your order for %s failed. Reason: %s",
                            symbol, reason));
        } catch (Exception e) {
            log.error("Error: {}",e.getMessage());
        }
    }

    @KafkaListener(topics = "user.register")
    public void consumeUserRegister(
            @Payload Map<String, Object> payload){

        try {
            String userId = (String) payload.get("userId");
            String firstName = (String) payload.get("firstName");
            Object walletBalance = payload.get("walletBalance");

            sendNotification(userId,
                    "Welcome to stock trading platforn",
                    String.format("Welcome %s! %s added to your wallet.",
                            firstName,walletBalance));
        }
        catch (Exception e){
            log.error("Error: {}",e.getMessage());
        }
    }

        private void sendNotification(String userId, String title, String message) {
            log.info("----------------------------");
            log.info("NOTIFICATION SENT");
            log.info("TO USER: {}",userId);
            log.info("TITLE: {}",title);
            log.info("MESSAGE: {}",message);
            log.info("--------------------------");
    }
}
