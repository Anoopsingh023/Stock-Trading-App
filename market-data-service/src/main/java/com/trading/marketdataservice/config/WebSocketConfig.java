package com.trading.marketdataservice.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

/**
 * How it works
 * 1. Client connects to /ws endpoints
 * 2. Client subscribe to /topic/prices/{symbol}
 * 3. Server publishes price update to that topic
 * 4. Client receives update in real time - no polling needed
 *
 * This is exactly how Zerodha/Robinhood/Grow stream real time prices
 */

@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry){
        // Topics prefixed with /topic -> broadcast to all subscriber
        registry.enableSimpleBroker("/topic");

        // Application destination prefix
        // Client messages to /app/** are routed to @Messagemapping methods
        registry.setApplicationDestinationPrefixes("/app");

    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry){

        // Websocket endpoint - clients connect here
        // SockJs fallback for browsers that don't support websocket

        registry.addEndpoint("/ws")
                .setAllowedOriginPatterns("*")
                .withSockJS();
    }
}
