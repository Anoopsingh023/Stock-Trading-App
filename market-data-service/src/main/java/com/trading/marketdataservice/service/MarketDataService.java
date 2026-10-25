package com.trading.marketdataservice.service;

import com.trading.marketdataservice.dto.StockPrice;
import com.trading.marketdataservice.entity.Stock;
import com.trading.marketdataservice.repository.StockRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
@RequiredArgsConstructor
public class MarketDataService {

    private final StockRepository stockRepository;
    private final Random random = new Random();
    private final RedisTemplate<String, Object> redisTemplate;
    private final SimpMessagingTemplate messagingTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String PRICE_KEY_PREFIX = "stock:price:";
    private static final String PRICE_UPDATED_TOPIC = "stock.price.updated";


    /**
     * ConcurrentHashMap - for thread safety
     */
    private final Map<String, StockPrice> stockPrices =
            new ConcurrentHashMap<>();

    /**
     * Initialize stock prices
     *
     * @PostConstruct - runs when context is ready
     */
    @PostConstruct
    public void initializePrices(){
        log.info("Loading stocks data..");

        List<Stock> stocks = stockRepository.findByActiveTrue();

        if(stocks.isEmpty()){
            log.warn("No active stocks found");
            return;
        }

        stocks.forEach(stock -> {
            StockPrice stockPrice = new StockPrice();
            stockPrice.setSymbol(stock.getSymbol());
            stockPrice.setCompanyName(stock.getCompanyName());
            stockPrice.setPrice(stock.getInitialPrice());
            stockPrice.setOpen(stock.getInitialPrice());
            stockPrice.setHigh(stock.getInitialPrice());
            stockPrice.setLow(stock.getInitialPrice());
            stockPrice.setChange(BigDecimal.ZERO);
            stockPrice.setChangePercent(BigDecimal.ZERO);
            stockPrice.setVolume(0L);
            stockPrice.setTimestamp(LocalDateTime.now());

            stockPrices.put(stock.getSymbol(), stockPrice);
            log.info("Loaded stock: {} at {}",stock.getSymbol(), stock.getInitialPrice());
        });

        log.info("Initialized {} stocks",stockPrices.size());

    }

    /**
     * Scheduled price update - every 2 second
     */
    @Scheduled(fixedRateString = "${market.price-update-interval}")
    public void updatePrices(){
        if(stockPrices.isEmpty()){
            log.info("No stocks loaded");
            return;
        }

        stockPrices.forEach((symbol,currentPrice)->{
            BigDecimal oldPrice = currentPrice.getPrice();

            // +- 0.5 price movement
            double priceMovement = (random.nextDouble()-0.5)*0.5;

            BigDecimal priceChange = oldPrice
                    .multiply(BigDecimal.valueOf(
                            priceMovement/100))
                    .setScale(2, RoundingMode.HALF_UP);

            BigDecimal newPrice = oldPrice
                    .add(priceChange)
                    .setScale(2,RoundingMode.HALF_UP);

            // Prevent negative price
            if(newPrice.compareTo(BigDecimal.valueOf(1))<0){
                newPrice = BigDecimal.valueOf(1);
            }

            BigDecimal changeAmount = newPrice
                    .subtract(currentPrice.getOpen())
                    .setScale(1,RoundingMode.HALF_UP);

            BigDecimal changePercent = changeAmount
                    .divide(currentPrice.getOpen(),4,RoundingMode.HALF_UP)
                    .multiply(BigDecimal.valueOf(100))
                    .setScale(2,RoundingMode.HALF_UP);

            // update high/low
            BigDecimal newHigh = newPrice
                    .compareTo(currentPrice.getHigh())>0
                    ? newPrice : currentPrice.getHigh();

            BigDecimal newLow = newPrice
                    .compareTo(currentPrice.getLow())<0
                    ? newPrice : currentPrice.getLow();

            currentPrice.setPrice(newPrice);
            currentPrice.setChange(changeAmount);
            currentPrice.setChangePercent(changePercent);
            currentPrice.setHigh(newHigh);
            currentPrice.setLow(newLow);
            currentPrice.setTimestamp(LocalDateTime.now());

            // 1. cache in redis
            redisTemplate.opsForValue().set(
                    PRICE_KEY_PREFIX + symbol, currentPrice);

            //2. Broadcast via WebSocket -> all clients
            messagingTemplate.convertAndSend(
                    "/topic/prices"+symbol,currentPrice);

            // Publish to Kafka
            Map<String, Object> priceUpdateEvent = new HashMap<>();
            priceUpdateEvent.put("symbol",symbol);
            priceUpdateEvent.put("price",newPrice);
            priceUpdateEvent.put("timestamp",
                    LocalDateTime.now().toString());

            kafkaTemplate.send(PRICE_UPDATED_TOPIC,symbol,priceUpdateEvent);

        });

        log.debug("Prices updated for {} stocks",stockPrices.size());
    }


    /**
     * Get all active prices
     * @return
     */
    public List<StockPrice> getAllPrice() {
        return stockPrices.values().stream().toList();
    }

    /**
     * Get current price from redis
     * @param symbol
     * @return
     */
    public StockPrice getPrice(String symbol){
        Object cached = redisTemplate.opsForValue()
                .get(PRICE_KEY_PREFIX+symbol);

        if(cached != null){
            return (StockPrice) cached;
        }

        // fallback
        StockPrice price = stockPrices.get(symbol.toUpperCase());
        if(price == null){
            throw new RuntimeException("Stock not found"+ symbol);
        }
        return price;
    }

    /**
     * Get all stocks
     * @return
     */
    public List<Stock> getAllStocks(){
        return stockRepository.findByActiveTrue();
    }
}
