package com.trading.userservice.service;

import com.trading.userservice.dto.AuthResponse;
import com.trading.userservice.dto.LoginRequest;
import com.trading.userservice.dto.RegisterRequest;
import com.trading.userservice.dto.UserResponse;
import com.trading.userservice.entity.User;
import com.trading.userservice.repository.UserRepository;
import jakarta.validation.Valid;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class UserService {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private static final String USER_REGISTERED_TOPIC = "user.registered";

    /**
     * register new trader
     * @param request
     * @return
     */
    public AuthResponse register(@Valid RegisterRequest request) {
        log.info("Registering user: {}",request.getEmail());

        if(userRepository.existsByEmail(request.getEmail())){
            throw new RuntimeException(
                    "Email already registered"+ request.getEmail()
            );
        }

        User user = new User();
        user.setFirstName(request.getFirstName());
        user.setLastName(request.getLastName());
        user.setEmail(request.getEmail());
        user.setPassword(passwordEncoder.encode(request.getPassword()));
        user.setWalletBalance(
                request.getInitialDeposit() != null
                ? request.getInitialDeposit()
                        : BigDecimal.valueOf(10000)
        );

        User savedUser = userRepository.save(user);

        log.info("user registered: {}", savedUser.getId());

        // Publish user.registered event
        Map<String, Object> userRegisteredEvent = new HashMap<>();
        userRegisteredEvent.put("userId",savedUser.getId());
        userRegisteredEvent.put("email",savedUser.getEmail());
        userRegisteredEvent.put("firstName",savedUser.getFirstName());
        userRegisteredEvent.put("lastName",savedUser.getLastName());
        userRegisteredEvent.put("walletBalance",savedUser.getWalletBalance());

        kafkaTemplate.send(USER_REGISTERED_TOPIC, savedUser.getId(), userRegisteredEvent);

        String token = generateAccessToken(savedUser.getId(),savedUser.getEmail());
        String refreshToken = generateRefreshToken(savedUser.getId());

        return buildAuthResponse(savedUser,token,refreshToken);

    }

    /**
     * login
     * @param request
     * @return
     */
    public AuthResponse login(@Valid LoginRequest request) {
        log.info("login attempt: {}",request.getEmail());

        User user = userRepository.findByEmail(request.getEmail())
                .orElseThrow(()->new RuntimeException("User not found"+request.getEmail()));

        if (!passwordEncoder.matches(
                request.getPassword(),user.getPassword())){
            throw new RuntimeException("Invalid credentials");
        }

        log.info("Login successful: {}",user.getId());

        String token = generateAccessToken(user.getId(),user.getEmail());
        String refreshToken = generateRefreshToken(user.getId());

        return buildAuthResponse(user,token,refreshToken);

    }

    /**
     * get user by id
     * @param userId
     * @return
     */
    public UserResponse getUserById(String userId){
        User user = userRepository.findById(userId)
                .orElseThrow(()-> new RuntimeException("User no found"+userId));

        return mapToResponse(user);
    }

    /**
     * add funds to wallet
     * @param userId
     * @param amount
     * @return
     */
    public UserResponse addFund(String userId, BigDecimal amount) {
        User user = userRepository.findById(userId)
                .orElseThrow(()-> new RuntimeException("user not found"+userId));

        user.setWalletBalance(user.getWalletBalance().add(amount));
        User saveduser = userRepository.save(user);

        log.info("Funds add: {} to user: {}",amount,userId);

        return mapToResponse(user);
    }

    /**
     * deduct from wallet - call on buy order
     * @param userId
     * @param amount
     * @return
     */
    public UserResponse deductFunds(String userId, BigDecimal amount){
        User user = userRepository.findById(userId)
                .orElseThrow(()-> new RuntimeException("user not found"+userId));

        if(user.getWalletBalance().compareTo(amount)<0){
            throw new RuntimeException("Insufficient balance");
        }
        user.setWalletBalance(user.getWalletBalance().subtract(amount));
        User saveduser = userRepository.save(user);

        log.info("Funds deducted: {} to user: {}",amount,userId);

        return mapToResponse(user);
    }

    /**
     * credit funds to wallet - called on sell order
     * @param userId
     * @param amount
     * @return
     */
    public UserResponse creditFunds(String userId, BigDecimal amount) {
        User user = userRepository.findById(userId)
                .orElseThrow(()-> new RuntimeException("user not found"+userId));

        user.setWalletBalance(user.getWalletBalance().add(amount));
        User saveduser = userRepository.save(user);

        log.info("Funds credited: {} to user: {}",amount,userId);

        return mapToResponse(user);
    }




    private AuthResponse buildAuthResponse(User user, String token,String refreshToken){
        AuthResponse response = new AuthResponse();
        response.setUserId(user.getId());
        response.setEmail(user.getEmail());
        response.setFirstName(user.getFirstName());
        response.setLastName(user.getLastName());
        response.setWalletBalance(user.getWalletBalance());

        response.setAccessToken(token);
        response.setRefreshToken(refreshToken);

        return response;
    }

    private UserResponse mapToResponse(User user){
        UserResponse response = new UserResponse();
        response.setId(user.getId());
        response.setEmail(user.getEmail());
        response.setFirstName(user.getFirstName());
        response.setLastName(user.getLastName());
        response.setWalletBalance(user.getWalletBalance());
        response.setStatus(user.getStatus());
        response.setCreatedAt(user.getCreatedAt());

        return response;
    }



}
