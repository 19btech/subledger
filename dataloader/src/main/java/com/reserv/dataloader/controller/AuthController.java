package com.reserv.dataloader.controller;

import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.Tenant;
import com.fyntrac.common.entity.User;
import com.fyntrac.common.repository.TenantRepository;
import com.fyntrac.common.repository.UserRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@RestController
@RequestMapping("/api/dataloader/fyntrac/auth")
public class AuthController {

    private final UserRepository userRepository;
    private final TenantRepository tenantRepository;

    @Autowired
    public AuthController(UserRepository userRepository,
                          TenantRepository tenantRepository) {
        this.userRepository = userRepository;
        this.tenantRepository = tenantRepository;
    }


    @PostMapping("/login")
// Change the return type to ResponseEntity<Object> to allow different body types
    public ResponseEntity<Object> authenticate(@RequestBody Records.AuthticationRecord auth) {

        try {
            Optional<User> userOptional = this.userRepository.findByEmail(auth.email());

            if (userOptional.isEmpty()) {
                // 1. Changed to HttpStatus.UNAUTHORIZED (401) for a failed login
                // 2. Removed the extra dot syntax error
                return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                        .body(Map.of("error", "Invalid email or password"));
            }

            User user = userOptional.get();

            // ---
            // CRITICAL SECURITY WARNING: You must add a password check here!
            // Without this, anyone with a valid email can log in.
            //
            // Example (assuming you have a passwordEncoder and auth.password()):
            // if (!passwordEncoder.matches(auth.password(), user.getPassword())) {
            //     return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
            //                          .body(Map.of("error", "Invalid email or password"));
            // }
            // ---

            // This is fine, or you can use the shorthand: return ResponseEntity.ok(user);
            List<Tenant> optionalTenants = this.tenantRepository.findByIdIn(user.getTenantIds());

            if (optionalTenants == null && optionalTenants.isEmpty()) {
                // 1. Changed to HttpStatus.UNAUTHORIZED (401) for a failed login
                // 2. Removed the extra dot syntax error
                return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                        .body(Map.of("error", "Invalid email or password or no tenant is associated"));
            }

            Records.UserTenantRecord userTenantRecord = RecordFactory.createUserTenantRecord(user, optionalTenants);
            return new ResponseEntity<>(userTenantRecord, HttpStatus.OK);

        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error("Error during authentication: {}", e.getMessage(), e); // Log the full stack trace

            // 3. Return a consistent error body
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "An internal server error occurred"));
        }
    }
}
