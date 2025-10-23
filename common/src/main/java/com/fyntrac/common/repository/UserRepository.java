package com.fyntrac.common.repository;

import com.fyntrac.common.entity.User;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.Update;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public interface UserRepository extends MongoRepository<User, String> {

    // Basic find operations
    Optional<User> findByUsername(String username);
    Optional<User> findByEmail(String email);
    Optional<User> findByPhoneNumber(String phoneNumber);

    // Existence checks
    boolean existsByUsername(String username);
    boolean existsByEmail(String email);
    boolean existsByPhoneNumber(String phoneNumber);

    // Find by tenant associations
    List<User> findByTenantIdsContaining(String tenantId);
    List<User> findByTenantIdsIn(List<String> tenantIds);

    // Find by merchant
    List<User> findByMerchantId(String merchantId);
    List<User> findByMerchantIdAndActiveTrue(String merchantId);

    // Find by status flags
    List<User> findByActiveTrue();
    List<User> findByActiveFalse();
    List<User> findByVerifiedTrue();
    List<User> findByVerifiedFalse();
    List<User> findByActiveTrueAndVerifiedTrue();

    // Find by name patterns
    List<User> findByFirstNameContainingIgnoreCase(String firstName);
    List<User> findByLastNameContainingIgnoreCase(String lastName);
    List<User> findByFirstNameContainingIgnoreCaseOrLastNameContainingIgnoreCase(String firstName, String lastName);

    // Complex queries with @Query
    @Query("{ '$and': [ " +
            "{ 'tenantIds': ?0 }, " +
            "{ 'active': true }, " +
            "{ 'verified': true } " +
            "] }")
    List<User> findActiveVerifiedUsersByTenant(String tenantId);

    @Query("{ 'merchantId': ?0, 'tenantIds': ?1, 'active': true }")
    List<User> findActiveUsersByMerchantAndTenant(String merchantId, String tenantId);

    @Query("{ 'roles.roleName': ?0 }")
    List<User> findByRoleName(String roleName);

    @Query("{ 'roles.system': ?0 }")
    List<User> findBySystem(String system);

    @Query("{ 'roles.system': ?0, 'roles.roleName': ?1 }")
    List<User> findBySystemAndRoleName(String system, String roleName);

    @Query("{ 'tenantIds': { $size: ?0 } }")
    List<User> findByTenantCount(int count);

    // Date-based queries
    List<User> findByCreatedAtAfter(Instant date);
    List<User> findByLastLoginAtAfter(Instant date);
    List<User> findByLastLoginAtBefore(Instant date);

    @Query("{ 'lastLoginAt': { $lt: ?0 } }")
    List<User> findInactiveUsersSince(Instant date);

    // Count queries
    long countByActiveTrue();
    long countByVerifiedTrue();
    long countByMerchantId(String merchantId);
    long countByTenantIdsContaining(String tenantId);

    // Update operations
    @Query("{ '_id': ?0 }")
    @Update("{ '$set': { 'lastLoginAt': ?1 } }")
    void updateLastLogin(String userId, Instant lastLoginAt);

    @Query("{ '_id': ?0 }")
    @Update("{ '$set': { 'active': ?1, 'updatedAt': ?2 } }")
    void updateActiveStatus(String userId, boolean active, Instant updatedAt);

    @Query("{ '_id': ?0 }")
    @Update("{ '$set': { 'verified': ?1, 'updatedAt': ?2 } }")
    void updateVerifiedStatus(String userId, boolean verified, Instant updatedAt);

    // Bulk operations
    @Query("{ 'merchantId': ?0 }")
    @Update("{ '$set': { 'active': ?1, 'updatedAt': ?2 } }")
    void updateActiveStatusByMerchant(String merchantId, boolean active, Instant updatedAt);

    @Query("{ 'tenantIds': ?0 }")
    @Update("{ '$pull': { 'tenantIds': ?0 } }")
    void removeTenantFromUsers(String tenantId);

    @Query("{ '_id': ?0 }")
    @Update("{ '$addToSet': { 'tenantIds': ?1 } }")
    void addTenantToUser(String userId, String tenantId);

    // Search with multiple criteria
    @Query("{ '$and': [ " +
            "{ '$or': [ " +
            "  { 'firstName': { $regex: ?0, $options: 'i' } }, " +
            "  { 'lastName': { $regex: ?0, $options: 'i' } }, " +
            "  { 'email': { $regex: ?0, $options: 'i' } } " +
            "] }, " +
            "{ 'merchantId': ?1 }, " +
            "{ 'active': true } " +
            "] }")
    List<User> searchActiveUsersInMerchant(String searchTerm, String merchantId);
}
