package com.fyntrac.common.repository;

import com.fyntrac.common.entity.Tenant;
import com.fyntrac.common.enums.TenantStatus;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.Update;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public interface TenantRepository extends MongoRepository<Tenant, String> {

    // Basic find operations
    Optional<Tenant> findByTenantCode(String tenantCode);
    Optional<Tenant> findByName(String name);

    // Multiple tenants by codes
    List<Tenant> findByTenantCodeIn(List<String> tenantCodes);
    List<Tenant> findByNameIn(List<String> names);

    // Multiple tenants by Ids
    List<Tenant> findByIdIn(List<String> ids);


    // Find by merchant
    List<Tenant> findByMerchantId(String merchantId);
    List<Tenant> findByMerchantIdAndStatus(String merchantId, TenantStatus status);

    // Find by status
    List<Tenant> findByStatus(TenantStatus status);
    List<Tenant> findByStatusOrderByNameAsc(TenantStatus status);

    // Find by locale and currency
    List<Tenant> findByLocale(String locale);
    List<Tenant> findByCurrency(String currency);
    List<Tenant> findByTimezone(String timezone);
    List<Tenant> findByLocaleAndCurrency(String locale, String currency);

    // Find tenants containing specific users
    List<Tenant> findByUserIdsContaining(String userId);
    List<Tenant> findByUserIdsIn(List<String> userIds);

    // Existence checks
    boolean existsByTenantCode(String tenantCode);
    boolean existsByName(String name);
    boolean existsByMerchantIdAndName(String merchantId, String name);

    // Search operations
    List<Tenant> findByNameContainingIgnoreCase(String name);
    List<Tenant> findByDescriptionContainingIgnoreCase(String description);

    // Complex queries with @Query
    @Query("{ '$and': [ " +
            "{ 'merchantId': ?0 }, " +
            "{ 'status': ?1 }, " +
            "{ '$or': [ " +
            "  { 'name': { $regex: ?2, $options: 'i' } }, " +
            "  { 'description': { $regex: ?2, $options: 'i' } } " +
            "] } " +
            "] }")
    List<Tenant> findByMerchantStatusAndSearchTerm(String merchantId, TenantStatus status, String searchTerm);

    @Query("{ 'userIds': { $size: ?0 } }")
    List<Tenant> findByUserCount(int userCount);

    @Query("{ 'userIds': { $exists: true, $ne: [] } }")
    List<Tenant> findTenantsWithUsers();

    @Query("{ 'userIds': { $exists: true, $size: 0 } }")
    List<Tenant> findTenantsWithoutUsers();

    @Query("{ 'currency': { $in: ?0 } }")
    List<Tenant> findByCurrenciesIn(List<String> currencies);

    @Query("{ 'locale': { $in: ?0 } }")
    List<Tenant> findByLocalesIn(List<String> locales);

    // Date-based queries
    List<Tenant> findByCreatedAtAfter(Instant date);
    List<Tenant> findByUpdatedAtAfter(Instant date);
    List<Tenant> findByCreatedAtBetween(Instant startDate, Instant endDate);

    // Count operations
    long countByMerchantId(String merchantId);
    long countByStatus(TenantStatus status);
    long countByMerchantIdAndStatus(String merchantId, TenantStatus status);
    long countByCurrency(String currency);
    long countByLocale(String locale);

    // Update operations
    @Query("{ '_id': ?0 }")
    @Update("{ '$set': { 'status': ?1, 'updatedAt': ?2 } }")
    void updateStatus(String tenantId, TenantStatus status, Instant updatedAt);

    @Query("{ '_id': ?0 }")
    @Update("{ '$set': { 'name': ?1, 'description': ?2, 'updatedAt': ?3 } }")
    void updateBasicInfo(String tenantId, String name, String description, Instant updatedAt);

    @Query("{ '_id': ?0 }")
    @Update("{ '$set': { 'timezone': ?1, 'currency': ?2, 'locale': ?3, 'updatedAt': ?4 } }")
    void updateRegionalSettings(String tenantId, String timezone, String currency, String locale, Instant updatedAt);

    // User management operations
    @Query("{ '_id': ?0 }")
    @Update("{ '$addToSet': { 'userIds': ?1 } }")
    void addUserToTenant(String tenantId, String userId);

    @Query("{ '_id': ?0 }")
    @Update("{ '$pull': { 'userIds': ?1 } }")
    void removeUserFromTenant(String tenantId, String userId);

    @Query("{ '_id': ?0 }")
    @Update("{ '$set': { 'userIds': ?1, 'updatedAt': ?2 } }")
    void setTenantUsers(String tenantId, List<String> userIds, Instant updatedAt);

    // Bulk operations
    @Query("{ 'merchantId': ?0 }")
    @Update("{ '$set': { 'status': ?1, 'updatedAt': ?2 } }")
    void updateStatusByMerchant(String merchantId, TenantStatus status, Instant updatedAt);

    // Find tenants with pagination support
    @Query("{ 'merchantId': ?0 }")
    List<Tenant> findByMerchantId(String merchantId, org.springframework.data.domain.Pageable pageable);
}