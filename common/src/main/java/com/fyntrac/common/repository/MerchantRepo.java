package com.fyntrac.common.repository;

import com.fyntrac.common.entity.Merchant;
import com.fyntrac.common.enums.MerchantStatus;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.Update;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public interface MerchantRepo extends MongoRepository<Merchant, String> {

    // Find by merchant code (unique identifier)
    Optional<Merchant> findByMerchantCode(String merchantCode);

    // Find by name (case-insensitive)
    List<Merchant> findByNameContainingIgnoreCase(String name);

    // Find by industry type
    List<Merchant> findByIndustryType(String industryType);

    // Find by status
    List<Merchant> findByStatus(MerchantStatus status);

    // Find by contact email
    Optional<Merchant> findByContactEmail(String contactEmail);

    // Find merchants that contain a specific tenant
    List<Merchant> findByTenantIdsContaining(String tenantId);

    // Find active merchants
    List<Merchant> findByStatusOrderByNameAsc(MerchantStatus status);

    // Check if merchant code exists
    boolean existsByMerchantCode(String merchantCode);

    // Check if contact email exists
    boolean existsByContactEmail(String contactEmail);

    // Find by name pattern using regex
    @Query("{ 'name': { $regex: ?0, $options: 'i' } }")
    List<Merchant> findByNamePattern(String namePattern);

    // Find by industry type and status
    List<Merchant> findByIndustryTypeAndStatus(String industryType, MerchantStatus status);

    // Find merchants created after a specific date
    List<Merchant> findByCreatedAtAfter(Instant createdAt);

    // Find merchants with pagination (you'll need Pageable parameter)
    // List<Merchant> findByStatus(MerchantStatus status, Pageable pageable);

    // Custom query to find merchants by multiple criteria
    @Query("{ '$and': [ " +
            "{ 'status': ?0 }, " +
            "{ '$or': [ " +
            "  { 'name': { $regex: ?1, $options: 'i' } }, " +
            "  { 'industryType': { $regex: ?1, $options: 'i' } } " +
            "] } " +
            "] }")
    List<Merchant> findByStatusAndNameOrIndustryType(MerchantStatus status, String searchTerm);

    // Find merchants with specific tenant count
    @Query("{ 'tenantIds': { $size: ?0 } }")
    List<Merchant> findByTenantCount(int tenantCount);

    // Find merchants with multiple tenant IDs
    @Query("{ 'tenantIds': { $in: ?0 } }")
    List<Merchant> findByTenantIdsIn(List<String> tenantIds);

    // Update merchant status
    @Query("{ '_id': ?0 }")
    @Update("{ '$set': { 'status': ?1, 'updatedAt': ?2 } }")
    void updateMerchantStatus(String merchantId, MerchantStatus status, Instant updatedAt);
}