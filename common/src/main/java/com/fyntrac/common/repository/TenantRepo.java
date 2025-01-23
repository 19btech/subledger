package com.fyntrac.common.repository;

import com.fyntrac.common.entity.Tenant;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface TenantRepo extends MongoRepository<Tenant, String> {
}