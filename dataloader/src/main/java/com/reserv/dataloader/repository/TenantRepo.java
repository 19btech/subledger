package com.reserv.dataloader.repository;

import com.reserv.dataloader.entity.Tenant;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface TenantRepo extends MongoRepository<Tenant, String> {
}