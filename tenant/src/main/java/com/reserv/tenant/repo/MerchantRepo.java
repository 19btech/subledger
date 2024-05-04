package com.reserv.tenant.repo;

import com.reserv.tenant.entity.Merchant;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;

public interface MerchantRepo extends ReactiveMongoRepository<Merchant,String> {
}
