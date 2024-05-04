package com.reserv.tenant.service;

import com.reserv.tenant.entity.Merchant;
import com.reserv.tenant.repo.MerchantRepo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class MerchantService {

    @Autowired
    MerchantRepo merchantRepo;

    public Mono<Merchant> getMerchant(String merchantId) {

        return merchantRepo.findById(merchantId);
    }

    public Mono<Merchant> addMerchant(Merchant merchant) {
        return merchantRepo.save(merchant);
    }
}
