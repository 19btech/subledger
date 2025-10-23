package com.fyntrac.common.service;

import com.fyntrac.common.entity.Tenant;
import com.fyntrac.common.repository.TenantRepo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
public class TenantService {

    private final TenantRepo tenantRepository;

    @Autowired
    public TenantService(TenantRepo tenantRepository) {
        this.tenantRepository = tenantRepository;
    }

    public List<Tenant> getAllTenants() {
        return tenantRepository.findAll();
    }
}